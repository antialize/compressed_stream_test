// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>

#include <vector>
#include <file_stream.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <map>
#include <cassert>
#include <snappy.h>

mutex_t crapper::m;
	
// file_stream_impl.h

std::queue<job> jobs;
mutex_t job_mutex;

std::condition_variable job_cond;

void file_impl::update_physical_size(lock_t & lock, uint64_t block, uint32_t size) {
	if (block == 0) m_first_physical_size = size;
	else {
		auto it = m_block_map.find(block -1);
		if (it != m_block_map.end()) it->second->m_next_physical_size = size;
	}
	if (block + 1 == m_blocks) m_last_physical_size = size;
	else {
		auto it = m_block_map.find(block + 1);
		if (it != m_block_map.end()) it->second->m_prev_physical_size = size;
	}
}


struct block_header {
	block_offset_t logical_offset;
	block_size_t physical_size;
	block_size_t logical_size;
};


void process_run() {
	char data1[1024*1024];
	char data2[1024*1024];
	lock_t l(job_mutex);
	log_info() << "Start job thread" << std::endl;
	while (true) {
		while (jobs.empty()) job_cond.wait(l);
		log_info() << "I HAVE JOB " << (int)jobs.front().type << std::endl;
		if (jobs.front().type == job_type::term) break;

		job j = jobs.front();
		jobs.pop();
		l.unlock();

		switch (j.type) {
		case job_type::term:
			break;
		case job_type::read:
		{
			block_idx_t block = j.buff->m_block;
			block_offset_t physical_offset = j.buff->m_physical_offset;
			block_offset_t logical_offset = j.buff->m_logical_offset;
			block_size_t physical_size = j.buff->m_physical_size;
			block_size_t logical_size = j.buff->m_logical_size;
			block_size_t prev_physical_size = j.buff->m_prev_physical_size;
			block_size_t next_physical_size = j.buff->m_next_physical_size;
			
			assert(block != no_block_idx);
			assert(physical_offset != no_block_offset);
	
			if (physical_size == no_block_size) {
				block_header h;
				::pread(j.buff->m_file->m_fd, &h, sizeof(block_header), physical_offset);
				physical_size = h.physical_size;
			}
	

			block_offset_t off = physical_offset;
			block_offset_t size = physical_size;
			if (block != 0 && prev_physical_size == no_block_size) {
				off -= sizeof(block_header);
				size += sizeof(block_header);
			}
	
			if (block + 1  != j.buff->m_file->m_blocks && next_physical_size == no_block_size) { //NOT THE LAST BLOCK
				size += sizeof(block_header);
			} 

			char * data = data1;
			
			auto r = ::pread(j.buff->m_file->m_fd, data, size, off);
			log_info() << "pread " << off << " " << size << " " << physical_size << std::endl;
			assert(r == size);			
			
			
			if (block != 0 && prev_physical_size == no_block_size) {
				log_info() << "read prev header" << std::endl;
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				prev_physical_size = h.physical_size;
			}

			{
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				log_info() << "Read current header " << physical_size << " " << h.physical_size << std::endl;
								
				data += sizeof(block_header);
				assert(physical_size == h.physical_size);
				logical_size = h.logical_size;
			}

			bool ok = snappy::RawUncompress(data, physical_size - 2*sizeof(block_header), (char *)j.buff->m_data);
			assert(ok);

			log_info() << "Read " << *j.buff << '\n'
					   << "Logical size " << logical_size << '\n'
					   << "First data " << j.buff->m_data[0] << " " << j.buff->m_data[1] << '\n';
			
			data += physical_size - sizeof(block_header); //Skip next block header
			if (block + 1  != j.buff->m_file->m_blocks && next_physical_size == no_block_idx) {
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}


			j.buff->m_prev_physical_size = prev_physical_size;
			j.buff->m_next_physical_size = next_physical_size;
			j.buff->m_logical_size = logical_size;
			j.buff->m_physical_size = physical_size;
			j.buff->m_logical_offset = logical_offset;

			j.buff->m_read = true;
			
			{
				lock_t l2(j.buff->m_mutex);
				j.buff->m_cond.notify_one();
			}

			{
				auto f = j.buff->m_file;
				lock_t l2(f->m_mut);
				f->free_block(l2, j.buff);
			}
			
		}
		break;
		case job_type::write:
		{
			// TODO check if it is undefined behaiviure to change data underneeth snappy
			// TODO only used bytes here
			memcpy(data1, j.buff->m_data, j.buff->m_logical_size * sizeof(size_t) );

			// TODO free the block here
	
			log_info() << "io write " << *j.buff << std::endl;
			size_t s2 = 1024*1024;
			snappy::RawCompress(data1, j.buff->m_logical_size * sizeof(size_t) , data2+sizeof(block_header), &s2);
			size_t bs = 2*sizeof(block_header) + s2;
			
			block_header h;
			h.physical_size = bs;
			h.logical_size = j.buff->m_logical_size;
			h.logical_offset = j.buff->m_logical_offset;
			memcpy(data2, &h, sizeof(block_header));
			memcpy(data2 + sizeof(h) + s2, &h, sizeof(block_header));
			

			uint64_t off = no_block_offset;
			{
				lock_t l2(j.buff->m_mutex);
				while (j.buff->m_physical_offset == no_block_offset) j.buff->m_cond.wait(l2);
				off = j.buff->m_physical_offset;
			}
	
			auto nb = j.buff->m_successor;
			if (nb) {
				lock_t l2(nb->m_mutex);
				nb->m_physical_offset = off + bs;
				nb->m_cond.notify_one();
			}

			::pwrite(j.buff->m_file->m_fd, data2, bs, off);
			log_info() << "write block "<< j.buff->m_block << " at " <<  off << " physical_size " << bs << " " << j.buff->m_block << std::endl;

			{
				auto f = j.buff->m_file;
				lock_t l2(f->m_mut);


				f->update_physical_size(l2, j.buff->m_block, bs);
				
				f->free_block(l2, j.buff);

				if (nb && nb->m_usage == 0) {
					nb->m_usage = 1;
					f->free_block(l2, nb);
				}
			}
	
			// Free buffer
			log_info() << "Done writing " << j.buff->m_block << std::endl;
		}
		break;
		case job_type::trunc:
			break;
		}
		l.lock();
	}
	log_info() << "End job thread" << std::endl;
}
// c++ struff

std::vector<std::thread> process_threads;

block_base void_block;
void file_stream_init(int threads) {
	void_block.m_logical_offset = 0;
	void_block.m_logical_size = 0;
	void_block.m_maximal_logical_size = 0;

	for (size_t i=0; i < threads +1; ++i)
		create_available_block();
	
	for (size_t i=0; i < threads; ++i)
		process_threads.emplace_back(process_run);
}

void file_stream_term() {
	lock_t l(job_mutex);
	job j;
	j.type = job_type::term;
	jobs.push(j);
	job_cond.notify_all();
	for (auto & t: process_threads)
		t.join();

	for (size_t i=0; i < process_threads.size() + 1; ++i)
		destroy_available_block();
	
	process_threads.clear();
}


file_base_base::file_base_base()
	: m_impl(nullptr)
	, m_last_block(nullptr)
	, m_logical_size(0)
{
	auto impl = new file_impl();
	m_impl = impl;
	impl->m_outer = this;
}

file_base_base::~file_base_base() {
	delete m_impl;
};


file_base_base::file_base_base(file_base_base &&o)
	: m_impl(o.m_impl)
	, m_last_block(o.m_last_block)
	, m_logical_size(o.m_logical_size) {
	o.m_impl = nullptr;
	o.m_last_block = nullptr;
	o.m_logical_size = 0;
	m_impl->m_outer = this;
}

void file_base_base::open(const std::string & path) {
	m_impl->m_fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 00660);
	if (m_impl->m_fd == -1)
		perror("open failed: ");
	
	::write(m_impl->m_fd, "head", 4);
}

stream_base_base::stream_base_base(file_base_base * file_base)
	: m_block(nullptr)
	, m_impl(nullptr)
	, m_cur_index(0) {
	m_impl = new stream_impl();
	m_impl->m_outer = this;
	m_impl->m_file = file_base->m_impl;
	m_block = &void_block;
	create_available_block();
}

stream_base_base::stream_base_base(stream_base_base && o)
	: m_block(o.m_block)
	, m_impl(o.m_impl)
	, m_cur_index(o.m_cur_index) {
	m_impl->m_outer = this;
	o.m_block = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
}

stream_base_base::~stream_base_base() {
	if (m_impl) {
		if (m_impl->m_cur_block) {
			lock_t lock(m_impl->m_file->m_mut);
			m_impl->m_file->free_block(lock, m_impl->m_cur_block);
		}
		destroy_available_block();
	}
	delete m_impl;
}


void stream_base_base::next_block() {
	m_impl->next_block();
}

void stream_base_base::seek(uint64_t off) {
	m_impl->seek(off);
}


block * file_impl::get_first_block(lock_t & l ) {
	auto it = m_block_map.find(0);
	if (it != m_block_map.end()) {
		log_info() << "\033[0;34mfetch " << it->second->m_idx << " " << 0 << "\033[0m" << std::endl;
		if (it->second->m_block != 0) {
			throw std::runtime_error("Logic error");
		}
		it->second->m_usage++;
		return it->second;
	}


	l.unlock();
	auto buff = pop_available_block();

	l.lock();
	buff->m_file = this;
	buff->m_dirty = 0;
	buff->m_block = 0;
	buff->m_physical_offset = 4;
	buff->m_logical_offset = 0;
	buff->m_logical_size = no_block_size;
	buff->m_physical_size = m_first_physical_size;
	buff->m_successor = nullptr;
	buff->m_usage = 1;
	m_block_map.emplace(buff->m_block, buff);
	buff->m_read = false;
  
	log_info() << "\033[0;31massign " << buff->m_idx << " " << 0 << "\033[0m" << std::endl;
    
	if (m_blocks == 0) {
		m_blocks = 1;
		buff->m_read = true;
		buff->m_logical_size = 0;
	} else {
		l.unlock();
		{
			lock_t l2(job_mutex);
			job j;
			j.type = job_type::read;
			j.buff = buff;
			buff->m_usage++;
			log_info() << "read block " << *buff << std::endl;
			jobs.push(j);
			job_cond.notify_all();
		}

		{
			lock_t l2(buff->m_mutex);
			while (!buff->m_read) buff->m_cond.wait(l2);
		}
    
		l.lock();
	}
  
	if (m_blocks == 1)
		m_last_block = buff;
  
	return buff;
}

block * file_impl::get_last_block(lock_t &) {
	return nullptr;
}



block * file_impl::get_successor_block(lock_t & l, block * t) {
	auto it = m_block_map.find(t->m_block+1);
	if (it != m_block_map.end()) {
		if (it->second->m_block != t->m_block+1){
			throw std::runtime_error("Logic error");
		}
		it->second->m_usage++;
		return it->second;
	}

	l.unlock();
	auto buff = pop_available_block();
	l.lock();
  
	size_t off = no_block_offset;
	if (t->m_physical_size != no_block_size && t->m_physical_offset != no_block_offset)
		off = t->m_physical_size + t->m_physical_offset;
  
	buff->m_file = this;
	buff->m_dirty = false;
	buff->m_block = t->m_block + 1;
	buff->m_physical_offset = off;
	buff->m_logical_offset = t->m_logical_offset + t->m_logical_size;
	buff->m_successor = nullptr;
	buff->m_logical_size = no_block_size;
	buff->m_usage = 1;
	buff->m_read = false;
	m_block_map.emplace(buff->m_block, buff);

	log_info() << "\033[0;31massign " << buff->m_idx << " " << buff->m_block << " " << buff->m_physical_offset << "\033[0m" << std::endl;
  
	if (off == no_block_offset) {
		t->m_successor = buff;
	}

	if (buff->m_block == m_blocks) {
		++m_blocks;
		buff->m_logical_size = 0;
	} else {

		l.unlock();
		{
			lock_t l2(job_mutex);
			job j;
			j.type = job_type::read;
			j.buff = buff;
			buff->m_usage++;
			log_info() << "read block " << *buff << std::endl;
			jobs.push(j);
			job_cond.notify_all();
		}

		{
			lock_t l2(buff->m_mutex);
			while (!buff->m_read) buff->m_cond.wait(l2);
		}
    
		l.lock();
		
		//We need to read stuff
	}
  
	if (buff->m_block + 1 == m_blocks)
		m_last_block = buff;
   
	log_info() << "get succ " << *buff << std::endl;
	return buff;
}

void file_impl::free_block(lock_t &, block * t) {
	if (t == nullptr) return;
	log_info() << "free block " << *t << std::endl;
	--t->m_usage;

	if (t->m_usage != 0) return;

	if (t->m_dirty) {
		// Write dirty block
		lock_t l2(job_mutex);
	  
		job j;
		j.type = job_type::write;
		j.buff = t;
		t->m_usage++;
		t->m_dirty = 0;
		log_info() << "write block " << *t << std::endl;
		jobs.push(j);
		job_cond.notify_all();
	} else if (t->m_physical_offset != no_block_offset) {
		log_info() << "avail block " << *t << std::endl;
		push_available_block(t);
	}
}

void stream_impl::next_block() {
	lock_t lock(m_file->m_mut);
	block * buff = m_cur_block;
	if (buff == nullptr) m_cur_block = m_file->get_first_block(lock);
	else m_cur_block = m_file->get_successor_block(lock, buff);
	m_file->free_block(lock, buff);
	m_outer->m_cur_index = 0;
	m_outer->m_block = m_cur_block;
}

void stream_impl::seek(uint64_t offset) {
	lock_t lock(m_file->m_mut); 
	if (offset != 0)
		throw std::runtime_error("Not supported");
    
	if (m_cur_block)
		m_file->free_block(lock, m_cur_block);
	
	m_cur_block = nullptr;
	m_outer->m_cur_index = 0;
	m_outer->m_block = &void_block;
}
