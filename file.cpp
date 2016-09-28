// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

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
}

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
