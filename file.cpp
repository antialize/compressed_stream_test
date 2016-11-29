// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

file_impl::file_impl()
	: m_outer(nullptr)
	, m_fd(-1)
	, m_last_block(nullptr)
	, m_blocks(0)
	, m_job_count(0)
	, m_first_physical_size(no_block_size)
	, m_last_physical_size(no_block_size)
	, m_item_size(no_block_size)
	, m_serialized(false)
	, m_direct(false) {}

file_base_base::file_base_base(bool serialized, uint32_t item_size)
	: m_impl(nullptr)
	, m_last_block(nullptr)
	, m_logical_size(0)
{
	auto impl = new file_impl();
	m_impl = impl;
	impl->m_outer = this;
	impl->m_item_size = item_size;
	impl->m_serialized = serialized;
}

file_base_base::~file_base_base() {
	close();
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

void file_base_base::close() {
	lock_t l(m_impl->m_mut);

	// Wait for all jobs to be completed for this file
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	// Free all blocks, possibly creating some write jobs
	for (auto p : m_impl->m_block_map) {
		block *b = p.second;
		m_impl->free_block(l, b);
	}

	// Wait for all freed dirty blocks to be written
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	// Set the owner of the blocks to nullptr
	for (auto p : m_impl->m_block_map) {
		block *b = p.second;
		b->m_file = nullptr;
	}

	m_impl->m_block_map.clear();

	::close(m_impl->m_fd);

	m_impl->m_fd = -1;
	m_last_block = m_impl->m_last_block = nullptr;
	m_impl->m_blocks = 0;
	m_impl->m_end_position = {0};
	m_impl->m_first_physical_size = no_block_size;
	m_impl->m_last_physical_size = no_block_size;

	m_logical_size = 0;
}

void file_impl::update_physical_size(lock_t & lock, block_idx_t block, block_size_t size) {
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

block * file_impl::get_block(lock_t & l, stream_position p, block * predecessor) {
	log_info() << "FILE  get_block   " << p.m_block << std::endl;

	block * b = get_available_block(l, p.m_block);
	if (b) {
		//log_info() << "\033[0;34mfetch " << it->second->m_idx << " " << block_number << "\033[0m" << std::endl;
		if (b->m_block != p.m_block) {
			throw std::runtime_error("Logic error");
		}
		if (b->m_usage == 0) make_block_unavailable(b);
		b->m_usage++;
		return b;
	}
	
	l.unlock();
	auto buff = pop_available_block();
	l.lock();
	
	buff->m_file = this;
	buff->m_dirty = false;
	buff->m_block = p.m_block;
	buff->m_physical_offset = p.m_physical_offset;
	buff->m_logical_offset = p.m_logical_offset;
	buff->m_logical_size = no_block_size;
	buff->m_usage = 1;
	buff->m_read = false;
	buff->m_maximal_logical_size = block_size() / buff->m_file->m_item_size;
	m_block_map.emplace(buff->m_block, buff);
	
	if (p.m_physical_offset == no_file_size) {
		assert(predecessor != nullptr);
		//log_info() << "\033[0;31massign " << buff->m_idx << " " << buff->m_block << " delayed" << "\033[0m" << std::endl;
	} else {
		//log_info() << "\033[0;31massign " << buff->m_idx << " " << buff->m_block << " " << buff->m_physical_offset << "\033[0m" << std::endl;
	}

	if (p.m_block == m_blocks) {
		++m_blocks;
		buff->m_logical_size = 0;
		buff->m_read = true;
	} else {
		log_info() << "FILE  read       " << *buff << std::endl;
		//We need to read stuff
		{
			m_job_count++;

			lock_t l2(job_mutex);
			job j;
			j.type = job_type::read;
			j.buff = buff;
			j.file = this;
			buff->m_usage++;
			buff->m_io = true;
			//log_info() << "read block " << *buff << std::endl;
			jobs.push(j);
			job_cond.notify_all();
		}

		while (!buff->m_read) buff->m_cond.wait(l);
	}
  
	if (p.m_block+ 1 == m_blocks) {
		//log_info() << "Setting last block to " << buff << std::endl;
		m_outer->m_last_block = m_last_block = buff;
	}
   
	//log_info() << "get succ " << *buff << std::endl;
	return buff;
}
	

block * file_impl::get_successor_block(lock_t & l, block * t) {
	stream_position p;
	p.m_block = t->m_block + 1;
	p.m_index = 0;
	p.m_logical_offset = t->m_logical_offset + t->m_logical_size;
	p.m_physical_offset = (t->m_physical_size != no_block_size && t->m_physical_offset != no_file_size)
		? t->m_physical_size + t->m_physical_offset
		: no_file_size;
	return get_block(l, p, t);
}

block * file_impl::get_predecessor_block(lock_t & l, block * t) {
	stream_position p;
	p.m_block = t->m_block - 1;
	p.m_index = 0;
	p.m_logical_offset = no_file_size;
	p.m_physical_offset = (t->m_physical_offset != no_block_size && t->m_prev_physical_size != no_block_size)
		? t->m_physical_offset - t->m_prev_physical_size
		: no_file_size;
	return get_block(l, p, nullptr);
}


void file_impl::free_block(lock_t &, block * t) {
	if (t == nullptr) return;
	--t->m_usage;

	// Even if t->m_usage > 1, we need to write the block if it's dirty
	// This is not a problem as we only support appending to a file
	if (t->m_dirty) {

		//TODO check that we are allowed to write to this block
		
		log_info() << "      free block " << *t << " write" << std::endl;

		m_job_count++;

		// Write dirty block
		lock_t l2(job_mutex);

		job j;
		j.type = job_type::write;
		j.buff = t;
		j.file = this;
		t->m_usage++;
		t->m_dirty = false;
		t->m_io = true;
		//log_info() << "write block " << *t << std::endl;
		jobs.push(j);
		job_cond.notify_all();

		return;
	}

	if (t->m_usage != 0) return;

	if (t->m_physical_offset != no_file_size) {
		log_info() << "      free block " << *t << " avail" << std::endl;
		//log_info() << "avail block " << *t << std::endl;

		// If this is the last block and it's size is 0
		// We shouldn't count this block
		if (t->m_file->m_last_block == t && t->m_logical_size == 0) {
			t->m_file->m_blocks--;
		}

		push_available_block(t);
	}
}

void file_impl::kill_block(lock_t & l, block * t) {
	log_info() << "      kill block " << *t << std::endl;
	assert(t->m_file == this);
	assert(t->m_logical_offset != no_file_size);
	assert(t->m_physical_offset != no_file_size);
	assert(t->m_logical_size != no_block_size);
	size_t c = m_block_map.erase(t->m_block);
	assert(c == 1);
	t->m_file = nullptr;
	if (m_last_block == t) {
		m_end_position.m_logical_offset = t->m_logical_offset;
		m_end_position.m_physical_offset = t->m_physical_offset;
		m_end_position.m_block = t->m_block;
		m_end_position.m_index = t->m_logical_size;
		m_outer->m_logical_size = t->m_logical_offset + t->m_logical_size;
		m_outer->m_last_block = m_last_block = nullptr;
	}
}
