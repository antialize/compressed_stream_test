// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <mutex>
#include <condition_variable>
#include <ostream>

typedef std::mutex mutex_t;
typedef std::unique_lock<mutex_t> lock_t;
typedef std::condition_variable cond_t;

class file;

constexpr size_t block_size = 1024;

class buffer {
public:
	size_t m_idx;
	file * m_file;
	uint64_t m_block;
	uint32_t m_dirty;

	uint32_t m_usage;
			
	uint64_t m_offset;
	uint32_t m_disk_size;
	
	buffer * m_successor;
	mutex_t m_mutex;  
	cond_t m_cond;  
	size_t m_data[block_size];

	friend std::ostream & operator << (std::ostream & o, const buffer & b) {
		o << "b(" << b.m_idx << "; block: " << b.m_block << "; usage: " << b.m_usage;
		if (b.m_offset == 0) o << "*";
		return o << ")";
	}
};
