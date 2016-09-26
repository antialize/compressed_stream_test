// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <mutex>
#include <condition_variable>
#include <ostream>
#include <iostream>

typedef std::mutex mutex_t;
typedef std::unique_lock<mutex_t> lock_t;
typedef std::condition_variable cond_t;

typedef uint64_t block_idx_t;
typedef uint64_t block_offset_t;
typedef uint32_t block_size_t;

constexpr block_idx_t no_block_idx = std::numeric_limits<block_idx_t>::max();
constexpr block_offset_t no_block_offset = std::numeric_limits<block_offset_t>::max();
constexpr block_size_t no_block_size = std::numeric_limits<block_size_t>::max();

struct crapper {
  static mutex_t m;
  lock_t l;
  crapper(): l(m) {}

};

template <typename T>
std::ostream & operator <<(const crapper & c, const T & t) {
  return std::cout << t;
}
			   

inline crapper log_info() {
  return crapper();
}


class file;

constexpr block_size_t block_size = 102*8;

class block {
public:
	uint64_t m_idx;
	block_idx_t m_block;
	file * m_file;
	bool m_dirty, m_read;
	uint32_t m_usage;
	block * m_successor;
	mutex_t m_mutex;  
	cond_t m_cond;  

	block_size_t m_prev_physical_size, m_next_physical_size, m_physical_size, m_logical_size;
	block_offset_t m_logical_offset, m_physical_offset;

	char m_data[block_size];

	block()
		: m_idx(0)
		, m_block(no_block_idx)
		, m_file(nullptr)
		, m_dirty(false)
		, m_read(false)
		, m_usage(0)
		, m_successor(nullptr)
		, m_prev_physical_size(no_block_size)
		, m_next_physical_size(no_block_size)
		, m_physical_size(no_block_size)
		, m_logical_size(no_block_size)
		, m_logical_offset(no_block_offset)
		, m_physical_offset(no_block_offset) {}
		
	
	friend std::ostream & operator << (std::ostream & o, const block & b) {
		o << "b(" << b.m_idx << "; block: " << b.m_block << "; usage: " << b.m_usage;
		if (b.m_physical_offset == 0) o << "*";
		return o << ")";
	}
};
