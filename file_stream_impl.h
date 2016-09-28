// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <file_stream.h>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <map>
#include <queue>

typedef std::mutex mutex_t;
typedef std::unique_lock<mutex_t> lock_t;
typedef std::condition_variable cond_t;

constexpr block_idx_t no_block_idx = std::numeric_limits<block_idx_t>::max();
constexpr block_offset_t no_block_offset = std::numeric_limits<block_offset_t>::max();
constexpr block_size_t no_block_size = std::numeric_limits<block_size_t>::max();

class block: public block_base {
public:
	uint64_t m_idx;
	block_idx_t m_block;
	file_impl * m_file;
	bool m_read;
	uint32_t m_usage;
	block * m_successor;
	mutex_t m_mutex;  
	cond_t m_cond;  

	block_size_t m_prev_physical_size, m_next_physical_size, m_physical_size;
	block_offset_t m_physical_offset;
	
	friend std::ostream & operator << (std::ostream & o, const block & b) {
		o << "b(" << b.m_idx << "; block: " << b.m_block << "; usage: " << b.m_usage;
		if (b.m_physical_offset == 0) o << "*";
		return o << ")";
	}
};

class file_impl {
public:
	mutex_t m_mut;
	file_base_base * m_outer;
	int m_fd;
	
	block * m_last_block; // A pointer to the last active block
	uint64_t m_logical_size; // The logical size of the file if m_last_block is nullptr
	uint64_t m_blocks; //The number of blocks in the file

	uint32_t m_first_physical_size;
	uint32_t m_last_physical_size;	
	std::map<uint64_t, block *> m_block_map;
	
	block * get_first_block(lock_t & lock);
	block * get_last_block(lock_t &);
	block * get_successor_block(lock_t & lock, block * block);
	void free_block(lock_t & lock, block * block);

	void update_physical_size(lock_t &, uint64_t block, uint32_t size);	
};

class stream_impl {
public:
	stream_base_base * m_outer;
	file_impl * m_file;

	block * m_cur_block;
	
	void next_block();
	void seek(uint64_t offset);
};

enum class job_type {
	term, write, read, trunc
};

class job {
public:
	job_type type;
	block * buff;
};

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


void create_available_block();
block * pop_available_block();
void destroy_available_block();
void push_available_block(block * b);
block * pop_available_block();

void process_run();

extern std::queue<job> jobs;
extern mutex_t job_mutex;
extern std::condition_variable job_cond;
extern block_base void_block;
