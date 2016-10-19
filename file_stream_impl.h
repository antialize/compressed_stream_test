// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <log.h>
#include <file_stream.h>
#include <mutex>
#include <condition_variable>
#include <limits>
#include <map>
#include <queue>

typedef std::mutex mutex_t;
typedef std::unique_lock<mutex_t> lock_t;
typedef std::condition_variable cond_t;

constexpr block_idx_t no_block_idx = std::numeric_limits<block_idx_t>::max();
constexpr block_offset_t no_block_offset = std::numeric_limits<block_offset_t>::max();
constexpr block_size_t no_block_size = std::numeric_limits<block_size_t>::max();

/**
 * Class representing a block in a file
 * if a block as attacted to a file all members except
 * m_logical_size, m_dirty and m_data  are protected by thats file mutex
 *
 */
class block: public block_base {
public:
	uint64_t m_idx;
	block_idx_t m_block;
	file_impl * m_file;
	bool m_read;
	uint32_t m_usage;
	block * m_successor;
	cond_t m_cond;  

	block_size_t m_prev_physical_size, m_next_physical_size, m_physical_size;
	block_offset_t m_physical_offset;
	
	friend std::ostream & operator << (std::ostream & o, const block & b) {
		o << "b(" << b.m_idx << "; block: " << b.m_block << "; usage: " << b.m_usage;
		if (b.m_physical_offset == 0) o << "*";
		return o << ")";
	}

	bool is_available(lock_t & file_lock) const noexcept {
		return m_usage == 0 && m_physical_offset != no_block_offset;
	}
};

class file_impl {
public:
	mutex_t m_mut;
	file_base_base * m_outer;
	int m_fd;

	// The following to variabels maintain the end of the file
	// if the last block is the file is loaded m_last_block will contain
	// a pointer to it, otherwise m_end_position will point to the end of the file.
	block * m_last_block; // A pointer to the last active block
	stream_position m_end_position;
	
	uint64_t m_logical_size; // The logical size of the file if m_last_block is nullptr
	uint64_t m_blocks; //The number of blocks in the file

	// Signals whether the file is closed and no jobs remain to be performed on the file.
	// This ensures that we don't deallocate a file_base_base and file_impl when more jobs needs to done on it.
	bool m_closed;

	uint32_t m_first_physical_size;
	uint32_t m_last_physical_size;
	uint32_t m_item_size;
	bool m_serialized;
	bool m_direct;
	std::map<uint64_t, block *> m_block_map;

	file_impl();

	block * get_block(lock_t & lock, stream_position p, block * predecessor = nullptr);
	
	static constexpr stream_position start_position() noexcept {
		return stream_position{0, 0, 0, 0};
	}

	stream_position end_position(lock_t &) const noexcept {
		if (m_last_block) {
			stream_position p;
			p.m_block = m_last_block->m_block;
			p.m_index = m_last_block->m_logical_size;
			p.m_logical_offset = m_last_block->m_logical_offset;
			p.m_physical_offset = m_last_block->m_physical_offset;
			return p;
		} else {
			return m_end_position;
		}
	}
	
	block * get_first_block(lock_t & lock) {return get_block(lock, start_position());}
	block * get_last_block(lock_t & lock) {return get_block(lock, end_position(lock));}
	block * get_successor_block(lock_t & lock, block * block);
	block * get_predecessor_block(lock_t & lock, block * block);
	void free_block(lock_t & lock, block * block);
	void kill_block(lock_t & lock, block * block);

	void update_physical_size(lock_t &, uint64_t block, uint32_t size);	
};

class stream_impl {
public:
	stream_base_base * m_outer;
	file_impl * m_file;
	block * m_cur_block;
	bool m_seek_end;

	
	void next_block();
	void seek(uint64_t offset, whence w);
	void set_position(lock_t & l, stream_position p);
};

enum class job_type {
	term, write, read, trunc, close
};

class job {
public:
	job_type type;
	file_impl * file;
	block * buff;
};


struct block_header {
	block_offset_t logical_offset;
	block_size_t physical_size;
	block_size_t logical_size;
};

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
