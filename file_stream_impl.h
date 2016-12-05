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
constexpr file_size_t no_file_size = std::numeric_limits<file_size_t>::max();
constexpr block_size_t no_block_size = std::numeric_limits<block_size_t>::max();

template <typename T>
bool is_known(T val) {
	return val != std::numeric_limits<T>::max();
}

class block;
void create_available_block();
block * pop_available_block();
void make_block_unavailable(block * b);
void destroy_available_block();
void push_available_block(block * b);
block * pop_available_block();

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
	cond_t m_cond;
	bool m_io; // false = owned by main thread, true = owned by job thread

	block_size_t m_prev_physical_size, m_next_physical_size, m_physical_size;
	file_size_t m_physical_offset;
	
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

	// The following to variabels maintain the end of the file
	// if the last block is the file is loaded m_last_block will contain
	// a pointer to it, otherwise m_end_position will point to the end of the file.
	block * m_last_block; // A pointer to the last active block
	stream_position m_end_position;
	
	block_idx_t m_blocks; //The number of blocks in the file

	// Tells how many jobs remain to be performed on the file.
	// We can only close a file when the job count is 0.
	uint32_t m_job_count;
	cond_t m_job_cond;

	block_size_t m_first_physical_size;
	block_size_t m_last_physical_size;
	uint32_t m_item_size;
	bool m_serialized;
	bool m_direct;
	std::map<block_idx_t, block *> m_block_map;

	file_impl();

	// Note: if you want to keep this block alive after unlocking the lock,
	// you have to increment its m_usage
	block * get_available_block(lock_t & lock, block_idx_t block) {
		auto it = m_block_map.find(block);
		if (it == m_block_map.end()) return nullptr;
		assert(it->second->m_block == block);
		return it->second;
	}

	block * get_block(lock_t & lock, stream_position p, block * predecessor = nullptr);
	
	static constexpr stream_position start_position() noexcept {
		return stream_position{0, 0, 0, 0};
	}

	void block_ref_inc(lock_t & l, block * b) const noexcept {
		if (b->m_usage == 0) make_block_unavailable(b);
		b->m_usage++;
	}

	stream_position end_position(lock_t & l) noexcept {
		if (m_last_block) {
			// Make sure m_last_block is not repurposed before, we can get its info
			block_ref_inc(l, m_last_block);
			while (m_last_block->m_io) m_last_block->m_cond.wait(l);

			stream_position p;
			p.m_block = m_last_block->m_block;
			p.m_index = m_last_block->m_logical_size;
			p.m_logical_offset = m_last_block->m_logical_offset;
			p.m_physical_offset = m_last_block->m_physical_offset;

			// Decrement m_last_block's usage
			free_block(l, m_last_block);

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

	void update_physical_size(lock_t &, block_idx_t block, block_size_t size);
};

class stream_impl {
public:
	stream_base_base * m_outer;
	file_impl * m_file;
	block * m_cur_block;
	bool m_seek_end;

	
	void next_block();
	void seek(file_size_t offset, whence w);
	void set_position(lock_t & l, stream_position p);
};

enum class job_type {
	term, write, read, trunc
};

class job {
public:
	job_type type;
	file_impl * file;
	block * buff;
};


struct block_header {
	file_size_t logical_offset;
	block_size_t physical_size;
	block_size_t logical_size;
};

void process_run();

extern std::queue<job> jobs;
extern mutex_t job_mutex;
extern std::condition_variable job_cond;
extern block_base void_block;
