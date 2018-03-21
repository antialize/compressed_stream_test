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
#include <unordered_set>
#include <functional>
#include <atomic>

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

template <typename T>
bool is_known(const std::atomic<T> & val) {
	return is_known(val.load());
}

class block;
void create_available_block();
block * pop_available_block();
void make_block_unavailable(block * b);
void destroy_available_block();
void push_available_block(block * b);
block * pop_available_block();

struct file_header {
	static const uint64_t magicConst = 0x454c494645495054ull;
	static const uint64_t versionConst = 0;

	uint64_t magic;
	uint64_t version;
	block_idx_t blocks;
	size_t user_data_size;
	size_t max_user_data_size;
	bool isCompressed : 1;
	bool isSerialized : 1;
};

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
	uint32_t m_usage;
	uint32_t m_readahead_usage;
	cond_t m_cond;
	bool m_io; // false = owned by main thread, true = owned by job thread

	block_size_t m_prev_physical_size, m_physical_size, m_next_physical_size;
	std::atomic<file_size_t> m_physical_offset;

	friend std::ostream & operator << (std::ostream & o, const block & b) {
		o << "b(" << b.m_idx << "; block: " << b.m_block << "; usage: " << b.m_usage << "; io: " << b.m_io;
		if (b.m_physical_offset == 0) o << "*";
		return o << ")";
	}
};

class file_impl {
public:
	file_base_base * m_outer;
	int m_fd;
	std::string m_path;

	// An unique id for the entire run of the program
	// Should change when the file is closed/opened
#ifndef NDEBUG
	size_t m_file_id;

	static size_t file_ctr;
#endif

	// Either m_last_block is null and m_end_position is the end position
	// or m_last_block is the last block
	block * m_last_block; // A pointer to the last active block
	stream_position m_end_position;

	block_idx_t m_blocks; //The number of blocks in the file

	// Tells how many jobs remain to be performed on the file.
	// We can only close a file when the job count is 0.
	uint32_t m_job_count;
	cond_t m_job_cond;

	block_size_t m_item_size;
	std::map<block_idx_t, block *> m_block_map;

	bool m_serialized;
	bool m_compressed;

	bool m_readahead;

	bool m_readonly;
	file_header m_header;

	std::unordered_set<stream_impl *> m_streams;


	file_impl(file_base_base * outer, bool serialized, block_size_t item_size);

	bool direct() const {
		return !m_compressed && !m_serialized;
	}

	// Note: if you want to keep this block alive after unlocking the lock,
	// you have to increment its m_usage
	block * get_available_block(lock_t &, block_idx_t block) const {
		auto it = m_block_map.find(block);
		if (it == m_block_map.end()) return nullptr;
		assert(it->second->m_block == block);
		return it->second;
	}

	block * get_block(lock_t & lock, stream_position p, bool find_next = true, block * rel = nullptr);

	stream_position start_position() const noexcept {
		return stream_position{0, 0, 0, sizeof(file_header) + m_outer->max_user_data_size()};
	}

	void block_ref_inc(lock_t &, block * b) const noexcept {
		if (b->m_usage == 0) make_block_unavailable(b);
		b->m_usage++;
	}

	stream_position end_position(lock_t & l) noexcept {
		if (!m_last_block) return m_end_position;

		// Make sure m_last_block is not repurposed before, we can get its info
		block_ref_inc(l, m_last_block);
		while (m_last_block->m_io) m_last_block->m_cond.wait(l);

		stream_position p;
		p.m_block = m_last_block->m_block;
		p.m_index = m_last_block->m_logical_size;
		p.m_logical_offset = m_last_block->m_logical_offset;
		p.m_physical_offset = m_last_block->m_physical_offset;

		free_block(l, m_last_block);

		return p;
	}

	stream_position position_from_offset(lock_t & l, file_size_t offset);

	// Calls a function for each block in m_block_map
	// Makes sure that if f kills any of the blocks, then it still works
	template <typename F>
	void foreach_block(F f) {
		for (auto it = m_block_map.begin(); it != m_block_map.end();) {
			auto itnext = std::next(it);
			block *b = it->second;
			it = itnext;

			f(b);
		}
	}

	block * get_first_block(lock_t & lock) {return get_block(lock, start_position());}
	block * get_last_block(lock_t & lock) {return get_block(lock, end_position(lock));}
	block * get_successor_block(lock_t & lock, block * block);
	block * get_predecessor_block(lock_t & lock, block * block);
	void free_readahead_block(lock_t & lock, block * block);
	void free_block(lock_t & lock, block * block);
	void kill_block(lock_t & lock, block * block);

	void update_related_physical_sizes(lock_t & l, block * b);

	void do_serialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) {
		assert(m_serialized);
		m_outer->do_serialize(in, in_items, out, out_size);
	}

	void do_unserialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) {
		assert(m_serialized);
		m_outer->do_unserialize(in, in_items, out, out_size);
	}
};

class stream_impl {
public:
	stream_base_base * m_outer;
	file_impl * m_file;
	block * m_cur_block;
	block * m_readahead_block;

	~stream_impl();

	void next_block();
	void prev_block();
	void seek(file_size_t offset, whence w);
	void set_position(lock_t & l, stream_position p);
};

enum class job_type {
	term, write, read, trunc
};

struct job {
	job_type type;
	file_impl * file;
	union {
		block * io_block;
		file_size_t truncate_size;
	};
};

void process_run();

extern std::queue<job> jobs;
extern mutex_t job_mutex;
extern std::condition_variable job_cond;
extern block_base void_block;
