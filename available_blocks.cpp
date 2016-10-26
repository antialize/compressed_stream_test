// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <file_stream_impl.h>
#include <queue>
#include <cassert>

#ifndef NDEBUG
struct queue_t : public std::queue<block *> {
	container_type container() { return c; }
};
#else
typedef std::queue<block *> queue_t;
#endif

namespace {
mutex_t available_blocks_mutex;
std::condition_variable available_block_cond;
queue_t available_blocks;
}

size_t ctr = 0;
void create_available_block() {
	lock_t l(available_blocks_mutex);
	auto b = new block();
	b->m_idx = ctr++;
	b->m_file = nullptr;
	available_blocks.push(b);
	available_block_cond.notify_one();


	log_info() << "AVAIL create     " << *b << std::endl;
}

block * pop_available_block();

void destroy_available_block() {
	auto b = pop_available_block();
	log_info() << "AVAIL destroy    " << *b << std::endl;
	delete b;
}

void push_available_block(block * b) {
	lock_t l(available_blocks_mutex);

#ifndef NDEBUG
	for (block * bb : available_blocks.container()) {
		assert(bb != b);
	}
#endif

	available_blocks.push(b);
	available_block_cond.notify_one();
	log_info() << "AVAIL push       " << *b << std::endl;
}


block * pop_available_block() {
	while (true) {
		block * b = nullptr;
		{
			lock_t l(available_blocks_mutex);
			while (available_blocks.empty()) available_block_cond.wait(l);
			b = available_blocks.front();
			available_blocks.pop();
		}
		if (b->m_file) {
			//log_info() << "\033[0;32mfree " << b->m_idx << " " << b->m_block << "\033[0m" << std::endl;
			lock_t l(b->m_file->m_mut);
			if (!b->is_available(l)) continue;
			b->m_file->kill_block(l, b);
		}
		
		b->m_file = nullptr;
		b->m_block = 0;
		b->m_dirty = false;
		b->m_usage = 0;
		b->m_physical_offset = no_block_offset;
		b->m_physical_size = no_block_size;
		b->m_next_physical_size = no_block_size;
		b->m_prev_physical_size = no_block_size;
		b->m_logical_offset = no_block_offset;
		b->m_logical_size = no_block_size;
		b->m_successor = nullptr;
		log_info() << "AVAIL pop        " << *b << std::endl;
		return b;
	}
}
