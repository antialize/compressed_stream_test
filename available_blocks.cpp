// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <file_stream_impl.h>
#include <unordered_set>
#include <cassert>

namespace {
mutex_t available_blocks_mutex;
std::condition_variable available_block_cond;
std::unordered_set<block *> available_blocks;
}

size_t ctr = 0;
void print_available_blocks() {
	log_info() << "Total number of blocks: " << ctr << "\n"
			   << "free: " << available_blocks.size() << ", "
			   << "non-free: " << (ctr - available_blocks.size()) << "\n";

	for (block * b : available_blocks) {
		log_info() << *b << "\n";
	}
}

void create_available_block() {
	lock_t l(available_blocks_mutex);
	auto b = new block();
	b->m_idx = ctr++;
	b->m_file = nullptr;
	available_blocks.insert(b);
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
	assert(available_blocks.count(b) == 0);
#endif

	available_blocks.insert(b);
	available_block_cond.notify_one();
	log_info() << "AVAIL push       " << *b << std::endl;
}

void make_block_unavailable(block * b) {
	lock_t l(available_blocks_mutex);
	assert(available_blocks.erase(b) == 1);
}

block * pop_available_block() {
	while (true) {
		block * b = nullptr;
		{
			lock_t l(available_blocks_mutex);
			while (available_blocks.empty()) available_block_cond.wait(l);
			auto it = available_blocks.begin();
			b = *it;
			available_blocks.erase(it);
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
		b->m_physical_offset = no_file_size;
		b->m_physical_size = no_block_size;
		b->m_next_physical_size = no_block_size;
		b->m_prev_physical_size = no_block_size;
		b->m_logical_offset = no_file_size;
		b->m_logical_size = no_block_size;
		log_info() << "AVAIL pop        " << *b << std::endl;
		return b;
	}
}
