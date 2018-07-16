// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <tpie/tpie_log.h>
#include <tpie/file_stream/file_stream_impl.h>
#include <unordered_set>
#include <cassert>

namespace {
std::unordered_set<block *> available_blocks;
}

size_t ctr = 0;

#ifndef NDEBUG
#include <unordered_map>

std::unordered_set<block *> all_blocks;
void print_debug() {
	std::unordered_set<file_impl *> files;
	std::unordered_map<file_impl *, std::vector<block *>> owned_blocks;
	for (block * b : all_blocks) {
		files.insert(b->m_file);
		owned_blocks[b->m_file].push_back(b);
	}

	std::cerr << "Blocks: " << all_blocks.size() << "\n"
			  << "  Non-free: " << (all_blocks.size() - available_blocks.size()) << "\n"
			  << "  Free: " << available_blocks.size() << ", "
			  << "  Belonging to a file: " << (all_blocks.size() - owned_blocks[nullptr].size()) << ", "
			  << "  Not belonging to a file: " << owned_blocks[nullptr].size() << "\n";

	std::cerr << "\n";

	std::cerr << "Files: " << (files.size() - files.count(nullptr)) << "\n";
	for (file_impl * f : files) {
		if (!f) continue;
		std::cerr << f->m_path << "\n";
		std::cerr << "  Streams: " << f->m_streams.size() << "\n";
		std::cerr << "  Blocks:  " << owned_blocks[f].size() << "\n";
		for (block * b : owned_blocks[f]) {
			std::cerr << "    " << *b << "\n";
		}
		std::cerr << "\n";
	}

	std::cerr << "Blocks not owned by a file:\n";
	for (block * b : owned_blocks[nullptr]) {
		std::cerr << "  " << *b << "\n";
	}
}
#endif

void create_available_block(lock_t &) {
	auto b = new block();
	b->m_idx = ctr++;
	b->m_file = nullptr;
	available_blocks.insert(b);
	global_cond.notify_all();
#ifndef NDEBUG
	all_blocks.insert(b);
#endif

	log_debug() << "AVAIL create     " << *b << std::endl;
}

block * pop_available_block(lock_t &);

void destroy_available_block(lock_t & l) {
	auto b = pop_available_block(l);
	assert(b->m_usage == 0);
	log_debug() << "AVAIL destroy    " << *b << std::endl;
#ifndef NDEBUG
	size_t c = all_blocks.erase(b);
	assert(c == 1);
#endif
	delete b;
}

void push_available_block(lock_t &, block * b) {
#ifndef NDEBUG
	assert(available_blocks.count(b) == 0);
#endif

	available_blocks.insert(b);
	global_cond.notify_all();
	log_debug() << "AVAIL push       " << *b << std::endl;
}

void make_block_unavailable(lock_t &, block * b) {
	size_t res = available_blocks.erase(b);
	assert(res == 1);
	unused(res);
}

block * pop_available_block(lock_t & l) {
	while (true) {
		while (available_blocks.empty()) global_cond.wait(l);
		auto it = available_blocks.begin();
		block * b = *it;
		available_blocks.erase(it);
		if (b->m_file) {
			//log_debug() << "\033[0;32mfree " << b->m_idx << " " << b->m_block << "\033[0m" << std::endl;
			b->m_file->kill_block(l, b);
		}

		b->m_logical_offset = no_file_size;
		b->m_logical_size = no_block_size;
		b->m_maximal_logical_size = no_block_size;
		b->m_serialized_size = no_block_size;
		b->m_dirty = false;

		b->m_block = 0;
		b->m_file = nullptr;
		b->m_usage = 0;
		b->m_readahead_usage = 0;
		b->m_done_reading = true;
		b->m_io = false;
		b->m_prev_physical_size = no_block_size;
		b->m_physical_size = no_block_size;
		b->m_next_physical_size = no_block_size;
		b->m_physical_offset = no_file_size;
		log_debug() << "AVAIL pop        " << *b << std::endl;
		return b;
	}
}
