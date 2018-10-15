// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <vector>
#include <thread>
#include <cassert>
#include <exception>
#include "exception.h"

std::vector<std::thread> process_threads;

#ifndef NDEBUG
#include <unordered_set>
extern std::unordered_set<block *> all_blocks;
#endif

size_t available_blocks(size_t threads) {
	return threads + 1;
}

void file_stream_init(size_t threads) {
	if (threads < 1) {
		throw exception("Need at least one file job thread");
	}
	void_block.m_logical_offset = 0;
	void_block.m_logical_size = 0;
	void_block.m_maximal_logical_size = 0;
	void_block.m_serialized_size = 0;

	{
		lock_t l(global_mutex);
		for (size_t i = 0; i < available_blocks(threads); ++i)
			create_available_block(l);
	}

	init_job_buffers();

	for (size_t i=0; i < threads; ++i)
		process_threads.emplace_back(process_run);
}

void file_stream_term() {
	destroy_job_buffers();

	lock_t l(global_mutex);
	{
		job j;
		j.type = job_type::term;
		j.file = nullptr;
		j.io_block = nullptr;
		jobs.push(j);
		global_cond.notify_all();
	}
	l.unlock();

	for (auto & t: process_threads)
		t.join();

	l.lock();
	for (size_t i = 0; i < available_blocks(process_threads.size()); ++i)
		destroy_available_block(l);

#ifndef NDEBUG
	assert(all_blocks.size() == 0);
#endif
	
	process_threads.clear();

	jobs.pop();
	assert(jobs.size() == 0);
}

#ifndef NDEBUG
mutex_t crapper::m;
#endif

