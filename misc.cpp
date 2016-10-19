// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <vector>
#include <thread>
#include <cassert>

std::vector<std::thread> process_threads;

size_t available_blocks(size_t threads) {
	return threads + 1;
}

void file_stream_init(size_t threads) {
	assert(threads >= 1);
	void_block.m_logical_offset = 0;
	void_block.m_logical_size = 0;
	void_block.m_maximal_logical_size = 0;

	for (size_t i=0; i < available_blocks(threads); ++i)
		create_available_block();
	
	for (size_t i=0; i < threads; ++i)
		process_threads.emplace_back(process_run);
}

void file_stream_term() {
	{
		lock_t l(job_mutex);
		job j;
		j.type = job_type::term;
		j.file = nullptr;
		jobs.push(j);
		job_cond.notify_all();
	}
	for (auto & t: process_threads)
		t.join();

	for (size_t i=0; i < available_blocks(process_threads.size()); ++i)
		destroy_available_block();
	
	process_threads.clear();
}

mutex_t crapper::m;

