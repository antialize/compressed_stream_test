// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <tpie/file_stream/file_utils.h>
#include <queue>
#include <cassert>
#include <snappy.h>
#include <atomic>
#include <tpie/tpie_log.h>

namespace tpie {
namespace new_streams {

void execute_read_job(lock_t & job_lock, file_impl * file, block * b);
void execute_write_job(lock_t & job_lock, file_impl * file, block * b);
void execute_truncate_job(lock_t &, file_impl * file, file_size_t truncate_size);

void init_job_buffers();
void destroy_job_buffers();

void process_run();

} // namespace new_streams
} // namespace tpie
