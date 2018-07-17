// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <tpie/file_stream/file_stream_impl.h>

#ifndef NDEBUG
#include <unordered_map>
#endif

namespace tpie {
namespace new_streams {

#ifndef NDEBUG
std::unordered_set<block *> all_blocks;
#endif

void create_available_block(lock_t &);
block * pop_available_block(lock_t &);
void destroy_available_block(lock_t & l);
void push_available_block(lock_t &, block * b);
void make_block_unavailable(lock_t &, block * b);
block * pop_available_block(lock_t & l);

} // namespace new_streams
} // namespace tpie
