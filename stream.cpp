// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <tpie/file_stream/file_stream_impl.h>
#include <cassert>
#include <tpie/tpie_log.h>
#include <tpie/util.h>

namespace tpie {
namespace new_streams {

block_base void_block;

stream_base_base::stream_base_base(file_base_base * file_base)
	: m_block(nullptr)
	, m_file_base(file_base)
	, m_impl(nullptr)
	, m_cur_index(0) {
	m_impl = new stream_impl();
	m_impl->m_outer = this;
	m_impl->m_file = file_base->m_impl;
	m_impl->m_file->m_streams.insert(m_impl);
	m_block = &void_block;

	lock_t l(global_mutex);
	create_available_block(l);
	if (m_impl->m_file->m_readahead)
		create_available_block(l);
}

stream_base_base::stream_base_base(stream_base_base && o)
	: m_block(o.m_block)
	, m_file_base(o.m_file_base)
	, m_impl(o.m_impl)
	, m_cur_index(o.m_cur_index) {

	if (m_impl) {
		m_impl->m_outer = this;
		assert(m_impl->m_file->m_streams.count(m_impl) == 1);
	}

	o.m_block = nullptr;
	o.m_file_base = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
}


stream_base_base & stream_base_base::operator=(stream_base_base && o) {
	assert(this != &o);
	delete m_impl;

	m_block = o.m_block;
	m_file_base = o.m_file_base;
	m_impl = o.m_impl;
	m_cur_index = o.m_cur_index;

	if (m_impl) {
		m_impl->m_outer = this;
		assert(m_impl->m_file->m_streams.count(m_impl) == 1);
	}

	o.m_block = nullptr;
	o.m_file_base = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
	return *this;
}


stream_base_base::~stream_base_base() {
	delete m_impl;
}

void stream_base_base::next_block() {
	m_impl->next_block();
}

void stream_base_base::prev_block() {
	m_impl->prev_block();
}

void stream_base_base::seek(file_size_t off, whence w) {
	m_impl->seek(off, w);
}

stream_position stream_base_base::get_position() {
	block * b = m_impl->m_cur_block;
	if (!b) return m_impl->m_file->start_position();

	while (!is_known(b->m_physical_offset)) {
		// Spin lock
	}

	return {
		b->m_block,
		m_cur_index,
		b->m_logical_offset,
		b->m_physical_offset
	};
}

void stream_base_base::set_position(stream_position p) {
	lock_t l(global_mutex);
	m_impl->set_position(l, p);
}

#ifndef NDEBUG
block_base * stream_base_base::get_last_block() {
	return m_file_base->m_impl->m_last_block;
}
#endif

stream_impl::~stream_impl() {
	lock_t l(global_mutex);
	if (m_cur_block) {
		m_file->free_block(l, m_cur_block);
	}
	if (m_readahead_block) {
		m_file->free_readahead_block(l, m_readahead_block);
	}

	destroy_available_block(l);
	if (m_file->m_readahead)
		destroy_available_block(l);

	size_t c = m_file->m_streams.erase(this);
	assert(c == 1);
	tpie::unused(c);
}

void stream_impl::next_block() {
	lock_t lock(global_mutex);
	block * b = m_cur_block;
	if (b == nullptr) m_cur_block = m_file->get_first_block(lock);
	else m_cur_block = m_file->get_successor_block(lock, b);
	m_file->free_block(lock, b);
	m_outer->m_cur_index = 0;
	m_outer->m_block = m_cur_block;

	if (m_file->m_readahead && m_cur_block->m_block + 1 != m_file->m_blocks) {
		m_file->free_readahead_block(lock, m_readahead_block);
		m_readahead_block = m_file->get_successor_block(lock, m_cur_block, false);
		m_readahead_block->m_readahead_usage++;
	}
}

void stream_impl::prev_block() {
	lock_t lock(global_mutex);
	block * b = m_cur_block;
	assert(b);
	m_cur_block = m_file->get_predecessor_block(lock, b);
	m_file->free_block(lock, b);
	m_outer->m_cur_index = m_cur_block->m_logical_size;
	m_outer->m_block = m_cur_block;

	if (m_file->m_readahead && m_cur_block->m_block != 0) {
		m_file->free_readahead_block(lock, m_readahead_block);
		m_readahead_block = m_file->get_predecessor_block(lock, m_cur_block, false);
		m_readahead_block->m_readahead_usage++;
	}
}

void stream_impl::set_position(lock_t & l, stream_position p) {
	if (m_cur_block && m_cur_block->m_block == p.m_block) {
		m_outer->m_cur_index = p.m_index;
		return;
	}

	if (m_cur_block)
		m_file->free_block(l, m_cur_block);

	m_cur_block = m_file->get_block(l, p);
	m_outer->m_cur_index = p.m_index;
	m_outer->m_block = m_cur_block;
}

void stream_impl::seek(file_size_t offset, whence w) {
	log_debug() << "STREM seek       " << offset << std::endl;
	lock_t l(global_mutex);
	
	stream_position p;

	if (m_file->direct()) {
		file_size_t loc = 0;
		switch (w) {
		case whence::set:
			loc = offset;
			break;
		case whence::cur:
			loc = m_outer->offset() + offset;
			break;
		case whence::end:
			loc = m_file->m_outer->size() + offset;
			break;
		}
		p = m_file->position_from_offset(l, loc);
	} else if (offset != 0 || (w != whence::set && w != whence::end)) {
		throw std::runtime_error("Arbitrary seek not supported for compressed or serialized files");
	} else if (w == whence::set) {
		p = m_file->start_position();
	} else {
		p = m_file->end_position(l);
		log_debug() << "STREM Seeked to end at offset " << p.m_physical_offset << std::endl;
	}
	set_position(l, p);
}

} // namespace new_streams
} // namespace tpie
