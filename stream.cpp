// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <cassert>

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
	create_available_block();
	create_available_block();
}

stream_base_base::stream_base_base(stream_base_base && o)
	: m_block(o.m_block)
	, m_file_base(o.m_file_base)
	, m_impl(o.m_impl)
	, m_cur_index(o.m_cur_index) {

	m_impl->m_outer = this;

	o.m_block = nullptr;
	o.m_file_base = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
}


stream_base_base & stream_base_base::operator=(stream_base_base && o) {
	if (m_impl) {
		m_impl->close();
		delete m_impl;
	}

	m_block = o.m_block;
	m_file_base = o.m_file_base;
	m_impl = o.m_impl;
	m_cur_index = o.m_cur_index;

	if (m_impl)
		m_impl->m_outer = this;

	o.m_block = nullptr;
	o.m_file_base = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
	return *this;
}


stream_base_base::~stream_base_base() {
	if (m_impl) {
		m_impl->close();
		delete m_impl;
	}
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

	lock_t l(m_impl->m_file->m_mut);
	while (!is_known(b->m_physical_offset)) {
		b->m_cond.wait(l);
	}

	return {
		b->m_block,
		m_cur_index,
		b->m_logical_offset,
		b->m_physical_offset
	};
}

void stream_base_base::set_position(stream_position p) {
	lock_t l(m_impl->m_file->m_mut);
	m_impl->set_position(l, p);
}

void stream_impl::close() {
	if (m_cur_block) {
		lock_t lock(m_file->m_mut);
		m_file->free_block(lock, m_cur_block);
	}
	if (m_readahead_block) {
		lock_t lock(m_file->m_mut);
		m_file->free_readahead_block(lock, m_readahead_block);
	}
	destroy_available_block();
	destroy_available_block();
	size_t c = m_file->m_streams.erase(this);
	assert(c == 1);
}

void stream_impl::next_block() {
	lock_t lock(m_file->m_mut);
	block * buff = m_cur_block;
	if (buff == nullptr) m_cur_block = m_file->get_first_block(lock);
	else m_cur_block = m_file->get_successor_block(lock, buff);
	m_file->free_block(lock, buff);
	m_outer->m_cur_index = 0;
	m_outer->m_block = m_cur_block;

	if (m_cur_block->m_block + 1 != m_file->m_blocks) {
		m_file->free_readahead_block(lock, m_readahead_block);
		m_readahead_block = m_file->get_successor_block(lock, m_cur_block);
		m_readahead_block->m_readahead_usage++;

	}
}

void stream_impl::prev_block() {
	lock_t lock(m_file->m_mut);
	block * buff = m_cur_block;
	assert(buff);
	m_cur_block = m_file->get_predecessor_block(lock, buff);
	m_file->free_block(lock, buff);
	m_outer->m_cur_index = m_cur_block->m_logical_size;
	m_outer->m_block = m_cur_block;

	if (m_cur_block->m_block != 0) {
		m_file->free_readahead_block(lock, m_readahead_block);
		m_readahead_block = m_file->get_predecessor_block(lock, m_cur_block);
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
	log_info() << "STREM seek       " << offset << std::endl;
	lock_t l(m_file->m_mut);
	
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
		log_info() << "STREM Seeked to end at offset " << p.m_physical_offset << std::endl;
	}
	set_position(l, p);
}


