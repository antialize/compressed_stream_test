// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <cassert>

block_base void_block;

stream_base_base::stream_base_base(file_base_base * file_base)
	: m_block(nullptr)
	, m_impl(nullptr)
	, m_cur_index(0) {
	m_impl = new stream_impl();
	m_impl->m_outer = this;
	m_impl->m_file = file_base->m_impl;
	m_block = &void_block;
	create_available_block();
}

stream_base_base::stream_base_base(stream_base_base && o)
	: m_block(o.m_block)
	, m_impl(o.m_impl)
	, m_cur_index(o.m_cur_index) {
	m_impl->m_outer = this;
	o.m_block = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
}


stream_base_base & stream_base_base::operator=(stream_base_base && o) {
	if (m_impl) {
		if (m_impl->m_cur_block) {
			lock_t lock(m_impl->m_file->m_mut);
			m_impl->m_file->free_block(lock, m_impl->m_cur_block);
		}
		destroy_available_block();
		delete m_impl;
	}
	
	m_block = o.m_block;
	m_impl = o.m_impl;
	m_cur_index = o.m_cur_index;
	o.m_block = nullptr;
	o.m_impl = nullptr;
	o.m_cur_index = 0;
	return *this;
}


stream_base_base::~stream_base_base() {
	if (m_impl) {
		if (m_impl->m_cur_block) {
			lock_t lock(m_impl->m_file->m_mut);
			m_impl->m_file->free_block(lock, m_impl->m_cur_block);
		}
		destroy_available_block();
		delete m_impl;
	}
}


void stream_base_base::next_block() {
	m_impl->next_block();
}

void stream_base_base::seek(uint64_t off, whence w) {
	m_impl->seek(off, w);
}

void stream_impl::next_block() {
	lock_t lock(m_file->m_mut);
	block * buff = m_cur_block;
	if (buff == nullptr) m_cur_block = m_file->get_first_block(lock);
	else m_cur_block = m_file->get_successor_block(lock, buff);
	m_file->free_block(lock, buff);
	m_outer->m_cur_index = 0;
	m_outer->m_block = m_cur_block;
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

void stream_impl::seek(uint64_t offset, whence w) {
	log_info() << "STREM seek       " << offset << std::endl;
	lock_t lock(m_file->m_mut);
	
	stream_position p;

	if (m_file->m_direct) {
		uint64_t loc = 0;
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
		auto logical_block_size = block_size() / m_file->m_item_size;
		
		p.m_block = loc / logical_block_size;
		p.m_logical_offset = p.m_block * logical_block_size;
		p.m_index = loc - p.m_logical_offset;
		p.m_physical_offset = 4 + p.m_block * (sizeof(block_header) *2 + block_size());
		
	} else if (offset != 0 || (w != whence::set && w != whence::end)) {
		throw std::runtime_error("Arbetrery seek not supported for compressed or serialized files");
	} else if (w == whence::set) {
		p.m_block = 0;
		p.m_index = 0;
		p.m_logical_offset = 0;
		p.m_physical_offset = 0;
	} else if (m_file->m_last_block) {
		p.m_block = m_file->m_last_block->m_block;
		p.m_index = m_file->m_last_block->m_logical_size;
		p.m_logical_offset = m_file->m_last_block->m_logical_offset;
		p.m_physical_offset = m_file->m_last_block->m_physical_offset;
	} else {
		p = m_file->m_end_position;
	}
	set_position(lock, p);
}


