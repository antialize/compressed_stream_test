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

stream_base_base::~stream_base_base() {
	if (m_impl) {
		if (m_impl->m_cur_block) {
			lock_t lock(m_impl->m_file->m_mut);
			m_impl->m_file->free_block(lock, m_impl->m_cur_block);
		}
		destroy_available_block();
	}
	delete m_impl;
}


void stream_base_base::next_block() {
	m_impl->next_block();
}

void stream_base_base::seek(uint64_t off) {
	m_impl->seek(off);
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

void stream_impl::seek(uint64_t offset) {
	log_info() << "STREM seek       " << offset << std::endl;
	lock_t lock(m_file->m_mut); 
	if (offset != 0)
		throw std::runtime_error("Not supported");
    
	if (m_cur_block)
		m_file->free_block(lock, m_cur_block);
	
	m_cur_block = nullptr;
	m_outer->m_cur_index = 0;
	m_outer->m_block = &void_block;
	
	assert(void_block.m_logical_offset == 0);
	assert(void_block.m_logical_size == 0);
	assert(void_block.m_maximal_logical_size == 0);
}

