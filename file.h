// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <buffer.h>
#include <map>



class file {
public:
	int m_fd;
	
	void open(std::string path);  
	
	mutex_t m_mut;
	std::map<uint64_t, block *> blocks;
	
	block * m_last_block; // A pointer to the last active block
	uint64_t m_logical_size; // The logical size of the file if m_last_block is nullptr
	uint64_t m_blocks; //The number of blocks in the file

	uint32_t m_first_physical_size;
	uint32_t m_last_physical_size;	
	
	file()
		: m_last_block(nullptr)
		, m_logical_size(0)
		, m_blocks(0)
		, m_first_physical_size(no_block_size)
		, m_last_physical_size(no_block_size) {}
	
	uint64_t size() {
		if (m_last_block == nullptr) return m_logical_size;
		return m_last_block->m_logical_offset + m_last_block->m_logical_size;
	}


	void update_physical_size(lock_t &, uint64_t block, uint32_t size);	
	
	block * get_successor_block(lock_t &, block * t);
	block * get_predecessor_block(lock_t &, block * t);
	void free_block(lock_t &, block * t);
	
	block * get_first_block(lock_t &);
	block * get_last_block(lock_t &);
};
