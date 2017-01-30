// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <file_utils.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

const uint64_t file_header::magicConst;
const uint64_t file_header::versionConst;

file_impl::file_impl()
	: m_outer(nullptr)
	, m_fd(-1)
	, m_file_id(file_ctr++)
	, m_last_block(nullptr)
	, m_blocks(0)
	, m_job_count(0)
	, m_first_physical_size(no_block_size)
	, m_last_physical_size(no_block_size)
	, m_item_size(no_block_size)
	, m_serialized(false) {
	create_available_block();
}

file_impl::~file_impl() {
	destroy_available_block();
}

file_base_base::file_base_base(bool serialized, block_size_t item_size)
	: m_impl(nullptr)
	, m_last_block(nullptr)
{
	auto impl = new file_impl();
	m_impl = impl;
	impl->m_outer = this;
	impl->m_item_size = item_size;
	impl->m_serialized = serialized;
}

file_base_base::file_base_base(file_base_base &&o)
	: m_impl(o.m_impl)
	, m_last_block(o.m_last_block)
{
	o.m_impl = nullptr;
	o.m_last_block = nullptr;
	m_impl->m_outer = this;
}

void file_base_base::open(const std::string & path, open_flags::open_flags flags, size_t max_user_data_size) {
	assert(!is_open());
	assert(!(flags & open_flags::read_only && flags & open_flags::truncate));

	int posix_flags = 0;
	if (flags & open_flags::read_only) {
		posix_flags |= O_RDONLY;
	} else if (flags & open_flags::truncate) {
		posix_flags |= O_CREAT | O_TRUNC | O_RDWR;
	} else {
		posix_flags |= O_CREAT | O_RDWR;
	}

	m_impl->m_readonly = flags & open_flags::read_only;
	m_impl->m_compressed = !(flags & open_flags::no_compress);

	int fd = ::open(path.c_str(), posix_flags, 00660);
	if (fd == -1)
		perror("open failed: ");

	lock_t l(m_impl->m_mut);
	m_impl->m_fd = fd;
	m_impl->m_file_id = file_ctr++;

	file_size_t fsize = (file_size_t)::lseek(fd, 0, SEEK_END);
	file_header & header = m_impl->m_header;
	if (fsize > 0) {
		assert(fsize >= sizeof(file_header));
		_pread(fd, &header, sizeof header, 0);
		if (header.magic != file_header::magicConst) {
			std::cout << file_header::magicConst;
			log_info() << "Header magic was wrong, expected " << file_header::magicConst
					   << ", got " << header.magic << "\n";
			abort();
		}
		if (header.version != file_header::versionConst) {
			log_info() << "Header version was wrong, expected " << file_header::versionConst
					   << ", got " << header.version << "\n";
			abort();
		}
		if (header.isCompressed != m_impl->m_compressed) {
			log_info() << "Opened file is " << (header.isCompressed? "": "not ") << "compressed"
					   << ", but file was opened with" << (m_impl->m_compressed? "": "out") << " compression\n";
			abort();
		}
		if (header.isSerialized != m_impl->m_serialized) {
			log_info() << "Opened file is " << (header.isSerialized? "": "not ") << "serialized"
					   << ", a " << (header.isSerialized? "": "non-") << "serialized file was required\n";
			abort();
		}

		assert(max_user_data_size == 0 || header.max_user_data_size == max_user_data_size);

		m_impl->m_blocks = header.blocks;

		if (header.blocks > 0) {
			assert(fsize >= sizeof(file_header) + header.max_user_data_size + 2 * sizeof(block_header));
			block_header last_header;
			_pread(fd, &last_header, sizeof last_header, fsize - sizeof last_header);

			stream_position p;
			p.m_block = header.blocks - 1;
			p.m_index = last_header.logical_size;
			p.m_logical_offset = last_header.logical_offset;
			p.m_physical_offset = fsize - last_header.physical_size;

			// This call sets m_last_block
			m_impl->get_block(l, p);
		} else {
			assert(fsize == sizeof(file_header) + header.max_user_data_size);
			// This call sets m_last_block
			m_impl->get_first_block(l);
		}
	} else {
		assert(!(flags & open_flags::read_only));

		::memset(&header, 0, sizeof header);
		header.magic = file_header::magicConst;
		header.version = file_header::versionConst;
		header.blocks = 0;
		header.user_data_size = 0;
		header.max_user_data_size = max_user_data_size;
		header.isCompressed = m_impl->m_compressed;
		header.isSerialized = m_impl->m_serialized;
		// This isn't really needed, because the header will be written when we close the file.
		// However if the file gets in an invalid state and we crash, it is nice to have a valid header.
		_pwrite(fd, &header, sizeof header, 0);

		void * zeros = calloc(max_user_data_size, 1);
		_pwrite(fd, zeros, max_user_data_size, sizeof header);
		free(zeros);

		// This call sets m_last_block
		m_impl->get_first_block(l);
	}
}

void file_base_base::close() {
	assert(is_open());
	lock_t l(m_impl->m_mut);

	// Wait for all jobs to be completed for this file
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	// Free all blocks, possibly creating some write jobs
	for (auto p : m_impl->m_block_map) {
		block *b = p.second;
		if (b->m_usage != 0) {
			// Make sure we release the block
			b->m_usage = 1;
			m_impl->free_block(l, b);
		}
	}

	// Wait for all freed dirty blocks to be written
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	m_last_block = m_impl->m_last_block = nullptr;

	// Kill all blocks
	// Note: kill_block invalidates it
	for (auto it = m_impl->m_block_map.begin(); it != m_impl->m_block_map.end();) {
		auto itnext = std::next(it);
		block *b = it->second;
		assert(b->m_usage == 0);
		m_impl->kill_block(l, b);
		it = itnext;
	}

	assert(m_impl->m_block_map.size() == 0);

	if (!m_impl->m_readonly) {
		// Write out header
		m_impl->m_header.blocks = m_impl->m_blocks;
		_pwrite(m_impl->m_fd, &m_impl->m_header, sizeof(file_header), 0);
	}

	::close(m_impl->m_fd);
	m_impl->m_fd = -1;

	m_impl->m_blocks = 0;
	m_impl->m_first_physical_size = no_block_size;
	m_impl->m_last_physical_size = no_block_size;

	m_impl->m_file_id = file_ctr++;
}

bool file_base_base::is_open() const noexcept {
	return m_impl->m_fd != -1;
}

bool file_base_base::is_readable() const noexcept {
	return true;
}

bool file_base_base::is_writable() const noexcept {
	return !m_impl->m_readonly;
}

size_t file_base_base::user_data_size() const noexcept {
	return m_impl->m_header.user_data_size;
}

size_t file_base_base::max_user_data_size() const noexcept {
	return m_impl->m_header.max_user_data_size;
}

void file_base_base::read_user_data(void *data, size_t count) {
	assert(count <= user_data_size());
	_pread(m_impl->m_fd, data, count, sizeof(file_header));
}

void file_base_base::write_user_data(const void *data, size_t count) {
	assert(count <= max_user_data_size());
	_pwrite(m_impl->m_fd, data, count, sizeof(file_header));
	m_impl->m_header.user_data_size = std::max(user_data_size(), count);
}

void file_impl::update_physical_size(lock_t & lock, block_idx_t block, block_size_t size) {
	if (block == 0) m_first_physical_size = size;
	else {
		auto it = m_block_map.find(block - 1);
		if (it != m_block_map.end()) it->second->m_next_physical_size = size;
	}
	if (block + 1 == m_blocks) m_last_physical_size = size;
	else {
		auto it = m_block_map.find(block + 1);
		if (it != m_block_map.end()) it->second->m_prev_physical_size = size;
	}
}

block * file_impl::get_block(lock_t & l, stream_position p, bool find_next, block * rel) {
	log_info() << "FILE  get_block  " << p.m_block << std::endl;

	block * b = get_available_block(l, p.m_block);
	if (b) {
		log_info() << "FILE  fetch      " << *b << std::endl;
		assert(b->m_block == p.m_block);
		block_ref_inc(l, b);
		return b;
	}
	
	l.unlock();
	auto buff = pop_available_block();
	l.lock();
	
	buff->m_file = this;
	buff->m_dirty = false;
	buff->m_block = p.m_block;
	buff->m_physical_offset = p.m_physical_offset;
	buff->m_logical_offset = p.m_logical_offset;
	buff->m_logical_size = no_block_size;
	buff->m_serialized_size = no_block_size;
	buff->m_usage = 1;
	buff->m_read = false;
	buff->m_io = false;
	buff->m_maximal_logical_size = block_size() / buff->m_file->m_item_size;
	m_block_map.emplace(buff->m_block, buff);

	// If we have to read the previous block we must know its offset
	if (!find_next) {
		if (!is_known(buff->m_physical_offset)) {
			log_info() << "Need to know offset for " << *buff << ", next block is " << *rel << "\n";
		}
		assert(is_known(buff->m_physical_offset));
	}

	// If we don't know this blocks offset,
	// the previous block must exist.
	// When we popped the available block, we had to unlock the file lock,
	// so the predecessor block might be done now, without having updated this blocks offset,
	// as this block was only just added to the block_map and so we need to update that now.
	// If not it will be updated later when the previous block is written to disk.
	if (!is_known(buff->m_physical_offset)) {
		assert(rel != nullptr);

		buff->m_physical_offset = get_next_physical_offset(l, rel);

		if (is_known(buff->m_physical_offset)) {
			log_info() << "\nUPDATED " << p.m_block << "\n\n";
		}
	}

	if (p.m_block == m_blocks) {
		assert(is_known(buff->m_logical_offset));
		++m_blocks;
		buff->m_logical_size = 0;
		buff->m_serialized_size = 0;
		buff->m_read = true;
	} else {
		log_info() << "FILE  read       " << *buff << std::endl;
		//We need to read stuff
		{
			m_job_count++;

			lock_t l2(job_mutex);
			job j;
			j.type = job_type::read;
			j.buff = buff;
			j.file = this;
			buff->m_usage++;
			buff->m_io = true;
			//log_info() << "read block " << *buff << std::endl;
			jobs.push(j);
			job_cond.notify_all();
		}

		while (!buff->m_read) buff->m_cond.wait(l);
	}
  
	if (p.m_block + 1 == m_blocks) {
		//log_info() << "Setting last block to " << buff << std::endl;
		if (m_last_block != buff) {
			free_block(l, m_last_block);
		}
		m_outer->m_last_block = m_last_block = buff;
		buff->m_usage++;
	}
   
	//log_info() << "get succ " << *buff << std::endl;
	return buff;
}
	

block * file_impl::get_successor_block(lock_t & l, block * t) {
	stream_position p;
	p.m_block = t->m_block + 1;
	p.m_index = 0;
	p.m_logical_offset = t->m_logical_offset + t->m_maximal_logical_size;
	p.m_physical_offset = get_next_physical_offset(l, t);
	return get_block(l, p, true, t);
}

block * file_impl::get_predecessor_block(lock_t & l, block * t) {
	stream_position p;
	p.m_block = t->m_block - 1;
	p.m_index = 0;
	// We can't assume that all blocks have same max logical size,
	// because of serialization
	p.m_logical_offset = no_file_size;
	p.m_physical_offset = get_prev_physical_offset(l, t);
	return get_block(l, p, false, t);
}


void file_impl::free_block(lock_t & l, block * t) {
	if (t == nullptr) return;
	assert(t->m_usage != 0);
	--t->m_usage;

	// Even if t->m_usage > 1, we need to write the block if it's dirty and its full
	// This is not a problem as we only support appending to a file
	if (t->m_dirty && (t->m_usage == 0 || t->m_logical_size == t->m_maximal_logical_size)) {

		//TODO check that we are allowed to write to this block
		
		log_info() << "      free block " << *t << " write" << std::endl;

		m_job_count++;

		// Write dirty block
		lock_t l2(job_mutex);

		job j;
		j.type = job_type::write;
		j.buff = t;
		j.file = this;
		t->m_usage++;
		t->m_dirty = false;
		t->m_io = true;
		//log_info() << "write block " << *t << std::endl;
		jobs.push(j);
		job_cond.notify_all();

		return;
	}

	if (t->m_usage != 0) return;

	if (is_known(t->m_physical_offset)) {
		log_info() << "      free block " << *t << " avail" << std::endl;
		//log_info() << "avail block " << *t << std::endl;

		// If this is the last block and it's size is 0
		// We shouldn't count this block
		if (t->m_file->m_last_block == t && t->m_logical_size == 0) {
			t->m_file->m_blocks--;
		}

		push_available_block(t);
	}
}

void file_impl::kill_block(lock_t & l, block * t) {
	log_info() << "      kill block " << *t << std::endl;
	assert(t->m_usage == 0);
	assert(t->m_file == this);
	assert(is_known(t->m_logical_offset));
	assert(is_known(t->m_physical_offset));
	assert(is_known(t->m_logical_size));
	assert(is_known(t->m_physical_size) || t->m_logical_size == 0);
	assert(is_known(t->m_serialized_size));
	// The predecessor to this block might have appeared after writing this block
	// so we need to tell it our size, so that we can read_back later
	update_physical_size(l, t->m_block, t->m_physical_size);

	if (t->m_file->m_serialized)
		m_outer->do_destruct(t->m_data, t->m_logical_size);

	size_t c = m_block_map.erase(t->m_block);
	assert(c == 1);
	t->m_file = nullptr;
}
