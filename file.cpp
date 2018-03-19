// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <file_utils.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <cstring>

const uint64_t file_header::magicConst;
const uint64_t file_header::versionConst;

file_impl::file_impl(file_base_base * outer, bool serialized, block_size_t item_size)
	: m_outer(outer)
	, m_fd(-1)
#ifndef NDEBUG
	, m_file_id(file_ctr++)
#endif
	, m_last_block(nullptr)
	, m_blocks(0)
	, m_job_count(0)
	, m_item_size(item_size)
	, m_serialized(serialized) {
}

file_base_base::~file_base_base() {
	delete m_impl;
}

file_base_base::file_base_base(bool serialized, block_size_t item_size)
	: m_impl(nullptr)
{
	m_impl = new file_impl(this, serialized, item_size);
}

file_base_base::file_base_base(file_base_base && o)
	: m_impl(o.m_impl)
{
	impl_changed();

	o.m_impl = nullptr;
}

file_base_base & file_base_base::operator=(file_base_base && o) {
	assert(this != &o);
	delete m_impl;

	m_impl = o.m_impl;

	impl_changed();

	o.m_impl = nullptr;

	return *this;
}

void file_base_base::impl_changed() {
	if (!m_impl) return;

	m_impl->m_outer = this;
	for (auto s : m_impl->m_streams) {
		s->m_file = m_impl;
		s->m_outer->m_file_base = this;
	}
}

void file_base_base::open(const std::string & path, open_flags::open_flags flags, size_t max_user_data_size) {
	if (is_open())
		throw exception("File is already open");
	if ((flags & open_flags::read_only) && (flags & open_flags::truncate))
		throw exception("Can't open file as truncated with read only flag");

	m_impl->m_path = path;

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
	m_impl->m_readahead = !(flags & open_flags::no_readahead);

	int fd = ::open(path.c_str(), posix_flags, 00660);
	if (fd == -1)
		throw exception("Failed to open file: " + std::string(std::strerror(errno)));

	lock_t l(job_mutex);
	m_impl->m_fd = fd;

#ifndef NDEBUG
	m_impl->m_file_id = file_impl::file_ctr++;
#endif

	file_size_t fsize = (file_size_t)::lseek(fd, 0, SEEK_END);
	file_header & header = m_impl->m_header;
	if (fsize > 0) {
		if (fsize < sizeof(file_header))
			throw exception("Invalid TPIE file (too small)");
		_pread(fd, &header, sizeof header, 0);
		if (header.magic != file_header::magicConst) {
			log_info() << "Header magic was wrong, expected " << file_header::magicConst
					   << ", got " << header.magic << "\n";
			throw exception("Invalid TPIE file (wrong magic)");
		}
		if (header.version != file_header::versionConst) {
			log_info() << "Header version was wrong, expected " << file_header::versionConst
					   << ", got " << header.version << "\n";
			throw exception("Invalid TPIE file (wrong version)");
		}
		if (header.isCompressed != m_impl->m_compressed) {
			log_info() << "Opened file is " << (header.isCompressed? "": "not ") << "compressed"
					   << ", but file was opened with" << (m_impl->m_compressed? "": "out") << " compression\n";
			throw exception("Invalid TPIE file (wrong compression)");
		}
		if (header.isSerialized != m_impl->m_serialized) {
			log_info() << "Opened file is " << (header.isSerialized? "": "not ") << "serialized"
					   << ", a " << (header.isSerialized? "": "non-") << "serialized file was required\n";
			throw exception("Invalid TPIE file (wrong serialized)");
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

			m_impl->m_end_position = p;
		} else {
			assert(fsize == sizeof(file_header) + header.max_user_data_size);

			m_impl->m_end_position = m_impl->start_position();
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

		m_impl->m_end_position = m_impl->start_position();
	}
}

void file_base_base::close() {
	if (!is_open())
		throw exception("File is already closed");

	lock_t l(job_mutex);

	// Wait for all jobs to be completed for this file
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	if (!m_impl->m_streams.empty())
		throw exception("Tried to close a file with open streams");

	// Kill all blocks
	m_impl->foreach_block([&](block * b){
		assert(b->m_usage == 0);
		m_impl->kill_block(l, b);
	});

	assert(m_impl->m_block_map.size() == 0);

	if (!m_impl->m_readonly) {
		// Write out header
		m_impl->m_header.blocks = m_impl->m_blocks;
		_pwrite(m_impl->m_fd, &m_impl->m_header, sizeof(file_header), 0);
	}

	::close(m_impl->m_fd);
	m_impl->m_fd = -1;
	m_impl->m_path = "";

	m_impl->m_blocks = 0;

#ifndef NDEBUG
	m_impl->m_file_id = file_impl::file_ctr++;
#endif
}

bool file_base_base::is_open() const noexcept {
	if (!m_impl) return false;
	return m_impl->m_fd != -1;
}

bool file_base_base::is_readable() const noexcept {
	return true;
}

bool file_base_base::is_writable() const noexcept {
	return !m_impl->m_readonly;
}

bool file_base_base::direct() const noexcept {
	return m_impl->direct();
}

size_t file_base_base::user_data_size() const noexcept {
	return m_impl->m_header.user_data_size;
}

size_t file_base_base::max_user_data_size() const noexcept {
	return m_impl->m_header.max_user_data_size;
}

void file_base_base::read_user_data(void *data, size_t count) {
	assert(is_open());
	assert(count <= user_data_size());
	_pread(m_impl->m_fd, data, count, sizeof(file_header));
}

void file_base_base::write_user_data(const void *data, size_t count) {
	assert(is_open() && is_readable());
	assert(count <= max_user_data_size());
	_pwrite(m_impl->m_fd, data, count, sizeof(file_header));
	m_impl->m_header.user_data_size = std::max(user_data_size(), count);
}

const std::string &file_base_base::path() const noexcept {
	return m_impl->m_path;
}

void file_base_base::truncate(stream_position pos) {
	assert(is_open() && is_writable());
	lock_t l(job_mutex);

	block * new_last_block = m_impl->get_block(l, pos);
	assert(new_last_block->m_logical_offset == pos.m_logical_offset);
	assert(is_known(new_last_block->m_physical_offset));

	// Wait for all jobs to be completed for this file
	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	// Make sure no one uses blocks past this one and kill them all
	// First free all readahead blocks...
	for (stream_impl * s : m_impl->m_streams) {
		block *b = s->m_readahead_block;
		if (b && b->m_block > pos.m_block) {
			m_impl->free_readahead_block(l, b);
			s->m_readahead_block = nullptr;
		}
	}

	// ... then kill all others
	m_impl->foreach_block([&](block * b){
		if (b->m_block > pos.m_block) {
			assert(b->m_readahead_usage == 0);
			if (b->m_usage != 0) {
				throw exception("Trying to truncate before an open stream's position");
			} else {
				m_impl->kill_block(l, b);
			}
		}
	});

	// Write the new last block if needed
	new_last_block->m_usage++;
	m_impl->free_block(l, new_last_block);
	while (new_last_block->m_io) new_last_block->m_cond.wait(l);

	block * old_last_block = m_impl->m_last_block;
	unused(old_last_block);

	m_impl->m_last_block = new_last_block;

	m_impl->m_blocks = pos.m_block + 1;

	assert(pos.m_index <= new_last_block->m_logical_size);
	block_size_t truncated_items = new_last_block->m_logical_size - pos.m_index;

	new_last_block->m_logical_size = pos.m_index;

	file_size_t truncate_size;

	// If truncated_items == 0, then we are truncating on a block boundary
	if (truncated_items != 0) {
		// We need to remove some items from the block
		if (m_impl->m_serialized) {
			do_destruct(new_last_block->m_data + m_impl->m_item_size * pos.m_index, truncated_items);
			// Update serialized size
			do_serialize(new_last_block->m_data, pos.m_index, nullptr, &new_last_block->m_serialized_size);
		}
		// We don't know this blocks physical_size and we mark it as dirty
		// We don't actually write this block yet, but just truncate the file to only include the previous blocks
		new_last_block->m_physical_size = no_block_size;
		m_impl->update_related_physical_sizes(l, new_last_block);
		new_last_block->m_dirty = true;

		truncate_size = new_last_block->m_physical_offset;
	} else {
		// Two possibilities, either the block is empty and b->m_logical_size = pos.m_index = 0
		// or the block is full and we're at the past the end of the block
		// In the first case we should truncate the block, in the last case we should include it
		if (pos.m_index == 0) {
			truncate_size = new_last_block->m_physical_offset;
		} else {
			// We have freed this block, so it should have been written out if
			// it was dirty and had reached its maximal size
			// If it has not reached its maximal size, this must be the last block
			// and so we shouldn't actually truncate at all
			if (new_last_block->m_logical_size < new_last_block->m_maximal_logical_size) {
				assert(new_last_block == old_last_block);
				assert(pos.m_logical_offset + pos.m_index == size());
				m_impl->free_block(l, new_last_block);
				return;
			} else {
				assert(!new_last_block->m_dirty);
				truncate_size = new_last_block->m_physical_offset + new_last_block->m_physical_size;
			}
		}
	}

	log_info() << "FILE  trunc       " << truncate_size << std::endl;
	{
		m_impl->m_job_count++;

		job j;
		j.type = job_type::trunc;
		j.file = m_impl;
		j.truncate_size = truncate_size;
		jobs.push(j);
		job_cond.notify_all();
	}

	while (m_impl->m_job_count) m_impl->m_job_cond.wait(l);

	// We can only free the new last block after the file has been truncated
	m_impl->free_block(l, new_last_block);
}

void file_base_base::truncate(file_size_t offset) {
	stream_position p;
	{
		lock_t l(job_mutex);
		p = m_impl->position_from_offset(l, offset);
	}
	truncate(p);
}

file_size_t file_base_base::size() const noexcept {
	if (!m_impl->m_last_block)
		return m_impl->m_end_position.m_logical_offset + m_impl->m_end_position.m_index;
	return m_impl->m_last_block->m_logical_offset + m_impl->m_last_block->m_logical_size;
}

#ifndef NDEBUG
size_t file_impl::file_ctr = 0;
#endif

stream_position file_impl::position_from_offset(lock_t &l, file_size_t offset) {
	stream_position p;
	if (direct()) {
		auto logical_block_size = block_size() / m_item_size;

		p.m_block = offset / logical_block_size;
		p.m_logical_offset = p.m_block * logical_block_size;
		p.m_index = offset - p.m_logical_offset;
		p.m_physical_offset = start_position().m_physical_offset + p.m_block * (sizeof(block_header) * 2 + block_size());
	} else if (offset == 0) {
		p = start_position();
	} else if (offset != m_outer->size()) {
		p = end_position(l);
	} else {
		throw std::runtime_error("Arbitrary offset find not supported for compressed or serialized files");
	}
	return p;
}

template <typename T1, typename T2>
void update_if_known(T1 & val, T2 new_val) {
	if (is_known(new_val)) {
		assert(!is_known(val) || val == new_val);
		val = new_val;
	}
}

void file_impl::update_related_physical_sizes(lock_t & l, block * b) {
	if (b->m_block != 0) { // Not first block
		auto pb = get_available_block(l, b->m_block - 1);
		if (pb) {
			update_if_known(pb->m_next_physical_size, b->m_physical_size);
			update_if_known(pb->m_physical_size, b->m_prev_physical_size);

			auto prev_offset = get_prev_physical_offset(l, b);
			update_if_known(pb->m_physical_offset, prev_offset);
		}
	}

	if (b->m_block + 1 != m_blocks) { // Not last block
		auto nb = get_available_block(l, b->m_block + 1);
		if (nb) {
			update_if_known(nb->m_prev_physical_size, b->m_physical_size);
			update_if_known(nb->m_physical_size, b->m_next_physical_size);

			auto next_offset = get_next_physical_offset(l, b);
			update_if_known(nb->m_physical_offset, next_offset);
		}
	}
}

block * file_impl::get_block(lock_t & l, stream_position p, bool find_next, block * rel) {
	log_info() << "FILE  get_block  " << p.m_block << std::endl;

	block * b = get_available_block(l, p.m_block);
	if (b) {
		log_info() << "FILE  fetch      " << *b << std::endl;
		assert(b->m_block < m_blocks);
		assert(b->m_block == p.m_block);
		block_ref_inc(l, b);

		if (b->m_block + 1 == m_blocks)
			assert(m_last_block == b);

		// If the file is direct and it is writable
		// we must wait for it to finish writing
		// as the write job may use the block's buffer
		// even after it has released the lock.
		// This doesn't apply to non-direct files as they are append-only
		if (direct() && m_outer->is_writable()) {
			while (b->m_io) b->m_cond.wait(l);
		}

		return b;
	}
	
	l.unlock();
	auto buff = pop_available_block();
	l.lock();
	
	buff->m_file = this;
	buff->m_dirty = false;
	buff->m_block = p.m_block;
	if (direct()) {
		auto p2 = position_from_offset(l, p.m_logical_offset + p.m_index);

		if (p.m_index == block_size() / m_item_size) {
			assert(p2.m_index == 0);
			assert(p2.m_block == p.m_block + 1);
			assert(p2.m_logical_offset == p.m_logical_offset + p.m_index);
			assert(is_known(p.m_physical_offset));
		} else {
			assert(p.m_index == p2.m_index);
			assert(p.m_logical_offset == p2.m_logical_offset);
			assert(p.m_block == p2.m_block);
			if (is_known(p.m_physical_offset)) {
				assert(p.m_physical_offset == p2.m_physical_offset);
			} else {
				p.m_physical_offset = p2.m_physical_offset;
			}
		}
		buff->m_physical_offset = p.m_physical_offset;
	} else {
		buff->m_physical_offset = p.m_physical_offset;
	}
	buff->m_logical_offset = p.m_logical_offset;
	buff->m_logical_size = no_block_size;
	buff->m_serialized_size = no_block_size;
	buff->m_usage = 1;
	buff->m_io = false;
	buff->m_maximal_logical_size = block_size() / m_item_size;
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

	// If empty block at end of file
	if (p.m_block == m_blocks) {
		assert(is_known(buff->m_logical_offset));
		++m_blocks;
		buff->m_logical_size = 0;
		buff->m_serialized_size = 0;
	} else {
		log_info() << "FILE  read       " << *buff << std::endl;
		//We need to read stuff
		{
			m_job_count++;

			job j;
			j.type = job_type::read;
			j.buff = buff;
			j.file = this;
			buff->m_usage++;
			assert(!buff->m_io);
			buff->m_io = true;
			//log_info() << "read block " << *buff << std::endl;
			jobs.push(j);
			job_cond.notify_all();
		}

		while (buff->m_io) buff->m_cond.wait(l);
	}

	if (p.m_block + 1 == m_blocks)
		m_last_block = buff;

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
	p.m_logical_offset = direct()? t->m_logical_offset - t->m_maximal_logical_size: no_file_size;
	p.m_physical_offset = get_prev_physical_offset(l, t);
	return get_block(l, p, false, t);
}

void file_impl::free_readahead_block(lock_t & l, block * t) {
	if (t == nullptr) return;
	assert(t->m_readahead_usage != 0);
	t->m_readahead_usage--;
	free_block(l, t);
}

void file_impl::free_block(lock_t & l, block * t) {
	if (t == nullptr) return;
	assert(t->m_usage != 0);
	--t->m_usage;

	// We should only write a block to disk if the following are all true:
	// - It should be dirty.
	// - It should have size greater than 0, as a dirty block can only have size 0
	//   if it has been truncated. In this case, we shouldn't write the block to disk.
	// - One of the following holds:
	//   - Its usage is 0
	//   - It is full and the file is not direct. In this case we need to write it
	//     to get the next blocks physical size.
	if (t->m_dirty &&
		t->m_logical_size != 0 &&
		(t->m_usage == 0 || (!direct() && t->m_logical_size == t->m_maximal_logical_size))) {
		assert(m_outer->is_writable());

		if (direct()) {
			t->m_physical_size = m_item_size * t->m_logical_size + 2 * sizeof(block_header);
			update_related_physical_sizes(l, t);
		}

		log_info() << "      free block " << *t << " write" << std::endl;

		m_job_count++;

		// Write dirty block
		job j;
		j.type = job_type::write;
		j.buff = t;
		j.file = this;
		t->m_usage++;
		t->m_dirty = false;
		assert(!t->m_io);
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

		push_available_block(t);

		// If this is the last block and it's size is 0
		// We shouldn't count this block and we don't want to reuse it later
		if (m_last_block == t && t->m_logical_size == 0) {
			m_blocks--;
			kill_block(l, t);
		}
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
	assert(!m_serialized || is_known(t->m_serialized_size));
	// The predecessor to this block might have appeared after writing this block
	// so we need to tell it our size, so that we can read_back later
	update_related_physical_sizes(l, t);

	if (m_serialized)
		m_outer->do_destruct(t->m_data, t->m_logical_size);

	// If the last block is killed, we need to set m_end_position
	if (t == m_last_block) {
		stream_position p;
		p.m_block = t->m_block;
		p.m_index = t->m_logical_size;
		p.m_logical_offset = t->m_logical_offset;
		p.m_physical_offset = t->m_physical_offset;

		m_end_position = p;
		m_last_block = nullptr;
	}

	size_t c = m_block_map.erase(t->m_block);
	assert(c == 1);
	unused(c);
	t->m_file = nullptr;
}
