// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_utils.h>
#include <file_stream_impl.h>
#include <cassert>
#include <snappy.h>
#include <atomic>

#ifndef NDEBUG
std::atomic_int64_t total_blocks_read, total_blocks_written, total_bytes_read, total_bytes_written;
int64_t get_total_blocks_read() {
	return total_blocks_read;
}
int64_t get_total_blocks_written() {
	return total_blocks_written;
}
int64_t get_total_bytes_read() {
	return total_bytes_read;
}
int64_t get_total_bytes_written() {
	return total_bytes_written;
}
std::map<size_t, std::map<block_idx_t, std::pair<file_size_t, file_size_t>>> block_offsets;
#endif

std::queue<job> jobs;
mutex_t global_mutex;
cond_t global_cond;

std::atomic_uint tid;

std::ostream & operator <<(std::ostream & o, const job_type t) {
	const char *s = nullptr;
	switch (t) {
	case job_type::write:
		s = "write";
		break;
	case job_type::read:
		s = "read";
		break;
	case job_type::trunc:
		s = "trunc";
		break;
	case job_type::term:
		s = "term";
		break;
	}
	return o << s;
}

void process_run() {
	auto id = tid.fetch_add(1);

	const size_t extra_before_buffer = 2 * sizeof(block_header);
	const size_t max_buffer_size = snappy::MaxCompressedLength(max_serialized_block_size()) + 2 * sizeof(block_header);
	char * _data1 = new char[extra_before_buffer + max_buffer_size];
	char * _data2 = new char[extra_before_buffer + max_buffer_size];

	char * buffer1 = _data1 + extra_before_buffer;
	char * buffer2 = _data2 + extra_before_buffer;

	lock_t job_lock(global_mutex);
	log_info() << "JOB " << id << " start" << std::endl;
	while (true) {
		while (jobs.empty()) global_cond.wait(job_lock);
		auto j = jobs.front();
		// Don't pop the job as all threads should terminate
		if (j.type == job_type::term) {
			log_info() << "JOB " << id << " pop job    TERM\n";
			break;
		}
		block * b = nullptr;
		log_info() << "JOB " << id << " pop job    " << j.type << " ";
		if (j.type == job_type::trunc) {
			log_info() << j.truncate_size;
		} else {
			b = j.io_block;
			log_info() << *b;

			if (j.type == job_type::write)
				log_info() << " " << b->m_logical_size;
		}
		log_info() << "\n";

		jobs.pop();

		auto file = j.file;

		assert(j.type == job_type::trunc || b->m_usage != 0);

		switch (j.type) {
		case job_type::term:
		{
			assert(false);
			break;
		}
		case job_type::read:
		{
			block_idx_t block = b->m_block;
			file_size_t physical_offset = b->m_physical_offset;
			block_size_t physical_size = b->m_physical_size;
			block_size_t prev_physical_size = b->m_prev_physical_size;
			block_size_t next_physical_size = b->m_next_physical_size;
			auto blocks = file->m_blocks;

			job_lock.unlock();

			assert(is_known(block));
			assert(is_known(physical_offset));

			if (!is_known(physical_size)) {
				block_header h;
				auto r = _pread(file->m_fd, &h, sizeof(block_header), physical_offset);
				assert(r == sizeof(block_header));
				unused(r);
#ifndef NDEBUG
				total_bytes_read += sizeof(block_header);
#endif
				physical_size = h.physical_size;
			}


			file_size_t read_off = physical_offset;
			file_size_t read_size = physical_size;
			bool read_prev_header = block != 0 && !is_known(prev_physical_size);
			if (read_prev_header) { // NOT THE FIRST BLOCK
				read_off -= sizeof(block_header);
				read_size += sizeof(block_header);
			}

			bool is_last_block = block + 1 == blocks;
			bool read_next_header = !is_last_block && !is_known(next_physical_size);
			if (read_next_header) {
				read_size += sizeof(block_header);
			}

			log_info() << "JOB " << id << " pread      " << *b << " from " << read_off << " - " << (read_off + read_size - 1) << std::endl;

			char * physical_data;
			if (file->direct()) {
				physical_data = b->m_data;
			} else {
				physical_data = buffer1;
			}

			physical_data -= (read_prev_header? 2: 1) * sizeof(block_header);

			char * uncompressed_data;
			if (file->m_compressed && !file->m_serialized) {
				uncompressed_data = b->m_data;
			} else {
				uncompressed_data = buffer2;
			}

			auto r = _pread(file->m_fd, physical_data, read_size, read_off);
			assert(r == static_cast<ssize_t>(read_size));
			unused(r);
#ifndef NDEBUG
			total_bytes_read += read_size;
#endif

			if (read_prev_header) {
				//log_info() << id << "read prev header" << std::endl;
				block_header h;
				memcpy(&h, physical_data, sizeof(block_header));
				physical_data += sizeof(block_header);
				prev_physical_size = h.physical_size;
			}

			file_size_t logical_offset;
			block_size_t logical_size;
			{
				block_header h;
				memcpy(&h, physical_data, sizeof(block_header));
				//log_info() << id << "Read current header " << physical_size << " " << h.physical_size << std::endl;

				physical_data += sizeof(block_header);
				assert(physical_size == h.physical_size);
				logical_size = h.logical_size;
				logical_offset = h.logical_offset;
			}

			char * compressed_data = physical_data;

			physical_data += physical_size - sizeof(block_header);
			if (read_next_header) {
				block_header h;
				memcpy(&h, physical_data, sizeof(block_header));
				assert(h.logical_offset == logical_offset + logical_size);
				physical_data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}

			block_size_t compressed_size = physical_size - 2 * sizeof(block_header);

			size_t uncompressed_size;
			if (file->m_compressed) {
				bool ok = snappy::GetUncompressedLength(compressed_data, compressed_size, &uncompressed_size);
				assert(ok);
				unused(ok);
				assert(uncompressed_size <= max_buffer_size);
				ok = snappy::RawUncompress(compressed_data, compressed_size, uncompressed_data);
				assert(ok);
			} else {
				uncompressed_data = compressed_data;
				uncompressed_size = compressed_size;
			}

			block_size_t serialized_size = uncompressed_size;

			if (file->m_serialized) {
				block_size_t unserialized_size;
				file->do_unserialize(uncompressed_data, logical_size, b->m_data, &unserialized_size);
				assert(unserialized_size == logical_size * file->m_item_size);
			}

			log_info() << "Read " << *b << '\n'
			 		   << "Logical size " << logical_size << '\n'
					   << "First data " << reinterpret_cast<int*>(b->m_data)[0]
					   << " " << reinterpret_cast<int*>(b->m_data)[1] << std::endl;

			job_lock.lock();

			// If the file is serialized and the current block is not
			// the last one we have to override its maximal_logical_size here
			// as we can only append
			if (file->m_serialized && !is_last_block) {
				b->m_maximal_logical_size = logical_size;
			}

			b->m_io = false;

			b->m_prev_physical_size = prev_physical_size;
			b->m_next_physical_size = next_physical_size;
			b->m_logical_size = logical_size;
			b->m_physical_size = physical_size;
			b->m_logical_offset = logical_offset;
			b->m_serialized_size = serialized_size;

			file->update_related_physical_sizes(job_lock, b);

			file->free_block(job_lock, b);

#ifndef NDEBUG
			total_blocks_read++;
#endif
		}
		break;
		case job_type::write:
		{
			block_size_t unserialized_size = b->m_logical_size * file->m_item_size;

			block_header h;
			h.logical_size = b->m_logical_size;
			h.logical_offset = b->m_logical_offset;
			log_info() << "JOB " << id << " compress   " << *b << " size " << unserialized_size << '\n'
					   << "First data " << reinterpret_cast<int*>(b->m_data)[0]
					   << " " << reinterpret_cast<int*>(b->m_data)[1] << std::endl;

			job_lock.unlock();

			char * unserialized_data = b->m_data;
			char * serialized_data = buffer1;
			char * physical_data = buffer2;

			block_size_t serialized_size;
			if (file->m_serialized) {
				assert(b->m_serialized_size <= max_buffer_size);
				file->do_serialize(unserialized_data, h.logical_size, serialized_data, &serialized_size);
				assert(serialized_size == b->m_serialized_size);
			} else {
				serialized_size = unserialized_size;
				serialized_data = unserialized_data;
			}

			size_t compressed_size;
			if (file->m_compressed) {
				assert(snappy::MaxCompressedLength(serialized_size) <= max_buffer_size - sizeof(block_header));
				snappy::RawCompress(serialized_data, serialized_size, physical_data + sizeof(block_header), &compressed_size);
			} else {
				// This is a valid pointer as both the block's m_data
				// and our own buffers have block_header padding
				physical_data = serialized_data - sizeof(block_header);
				compressed_size = serialized_size;
			}

			block_size_t physical_size = 2 * sizeof(block_header) + compressed_size;

			h.physical_size = physical_size;
			memcpy(physical_data, &h, sizeof(block_header));
			memcpy(physical_data + sizeof(h) + compressed_size, &h, sizeof(block_header));

			log_info() << "JOB " << id << " compressed " << *b << " size " << physical_size << std::endl;

			while (!is_known(b->m_physical_offset)) {
				// Spin lock
			}

			file_size_t off = b->m_physical_offset;
			assert(is_known(off));
			unused(off);

			auto r = _pwrite(file->m_fd, physical_data, physical_size, off);
			assert(r == physical_size);
			unused(r);
#ifndef NDEBUG
			total_bytes_written += physical_size;
#endif

			job_lock.lock();

			log_info() << "JOB " << id << " written    " << *b << " at " <<  off << " - " << off + physical_size - 1 <<  " physical_size " << std::endl;

#ifndef NDEBUG
			{
				auto & offsets = block_offsets[file->m_file_id];
				if (b->m_block + 1 != file->m_blocks) {
					auto it = offsets.find(b->m_block + 1);
					if (it != offsets.end()) {
						assert(it->second.first == off + physical_size);
					}
				}
				auto it = offsets.find(b->m_block - 1);
				if (it != offsets.end() && is_known(it->second.second)) {
					assert(it->second.second == off);
				}

				offsets[b->m_block] = {off, off + physical_size};
			}
#endif

			b->m_io = false;

			b->m_physical_size = physical_size;
			file->update_related_physical_sizes(job_lock, b);
			file->free_block(job_lock, b);

#ifndef NDEBUG
			total_blocks_written++;
#endif
		}
		break;
		case job_type::trunc: {
			assert(is_known(j.truncate_size));

			int r = ::ftruncate(file->m_fd, j.truncate_size);
			assert(r == 0);
			unused(r);

			log_info() << "JOB " << id << " truncated  " << file->m_path << " to size " << j.truncate_size << std::endl;

#ifndef NDEBUG
			{
				auto & offsets = block_offsets[file->m_file_id];
				auto it = offsets.lower_bound(file->m_blocks - 1);
				if (it != offsets.end()) {
					if (it->first == file->m_blocks - 1) {
						it->second.second = no_file_size;
						it = std::next(it);
					}
					offsets.erase(it, offsets.end());
				}
			}
#endif
		}
		break;
		}

		file->m_job_count--;
		global_cond.notify_all();
	}
	delete[] _data1;
	delete[] _data2;
	log_info() << "JOB " << id << " end" << std::endl;
}
