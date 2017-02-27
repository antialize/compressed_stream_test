// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_utils.h>
#include <file_stream_impl.h>
#include <cassert>
#include <snappy.h>
#include <atomic>

#ifndef NDEBUG
std::map<size_t, std::map<block_idx_t, std::pair<file_size_t, file_size_t>>> block_offsets;
#endif

std::queue<job> jobs;
mutex_t job_mutex;
std::condition_variable job_cond;

std::atomic_uint tid;

std::ostream & operator <<(std::ostream & o, const job_type t) {
	const char *s;
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

void update_next_block(lock_t & file_lock, unsigned int id, const job & j, file_size_t physical_offset) {
	block * b = j.buff;
	if (b->m_logical_size != b->m_maximal_logical_size) return;
	auto nb = j.file->get_available_block(file_lock, b->m_block + 1);
	if (nb) {
		log_info() << "JOB " << id << " update nb  " << *b << std::endl;
		nb->m_physical_offset = physical_offset;
		nb->m_cond.notify_all();
	}
}

void process_run() {
	auto id = tid.fetch_add(1);
	thread_local char _data1[1024*1024];
	thread_local char _data2[1024*1024];
	char * current_buffer = _data1;
	char * next_buffer = _data2;
	lock_t job_lock(job_mutex);
	log_info() << "JOB " << id << " start" << std::endl;
	while (true) {
		while (jobs.empty()) job_cond.wait(job_lock);
		auto j = jobs.front();
		// Don't pop the job as all threads should terminate
		if (j.type == job_type::term) {
			log_info() << "JOB " << id << " pop job    TERM\n";
			break;
		}
		block * b = nullptr;
		if (j.type == job_type::trunc) {
			log_info() << "JOB " << id << " pop job    " << j.truncate_size << " " << j.type << '\n';
		} else {
			b = j.buff;
			log_info() << "JOB " << id << " pop job    " << *b << " " << j.type << " " << b->m_logical_size << '\n';
		}

		jobs.pop();
		job_lock.unlock();

		auto file = j.file;
		lock_t file_lock(file->m_mut);

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

			// Not necessarily known in advance
			file_size_t logical_offset;
			block_size_t logical_size;
			block_size_t serialized_size;

			file_lock.unlock();

			assert(is_known(block));
			assert(is_known(physical_offset));

			if (!is_known(physical_size)) {
				block_header h;
				auto r = _pread(file->m_fd, &h, sizeof(block_header), physical_offset);
				assert(r == sizeof(block_header));
				physical_size = h.physical_size;
			}


			file_size_t off = physical_offset;
			file_size_t size = physical_size;
			if (block != 0 && !is_known(prev_physical_size)) { // NOT THE FIRST BLOCK
				off -= sizeof(block_header);
				size += sizeof(block_header);
			}

			// If the next block has never been written out to disk
			// we can't find its physical size
			bool is_last_block = block + 1 == blocks;
			bool should_read_next_physical_size = false;
			if (!is_last_block && !is_known(next_physical_size)) { // NOT THE LAST BLOCK
				auto it = file->m_block_map.find(block + 1);
				if (it != file->m_block_map.end()) {
					next_physical_size = it->second->m_physical_size;
				} else {
					size += sizeof(block_header);
					should_read_next_physical_size = true;
				}
			}

			log_info() << "JOB " << id << " pread      " << *b << " from " << off << " - " << (off + size - 1) << std::endl;

			char * physical_data = current_buffer;
			char * uncompressed_data = next_buffer;

			auto r = _pread(file->m_fd, physical_data, size, off);

			assert(r == size);

			if (block != 0 && !is_known(prev_physical_size)) {
				//log_info() << id << "read prev header" << std::endl;
				block_header h;
				memcpy(&h, physical_data, sizeof(block_header));
				physical_data += sizeof(block_header);
				prev_physical_size = h.physical_size;
			}

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

			physical_data += physical_size - sizeof(block_header); //Skip next block header
			if (should_read_next_physical_size) {
				block_header h;
				memcpy(&h, physical_data, sizeof(block_header));
				physical_data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}

			block_size_t compressed_size = physical_size - 2 * sizeof(block_header);

			size_t uncompressed_size;
			if (file->m_compressed) {
				bool ok = snappy::GetUncompressedLength(compressed_data, compressed_size, &uncompressed_size);
				assert(ok);
				assert(uncompressed_size <= sizeof(_data1));
				ok = snappy::RawUncompress(compressed_data, compressed_size, uncompressed_data);
				assert(ok);
			} else {
				uncompressed_data = compressed_data;
				uncompressed_size = compressed_size;
			}

			serialized_size = uncompressed_size;

			if (file->m_serialized) {
				block_size_t unserialized_size;
				file->m_outer->do_unserialize(uncompressed_data, logical_size, b->m_data, &unserialized_size);
				assert(unserialized_size == logical_size * file->m_item_size);
			} else {
				memcpy(b->m_data, uncompressed_data, uncompressed_size);
			}

			log_info() << "Read " << *b << '\n'
			 		   << "Logical size " << logical_size << '\n'
					   << "First data " << reinterpret_cast<int*>(b->m_data)[0]
					   << " " << reinterpret_cast<int*>(b->m_data)[1] << std::endl;

			file_lock.lock();

			// If the file is serialized and the current block is not
			// the last one we have to override its maximal_logical_size here
			// to get update_next_block to work as expected
			if (file->m_serialized && !is_last_block) {
				b->m_maximal_logical_size = logical_size;
			}

			update_next_block(file_lock, id, j, off + size);

			b->m_io = false;

			b->m_prev_physical_size = prev_physical_size;
			b->m_next_physical_size = next_physical_size;
			b->m_logical_size = logical_size;
			b->m_physical_size = physical_size;
			b->m_logical_offset = logical_offset;
			b->m_serialized_size = serialized_size;

			b->m_cond.notify_all();

			file->free_block(file_lock, b);
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

			file_lock.unlock();

			// TODO check if it is undefined behaiviure to change data underneeth snappy
			// TODO only used bytes here
			memcpy(current_buffer, b->m_data, unserialized_size);

			// TODO free the block here

			block_size_t serialized_size;

			if (file->m_serialized) {
				assert(b->m_serialized_size <= sizeof(_data1));
				file->m_outer->do_serialize(current_buffer, h.logical_size, next_buffer, &serialized_size);
				assert(serialized_size == b->m_serialized_size);
				std::swap(current_buffer, next_buffer);
			} else {
				serialized_size = unserialized_size;
			}

			size_t compressed_size;
			if (file->m_compressed) {
				assert(snappy::MaxCompressedLength(serialized_size) <= sizeof(_data1) - sizeof(block_header));
				snappy::RawCompress(current_buffer, serialized_size, next_buffer + sizeof(block_header), &compressed_size);
			} else {
				memcpy(next_buffer + sizeof(block_header), current_buffer, serialized_size);
				compressed_size = serialized_size;
			}
			char * physical_data = next_buffer;
			block_size_t bs = 2 * sizeof(block_header) + compressed_size;

			h.physical_size = (block_size_t)bs;
			memcpy(physical_data, &h, sizeof(block_header));
			memcpy(physical_data + sizeof(h) + compressed_size, &h, sizeof(block_header));

			file_lock.lock();
			log_info() << "JOB " << id << " compressed " << *b << " size " << bs << std::endl;

			if (!is_known(b->m_physical_offset)) {
				// If the previous block is doing io,
				// we have to wait for it to finish and update our blocks physical offset.
				// Even if we have a physical offset, the io operation from the previous block,
				// might change it.
				auto pb = file->get_available_block(file_lock, b->m_block - 1);
				assert(pb != nullptr);
				assert(pb->m_io);
				log_info() << "JOB " << id << " waitfor    " << *b << std::endl;
				// We can't use pb anymore as when unlocking the file lock,
				// it might be repurposed for another block id
				while (!is_known(b->m_physical_offset))
					b->m_cond.wait(file_lock);
			}

			file_size_t off = b->m_physical_offset;
			assert(is_known(off));

			file_lock.unlock();

			auto r = _pwrite(file->m_fd, physical_data, bs, off);
			assert(r == bs);
			log_info() << "JOB " << id << " written    " << *b << " at " <<  off << " - " << off + bs - 1 <<  " physical_size " << std::endl;

			file_lock.lock();

#ifndef NDEBUG
			{
				auto & offsets = block_offsets[file->m_file_id];
				if (b->m_block + 1 != file->m_blocks) {
					auto it = offsets.find(b->m_block + 1);
					if (it != offsets.end()) {
						assert(it->second.first == off + bs);
					}
				}
				auto it = offsets.find(b->m_block - 1);
				if (it != offsets.end() && is_known(it->second.second)) {
					assert(it->second.second == off);
				}

				offsets[b->m_block] = {off, off + bs};
			}
#endif

			update_next_block(file_lock, id, j, off + bs);

			b->m_physical_size = bs;
			b->m_io = false;

			file->update_physical_size(file_lock, b->m_block, bs);
			file->free_block(file_lock, b);
		}
		break;
		case job_type::trunc: {
			::ftruncate(file->m_fd, j.truncate_size);

			log_info() << "JOB " << id << " truncated  " << file->m_path << " to size " << j.truncate_size << std::endl;

#ifndef NDEBUG
			{
				auto & offsets = block_offsets[file->m_file_id];
				auto it = offsets.lower_bound(file->m_blocks - 1);
				if (it != offsets.end()) {
					if (it->second == std::make_pair(file->m_file_id, file->m_blocks - 1)) {
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
		file->m_job_cond.notify_one();
		if (b) b->m_cond.notify_all();
		job_lock.lock();
	}
	log_info() << "JOB " << id << " end" << std::endl;
}
