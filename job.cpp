// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <queue>
#include <cassert>
#include <unistd.h>
#include <snappy.h>
#include <atomic>

std::queue<job> jobs;
mutex_t job_mutex;
std::condition_variable job_cond;

std::atomic_uint tid;

ssize_t _pread(int fd, void *buf, size_t count, off_t offset) {
	char * cbuf = static_cast<char *>(buf);
	ssize_t i = 0;
	do {
		ssize_t r = ::pread(fd, cbuf + i, count - i, offset + i);
		// EOF
		if (r == 0) return i;
		// Error
		if (r < 0) {
			perror("pread");
			return r;
		}
		i += r;
	} while(i < count);
	return i;
}

ssize_t _pwrite(int fd, const void *buf, size_t count, off_t offset) {
	const char * cbuf = static_cast<const char *>(buf);
	ssize_t i = 0;
	do {
		ssize_t r = ::pwrite(fd, cbuf + i, count - i, offset + i);
		// EOF
		if (r == 0) return i;
		// Error
		if (r < 0) {
			perror("pwrite");
			return r;
		}
		i += r;
	} while(i < count);
	return i;
}

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

std::ostream & operator <<(std::ostream & o, const block * b) {
	if (b) {
		return o << *b;
	} else {
		return o;
	}
}

void update_next_block(lock_t & file_lock, unsigned int id, const job & j, file_size_t physical_offset) {
	if (j.buff->m_logical_size < j.buff->m_maximal_logical_size) return;
	auto nb = j.file->get_available_block(file_lock, j.buff->m_block + 1);
	if (nb) {
		log_info() << "JOB " << id << " update nb  " << *j.buff << std::endl;
		nb->m_physical_offset = physical_offset;
		nb->m_cond.notify_all();
	}
}

void process_run() {
	auto id = tid.fetch_add(1);
	thread_local char data1[1024*1024];
	thread_local char data2[1024*1024];
	lock_t job_lock(job_mutex);
	log_info() << "JOB " << id << " start" << std::endl;
	while (true) {
		while (jobs.empty()) job_cond.wait(job_lock);
		auto j = jobs.front();
		//log_info() << "JOB " << id << " pop job    " << j.buff << " " << j.type << '\n';
		// Don't pop the job as all threads should terminate
		if (j.type == job_type::term) break;

		jobs.pop();
		job_lock.unlock();

		auto file = j.file;
		lock_t file_lock(file->m_mut);

		switch (j.type) {
		case job_type::term:
		{
			assert(false);
			break;
		}
		case job_type::read:
		{
			block_idx_t block = j.buff->m_block;
			file_size_t physical_offset = j.buff->m_physical_offset;
			file_size_t logical_offset = j.buff->m_logical_offset;
			block_size_t physical_size = j.buff->m_physical_size;
			block_size_t logical_size = j.buff->m_logical_size;
			block_size_t prev_physical_size = j.buff->m_prev_physical_size;
			block_size_t next_physical_size = j.buff->m_next_physical_size;
			auto blocks = file->m_blocks;

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
			bool should_read_next_physical_size = false;
			if (block + 1 != blocks && !is_known(next_physical_size)) { // NOT THE LAST BLOCK
				auto it = file->m_block_map.find(block + 1);
				if (it != file->m_block_map.end()) {
					next_physical_size = it->second->m_physical_size;
				} else {
					size += sizeof(block_header);
					should_read_next_physical_size = true;
				}
			}

			char * data = data1;

			log_info() << "JOB " << id << " pread      " << *j.buff << " from " << off << " - " << (off + size - 1) << std::endl;

			auto r = _pread(file->m_fd, data, size, off);

			assert(r == size);

			if (block != 0 && !is_known(prev_physical_size)) {
				//log_info() << id << "read prev header" << std::endl;
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				prev_physical_size = h.physical_size;
			}

			{
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				//log_info() << id << "Read current header " << physical_size << " " << h.physical_size << std::endl;

				data += sizeof(block_header);
				assert(physical_size == h.physical_size);
				logical_size = h.logical_size;
			}

			bool ok = snappy::RawUncompress(data, physical_size - 2*sizeof(block_header), (char *)j.buff->m_data);
			assert(ok);

			log_info() << "Read " << *j.buff << '\n'
			 		   << "Logical size " << logical_size << '\n'
					   << "First data " << reinterpret_cast<int*>(j.buff->m_data)[0]
					   << " " << reinterpret_cast<int*>(j.buff->m_data)[1] << std::endl;

			data += physical_size - sizeof(block_header); //Skip next block header
			if (should_read_next_physical_size) {
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}

			file_lock.lock();

			update_next_block(file_lock, id, j, off + size);

			j.buff->m_io = false;

			j.buff->m_prev_physical_size = prev_physical_size;
			j.buff->m_next_physical_size = next_physical_size;
			j.buff->m_logical_size = logical_size;
			j.buff->m_physical_size = physical_size;
			j.buff->m_logical_offset = logical_offset;

			j.buff->m_read = true;
			j.buff->m_cond.notify_all();

			file->free_block(file_lock, j.buff);
		}
		break;
		case job_type::write:
		{
			const auto bytes = j.buff->m_logical_size * file->m_item_size;

			block_header h;
			h.logical_size = j.buff->m_logical_size;
			h.logical_offset = j.buff->m_logical_offset;
			log_info() << "JOB " << id << " compress   " << *j.buff << " size " << bytes << '\n'
					   << "First data " << reinterpret_cast<int*>(j.buff->m_data)[0]
					   << " " << reinterpret_cast<int*>(j.buff->m_data)[1] << std::endl;

			file_lock.unlock();

			// TODO check if it is undefined behaiviure to change data underneeth snappy
			// TODO only used bytes here
			memcpy(data1, j.buff->m_data, bytes);

			// TODO free the block here


			size_t s2 = 1024*1024;
			snappy::RawCompress(data1, bytes , data2+sizeof(block_header), &s2);
			size_t bs = 2*sizeof(block_header) + s2;

			h.physical_size = bs;
			memcpy(data2, &h, sizeof(block_header));
			memcpy(data2 + sizeof(h) + s2, &h, sizeof(block_header));

			file_lock.lock();
			log_info() << "JOB " << id << " compressed " << *j.buff << " size " << bs << std::endl;

			if (!is_known(j.buff->m_physical_offset)) {
				// If the previous block is doing io,
				// we have to wait for it to finish and update our blocks physical offset.
				// Even if we have a physical offset, the io operation from the previous block,
				// might change it.
				auto pb = file->get_available_block(file_lock, j.buff->m_block - 1);
				assert(pb != nullptr);
				assert(pb->m_io);
				log_info() << "JOB " << id << " waitfor    " << *j.buff << std::endl;
				// We can't use pb anymore as when unlocking the file lock,
				// it might be repurposed for another block id
				while (!is_known(j.buff->m_physical_offset))
					j.buff->m_cond.wait(file_lock);
			}

			file_size_t off = j.buff->m_physical_offset;
			assert(is_known(off));

			update_next_block(file_lock, id, j, off + bs);

			j.buff->m_physical_size = bs;
			j.buff->m_io = false;

			file_lock.unlock();

			auto r = _pwrite(file->m_fd, data2, bs, off);
			assert(r == bs);
			log_info() << "JOB " << id << " written    " << *j.buff << " at " <<  off << " - " << off + bs - 1 <<  " physical_size " << std::endl;

			file_lock.lock();
			file->update_physical_size(file_lock, j.buff->m_block, bs);
			file->free_block(file_lock, j.buff);
		}
		break;
		case job_type::trunc:
			break;
		}

		file->m_job_count--;
		file->m_job_cond.notify_one();
		j.buff->m_cond.notify_all();
		job_lock.lock();
	}
	log_info() << "JOB " << id << " end" << std::endl;
}
