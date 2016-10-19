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


void process_run() {
	auto id = tid.fetch_add(1);
	char data1[1024*1024];
	char data2[1024*1024];
	lock_t l(job_mutex);
	log_info() << "JOB " << id << " start" << std::endl;
	while (true) {
		while (jobs.empty()) job_cond.wait(l);
		auto j = jobs.front();
		if (j.type == job_type::term) break;
		jobs.pop();
		l.unlock();
		
		auto file = j.file;
		lock_t file_lock(file->m_mut);
		
		switch (j.type) {
		case job_type::term:
			break;
		case job_type::read:
		{
			block_idx_t block = j.buff->m_block;
			block_offset_t physical_offset = j.buff->m_physical_offset;
			block_offset_t logical_offset = j.buff->m_logical_offset;
			block_size_t physical_size = j.buff->m_physical_size;
			block_size_t logical_size = j.buff->m_logical_size;
			block_size_t prev_physical_size = j.buff->m_prev_physical_size;
			block_size_t next_physical_size = j.buff->m_next_physical_size;
			auto blocks = file->m_blocks;

			file_lock.unlock();
			
			assert(block != no_block_idx);
			assert(physical_offset != no_block_offset);
	
			if (physical_size == no_block_size) {
				block_header h;
				::pread(file->m_fd, &h, sizeof(block_header), physical_offset);
				physical_size = h.physical_size;
			}
	

			block_offset_t off = physical_offset;
			block_offset_t size = physical_size;
			if (block != 0 && prev_physical_size == no_block_size) {
				off -= sizeof(block_header);
				size += sizeof(block_header);
			}
	
			if (block + 1  != blocks && next_physical_size == no_block_size) { //NOT THE LAST BLOCK
				size += sizeof(block_header);
			} 

			char * data = data1;

			log_info() << "JOB " << id << " pread      " << *j.buff << " " << off << " " << size << " " << physical_size << std::endl;
			
			auto r = ::pread(file->m_fd, data, size, off);
			
			assert(r == size);			
			
			if (block != 0 && prev_physical_size == no_block_size) {
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
			if (block + 1 != j.buff->m_file->m_blocks && next_physical_size == no_block_size) {
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}

			file_lock.lock();

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
			uint64_t off = j.buff->m_physical_offset;
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
			
			if (off == no_block_offset) {
				log_info() << "JOB " << id << " waitfor    " << *j.buff << std::endl;
				while (j.buff->m_physical_offset == no_block_offset) j.buff->m_cond.wait(file_lock);
				off = j.buff->m_physical_offset;
			}
	
			auto nb = j.buff->m_successor;
			if (nb) {
			  nb->m_physical_offset = off + bs;
			  nb->m_cond.notify_one();
			}

			file_lock.unlock();

			::pwrite(file->m_fd, data2, bs, off);
			log_info() << "JOB " << id << " written    " << *j.buff << " at " <<  off << " physical_size " << std::endl;
			
			file_lock.lock();
			file->update_physical_size(file_lock, j.buff->m_block, bs);
			file->free_block(file_lock, j.buff);
			if (nb && nb->m_usage == 0) {
				nb->m_usage = 1;
				file->free_block(file_lock, nb);
			}
		}
		break;
		case job_type::trunc:
			break;
		}
		l.lock();
	}
	log_info() << "JOB " << id << " end" << std::endl;
}
