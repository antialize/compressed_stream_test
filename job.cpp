// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream_impl.h>
#include <queue>
#include <cassert>
#include <unistd.h>
#include <snappy.h>

std::queue<job> jobs;
mutex_t job_mutex;
std::condition_variable job_cond;

struct block_header {
	block_offset_t logical_offset;
	block_size_t physical_size;
	block_size_t logical_size;
};


void process_run() {
	char data1[1024*1024];
	char data2[1024*1024];
	lock_t l(job_mutex);
	log_info() << "Start job thread" << std::endl;
	while (true) {
		while (jobs.empty()) job_cond.wait(l);
		log_info() << "I HAVE JOB " << (int)jobs.front().type << std::endl;
		if (jobs.front().type == job_type::term) break;

		job j = jobs.front();
		jobs.pop();
		l.unlock();

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
			
			assert(block != no_block_idx);
			assert(physical_offset != no_block_offset);
	
			if (physical_size == no_block_size) {
				block_header h;
				::pread(j.buff->m_file->m_fd, &h, sizeof(block_header), physical_offset);
				physical_size = h.physical_size;
			}
	

			block_offset_t off = physical_offset;
			block_offset_t size = physical_size;
			if (block != 0 && prev_physical_size == no_block_size) {
				off -= sizeof(block_header);
				size += sizeof(block_header);
			}
	
			if (block + 1  != j.buff->m_file->m_blocks && next_physical_size == no_block_size) { //NOT THE LAST BLOCK
				size += sizeof(block_header);
			} 

			char * data = data1;
			
			auto r = ::pread(j.buff->m_file->m_fd, data, size, off);
			log_info() << "pread " << off << " " << size << " " << physical_size << std::endl;
			assert(r == size);			
			
			
			if (block != 0 && prev_physical_size == no_block_size) {
				log_info() << "read prev header" << std::endl;
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				prev_physical_size = h.physical_size;
			}

			{
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				log_info() << "Read current header " << physical_size << " " << h.physical_size << std::endl;
								
				data += sizeof(block_header);
				assert(physical_size == h.physical_size);
				logical_size = h.logical_size;
			}

			bool ok = snappy::RawUncompress(data, physical_size - 2*sizeof(block_header), (char *)j.buff->m_data);
			assert(ok);

			log_info() << "Read " << *j.buff << '\n'
					   << "Logical size " << logical_size << '\n'
					   << "First data " << j.buff->m_data[0] << " " << j.buff->m_data[1] << '\n';
			
			data += physical_size - sizeof(block_header); //Skip next block header
			if (block + 1  != j.buff->m_file->m_blocks && next_physical_size == no_block_idx) {
				block_header h;
				memcpy(&h, data, sizeof(block_header));
				data += sizeof(block_header);
				next_physical_size = h.physical_size;
			}


			j.buff->m_prev_physical_size = prev_physical_size;
			j.buff->m_next_physical_size = next_physical_size;
			j.buff->m_logical_size = logical_size;
			j.buff->m_physical_size = physical_size;
			j.buff->m_logical_offset = logical_offset;

			j.buff->m_read = true;
			
			{
				lock_t l2(j.buff->m_mutex);
				j.buff->m_cond.notify_one();
			}

			{
				auto f = j.buff->m_file;
				lock_t l2(f->m_mut);
				f->free_block(l2, j.buff);
			}
			
		}
		break;
		case job_type::write:
		{
			// TODO check if it is undefined behaiviure to change data underneeth snappy
			// TODO only used bytes here
			memcpy(data1, j.buff->m_data, j.buff->m_logical_size * sizeof(size_t) );

			// TODO free the block here
	
			log_info() << "io write " << *j.buff << std::endl;
			size_t s2 = 1024*1024;
			snappy::RawCompress(data1, j.buff->m_logical_size * sizeof(size_t) , data2+sizeof(block_header), &s2);
			size_t bs = 2*sizeof(block_header) + s2;
			
			block_header h;
			h.physical_size = bs;
			h.logical_size = j.buff->m_logical_size;
			h.logical_offset = j.buff->m_logical_offset;
			memcpy(data2, &h, sizeof(block_header));
			memcpy(data2 + sizeof(h) + s2, &h, sizeof(block_header));
			

			uint64_t off = no_block_offset;
			{
				lock_t l2(j.buff->m_mutex);
				while (j.buff->m_physical_offset == no_block_offset) j.buff->m_cond.wait(l2);
				off = j.buff->m_physical_offset;
			}
	
			auto nb = j.buff->m_successor;
			if (nb) {
				lock_t l2(nb->m_mutex);
				nb->m_physical_offset = off + bs;
				nb->m_cond.notify_one();
			}

			::pwrite(j.buff->m_file->m_fd, data2, bs, off);
			log_info() << "write block "<< j.buff->m_block << " at " <<  off << " physical_size " << bs << " " << j.buff->m_block << std::endl;

			{
				auto f = j.buff->m_file;
				lock_t l2(f->m_mut);


				f->update_physical_size(l2, j.buff->m_block, bs);
				
				f->free_block(l2, j.buff);

				if (nb && nb->m_usage == 0) {
					nb->m_usage = 1;
					f->free_block(l2, nb);
				}
			}
	
			// Free buffer
			log_info() << "Done writing " << j.buff->m_block << std::endl;
		}
		break;
		case job_type::trunc:
			break;
		}
		l.lock();
	}
	log_info() << "End job thread" << std::endl;
}
