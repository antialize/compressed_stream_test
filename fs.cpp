#include <stdint.h>
#include <map>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <snappy.h>
#include <string.h>

#include <buffer.h>
#include <available_buffers.h>
#include <file.h>



enum class job_type {
  term, write, read, trunc
};

class job {
public:
  job_type type;
  buffer * buff;
};

std::queue<job> jobs;
mutex_t job_mutex;

std::condition_variable job_cond;


struct block_header {
  uint32_t block_bytes;
};

struct crapper {
  static mutex_t m;
  lock_t l;
  crapper(): l(m) {}

};

template <typename T>
std::ostream & operator <<(const crapper & c, const T & t) {
  return std::cout << t;
}
			   

mutex_t crapper::m;

crapper log_info() {
  return crapper();
}


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
      break;
    case job_type::write:
      {
	// TODO check if it is undefined behaiviure to change data underneeth snappy
	memcpy(data1, j.buff->m_data, sizeof(size_t) * block_size);

	// TODO free the block here
	using namespace std::chrono_literals;
	//std::this_thread::sleep_for(10s);
	
	log_info() << "io write " << *j.buff << std::endl;
	size_t s2 = 1024*1024;
	snappy::RawCompress(data1, sizeof(size_t) * block_size, data2+sizeof(block_header), &s2);
	block_header h;
	h.block_bytes = s2;
	memcpy(data2, &h, sizeof(block_header));
	memcpy(data2 + sizeof(h) + s2, &h, sizeof(block_header));

	size_t bs = 2*sizeof(h) + s2;

	uint64_t off=0;
	{
	  lock_t l2(j.buff->m_mutex);
	  while (j.buff->m_physical_offset == 0) j.buff->m_cond.wait(l2);
	  off = j.buff->m_physical_offset;
	}
	
	auto nb = j.buff->m_successor;
	if (nb) {
	  lock_t l2(nb->m_mutex);
	  nb->m_physical_offset = off + bs;
	  nb->m_cond.notify_one();
	}

	::pwrite(j.buff->m_file->m_fd, data2, bs, off);

	{
	  auto f = j.buff->m_file;
	  lock_t l2(f->m_mut);
	  f->free_buffer(l2, j.buff);

	  if (nb && nb->m_usage == 0) {
	    nb->m_usage = 1;
	    f->free_buffer(l2, nb);
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


buffer * file::get_successor_buffer(lock_t & l, buffer * t) {
  auto it = buffers.find(t->m_block+1);
  if (it != buffers.end()) {
    it->second->m_usage++;
    return it->second;
  }

  l.unlock();
  auto buff = pop_available_buffer();
  l.lock();
  
  size_t off = 0;
  if (t->m_physical_size != 0 && t->m_physical_offset != 0)
    off = t->m_physical_size + t->m_physical_offset;
  
  buff->m_file = this;
  buff->m_dirty = false;
  buff->m_block = t->m_block + 1;
  buff->m_physical_offset = off;
  buff->m_logical_offset = t->m_logical_offset + t->m_logical_size;
  buff->m_successor = nullptr;
  buff->m_usage = 1;
  buffers.emplace(0, buff);

  if (off == 0) {
    t->m_successor = buff;
  }

  if (buff->m_block == m_blocks)
    ++m_blocks;
  else {
    log_info() << "READ STUFF" << std::endl;
    //We need to read stuff
  }
  
  if (buff->m_block + 1 == m_blocks)
    m_last_buffer = buff;
   
  log_info() << "get succ " << *buff << std::endl;
  return buff;
}

buffer * file::get_predecessor_buffer(lock_t &, buffer * t) {
  return nullptr;
}

void file::free_buffer(lock_t &, buffer * t) {
  if (t == nullptr) return;
  log_info() << "free buffer " << *t << std::endl;
  --t->m_usage;

  if (t->m_usage != 0) return;

  if (t->m_dirty) {
    // Write dirty block
    lock_t l2(job_mutex);
	  
    job j;
    j.type = job_type::write;
    j.buff = t;
    t->m_usage++;
    t->m_dirty = 0;
    log_info() << "write buffer " << *t << std::endl;
    jobs.push(j);
    job_cond.notify_all();
  } else if (t->m_physical_offset != 0) {
    log_info() << "avail buffer " << *t << std::endl;
    push_available_buffer(t);
  }
}

buffer * file::get_first_buffer(lock_t & l ) {
  log_info() << "GET FIRST BLOCK" << std::endl;
  auto it = buffers.find(0);
  if (it != buffers.end()) {
    it->second->m_usage++;
    return it->second;
  }

  log_info() << "LOG INFO HERE" << std::endl;

  l.unlock();
  auto buff = pop_available_buffer();
  l.lock();
  buff->m_file = this;
  buff->m_dirty = 0;
  buff->m_block = 0;
  buff->m_physical_offset = 4;
  buff->m_logical_offset = 0;
  buff->m_successor = nullptr;
  buff->m_usage = 1;
  buffers.emplace(0, buff);

  if (m_blocks == 0)
    m_blocks = 1;
  else
    log_info() << "WE NEED TO READ STUFF" << std::endl;
  if (m_blocks == 1)
    m_last_buffer = buff;
  
  return buff;
}

buffer * file::get_last_buffer(lock_t &) {
  return nullptr;
}

template <typename T>
class stream {
public:
  file * m_file;
  buffer * m_cur_buffer;
  uint32_t m_cur_index;
  
  stream(file & file) :
    m_file(&file), m_cur_buffer(nullptr), m_cur_index(block_size) {
    create_available_buffer();
  }

  ~stream() {
    if (m_cur_buffer) {
      lock_t lock(m_file->m_mut);
      m_file->free_buffer(lock, m_cur_buffer);
    }
    destroy_available_buffer();
  }
    
  void next_buffer() {
    lock_t lock(m_file->m_mut);
    buffer * buff = m_cur_buffer;
    if (buff == nullptr) m_cur_buffer = m_file->get_first_buffer(lock);
    else m_cur_buffer = m_file->get_successor_buffer(lock, buff);
    m_file->free_buffer(lock, buff);
    m_cur_index = 0;
  }

  void prev_buffer() {
    lock_t lock(m_file->m_mut);
    buffer * buff = m_cur_buffer;
    if (buff = nullptr) m_cur_buffer = m_file->get_last_buffer(lock);
    else m_cur_buffer = m_file->get_predecessor_buffer(lock, buff);
    m_file->free_buffer(lock, buff);
  }
  
  void write(T item) {
    if (m_cur_index == block_size) next_buffer();
    m_cur_buffer->m_data[m_cur_index++] = std::move(item);
    m_cur_buffer->m_logical_size = std::max(m_cur_buffer->m_logical_size, m_cur_index); //Hopefully this is a cmove
    m_cur_buffer->m_dirty = true;
  }
  
  const T & read() {
    if (m_cur_index == block_size) next_buffer();
    return m_cur_buffer->m_data[m_cur_index++];
  }
  
  void skip() {
    if (m_cur_index == block_size) next_buffer();
    m_cur_index++;
  }
  
  const T & peek() {
    if (m_cur_index == block_size) next_buffer();
    return m_cur_buffer->m_data[m_cur_index];
  }
  
  void seek(uint64_t offset) {
    lock_t lock(m_file->m_mut); 
    if (offset != 0)
      throw std::runtime_error("Not supported");
    
    if (m_cur_buffer)
      m_file->free_buffer(lock, m_cur_buffer);

    m_cur_buffer = nullptr;
    m_cur_index = block_size;
  }
};

int main() {
  std::thread p1(process_run); create_available_buffer();
  std::thread p2(process_run); create_available_buffer();
  std::thread p3(process_run); create_available_buffer();
  std::thread p4(process_run); create_available_buffer();
  std::thread p5(process_run); create_available_buffer();
  std::thread p6(process_run); create_available_buffer();
  std::thread p7(process_run); create_available_buffer();

  {
    file f;
    f.open("/dev/shm/hello.tst");
    stream<size_t> s(f);
    
    //log_info() << f.size() << std::endl;
    for (size_t i=0; i < block_size * 20; ++i) {
      s.write(i);
      //log_info() << f.size() << std::endl;
    }      
    
    s.seek(0);
    for (size_t i=0; i < 102; ++i)
      log_info() << "r " << s.read() << std::endl;

    
    log_info() << "WHAT 2" << std::endl;
    
    {
      lock_t l(job_mutex);
      job j;
      j.type = job_type::term;
      jobs.push(j);
      job_cond.notify_all();
    }
  }
  
  p1.join();
  p2.join();
  p3.join();
  p4.join();
  p5.join();
  p6.join();
  p7.join();
  
}
