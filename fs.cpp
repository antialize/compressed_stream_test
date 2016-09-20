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

constexpr size_t block_size = 1024;

typedef std::mutex mutex_t;
typedef std::unique_lock<mutex_t> lock_t;

template <typename T>
class file;


class buffer {
public:
  file<size_t> * m_file;
  uint64_t m_block;
  uint32_t m_dirty;
  uint32_t m_peg;


  uint64_t m_offset;
  uint32_t m_disk_size;
  
  
  buffer * m_successor;
  mutex_t m_mutex;  
  std::condition_variable m_cond;  
  size_t m_data[block_size];
};


mutex_t m_available_buffers_mutex;
std::condition_variable m_available_buffer_cond;
std::queue<buffer *> m_available_buffers;

void create_available_buffer() {
  lock_t l(m_available_buffers_mutex);
  auto b = new buffer();
  b->m_file = nullptr;
  m_available_buffers.push(b);
  m_available_buffer_cond.notify_one();
}

buffer * pop_available_buffer();

void destroy_available_buffer() {
  delete pop_available_buffer();
}


void push_available_buffer(buffer * b) {
  lock_t l(m_available_buffers_mutex);
  m_available_buffers.push(b);
  m_available_buffer_cond.notify_one();
}

template <typename T>
class file {
public:
  int m_fd;

  void open(std::string path) {
    m_fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 00660);
    std::cout << "FD " << m_fd << std::endl;
    if (m_fd == -1)
      perror("open failed: ");

    ::write(m_fd, "head", 4);
  }
  
  
  mutex_t m_mut;
  std::map<uint64_t, buffer *> buffers;

  buffer * get_successor_buffer(lock_t &, buffer * t);
  buffer * get_predecessor_buffer(lock_t &, buffer * t);
  void free_buffer(lock_t &, buffer * t);

  buffer * get_first_buffer(lock_t &);
  buffer * get_last_buffer(lock_t &);
};

buffer * pop_available_buffer() {
  buffer * b = nullptr;
  {
    lock_t l(m_available_buffers_mutex);
    while (m_available_buffers.empty()) m_available_buffer_cond.wait(l);
    b = m_available_buffers.front();
    m_available_buffers.pop();
  }
  if (b->m_file)
    b->m_file->buffers.erase(b->m_block);

  b->m_file = nullptr;
  b->m_block = 0;
  b->m_dirty = 0;
  b->m_peg = 0;
  b->m_offset = 0;
  b->m_disk_size = 0;
  b->m_successor = nullptr;
}


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

	log_info() << "WRITE " << j.buff << " " << j.buff->m_block << " " << std::endl;
	
	size_t s2 = 1024*1024;

	snappy::RawCompress(data1, sizeof(size_t) * block_size, data2+sizeof(block_header), &s2);
	
	block_header h;
	h.block_bytes = s2;

	memcpy(data2, &h, sizeof(block_header));
	memcpy(data2 + sizeof(h) + s2, &h, sizeof(block_header));

	size_t bs = 2*sizeof(h) + s2;

	uint64_t off=0;
	{
	  log_info() << "BAR " << j.buff << " " << j.buff->m_block << " " << std::endl;
	  lock_t l2(j.buff->m_mutex);
	  while (j.buff->m_offset == 0) j.buff->m_cond.wait(l2);
	  off = j.buff->m_offset;
	}

	::pwrite(j.buff->m_file->m_fd, data2, bs, off);

	log_info() << "Done writing " << j.buff->m_block << std::endl;
	auto nb = j.buff->m_successor;
	if (nb) {
	  lock_t l2(nb->m_mutex);
	  nb->m_offset = off + bs;
	  nb->m_cond.notify_one();
	}
      }
      break;
    case job_type::trunc:
      break;
    }
    l.lock();
  }
  log_info() << "End job thread" << std::endl;
}


template <typename T>
buffer * file<T>::get_successor_buffer(lock_t & l, buffer * t) {
  auto it = buffers.find(t->m_block);
  if (it != buffers.end()) {
    it->second->m_peg++;
    return it->second;
  }

  l.unlock();
  auto buff = pop_available_buffer();
  l.lock();
  
  size_t off = 0;
  if (t->m_disk_size != 0 && t->m_offset != 0)
    off = t->m_disk_size + t->m_offset;
  
  buff->m_file = this;
  buff->m_dirty = 0;
  buff->m_block = t->m_block + 1;
  buff->m_offset = off;
  buff->m_successor = nullptr;
  buff->m_peg = 1;
  buffers.emplace(0, buff);

  if (off == 0) {
    t->m_successor = buff;
    buff->m_peg++;
  }
  return buff;
}

template <typename T>
buffer * file<T>::get_predecessor_buffer(lock_t &, buffer * t) {

}

template <typename T>
void file<T>::free_buffer(lock_t &, buffer * t) {
  if (t == nullptr) return;
  --t->m_peg;

  if (t->m_peg != 0) return;

  if (t->m_dirty) {
    
    lock_t l2(job_mutex);
	  
    job j;
    j.type = job_type::write;
    j.buff = t;
    
    t->m_peg++;
    
    jobs.push(j);
    job_cond.notify_one();
  } else {
    push_available_buffer(t);
  }
}

template <typename T>
buffer * file<T>::get_first_buffer(lock_t & l ) {
  auto it = buffers.find(0);
  if (it != buffers.end()) {
    it->second->m_peg++;
    return it->second;
  }

  auto buff = new buffer();
  buff->m_file = this;
  buff->m_dirty = 0;
  buff->m_block = 0;
  buff->m_offset = 4;
  buff->m_successor = nullptr;
  buff->m_peg = 1;
  buffers.emplace(0, buff);

  // We need to check if we should read here
  return buff;
}

template <typename T>
buffer * file<T>::get_last_buffer(lock_t &) {
}

template <typename T>
class stream {
public:
  file<T> * m_file;
  buffer * m_cur_buffer;
  uint32_t m_cur_index;


  stream(file<T> & file) :
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
  
    
    // uint64_t block = m_cur_buffer ? m_cur_buffer->m_block + 1 : 0;

    // auto m_old_buffer = m_cur_buffer;
    
    // auto it = m_file->buffers.find(block);
    // if (it != m_file->buffers.end()) {
    //   m_cur_buffer = it->second;
    // } else {
    //   m_cur_buffer = new buffer();
    //   m_cur_buffer->m_file = m_file;
    //   m_cur_buffer->m_dirty = 0;
    //   m_cur_buffer->m_block = block;
    //   m_cur_buffer->m_offset = (block == 0) ? 4 : 0;
    //   m_cur_buffer->m_peg = 0;
    //   log_info() << "HELLO " << m_cur_buffer << " " << m_cur_buffer->m_offset << " " << block << std::endl;
	
    //   if (m_old_buffer)
    // 	m_old_buffer->m_successor = m_cur_buffer;

    //   //::read(m_file->m_fd, m_cur_buffer->m_data, block_size * sizeof(T));

    //   m_file->buffers.emplace(block, m_cur_buffer);
    // }
    // m_cur_buffer->m_peg++;
    // m_cur_index = 0;
    // 
  //}
  
  void write(T item) {
    if (m_cur_index == block_size) next_buffer();
    m_cur_buffer->m_data[m_cur_index++] = std::move(item);
    m_cur_buffer->m_dirty = m_cur_index;
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
  std::thread p1(process_run); //create_available_buffer();
  std::thread p2(process_run); //create_available_buffer();
  std::thread p3(process_run); //create_available_buffer();
  std::thread p4(process_run); //create_available_buffer();
  std::thread p5(process_run); //create_available_buffer();
  std::thread p6(process_run); //create_available_buffer();
  std::thread p7(process_run); //create_available_buffer();

  {
    file<size_t> f;
    f.open("/dev/shm/hello.tst");
    stream<size_t> s(f);
    
    for (size_t i=0; i < block_size * 20; ++i)
      s.write(i);
    
    
    // s.seek(0);
    // for (size_t i=0; i < 102; ++i)
    //   log_info() << "r " << s.read() << std::endl;
    
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
