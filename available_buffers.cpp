// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <available_buffers.h>
#include <buffer.h>
#include <queue>
#include <file.h>

namespace {
mutex_t available_buffers_mutex;
std::condition_variable available_buffer_cond;
std::queue<buffer *> available_buffers;
}

static size_t ctr = 0;
void create_available_buffer() {
  lock_t l(available_buffers_mutex);
  auto b = new buffer();
  b->m_idx = ctr++;
  b->m_file = nullptr;
  available_buffers.push(b);
  available_buffer_cond.notify_one();
}

buffer * pop_available_buffer();

void destroy_available_buffer() {
  delete pop_available_buffer();
}

void push_available_buffer(buffer * b) {
  lock_t l(available_buffers_mutex);
  available_buffers.push(b);
  available_buffer_cond.notify_one();
}


buffer * pop_available_buffer() {
  buffer * b = nullptr;
  {
    lock_t l(available_buffers_mutex);
    while (available_buffers.empty()) available_buffer_cond.wait(l);
    b = available_buffers.front();
    available_buffers.pop();
  }
  if (b->m_file)
    b->m_file->buffers.erase(b->m_block);

  b->m_file = nullptr;
  b->m_block = 0;
  b->m_dirty = false;
  b->m_usage = 0;
  b->m_physical_offset = 0;
  b->m_physical_size = 0;
  b->m_logical_offset = 0;
  b->m_logical_size = 0;
  b->m_successor = nullptr;
  return b;
}
