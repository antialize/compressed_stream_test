// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <buffer.h>
#include <map>

class file {
public:
  int m_fd;

  void open(std::string path);  
  
  mutex_t m_mut;
  std::map<uint64_t, buffer *> buffers;

  buffer * get_successor_buffer(lock_t &, buffer * t);
  buffer * get_predecessor_buffer(lock_t &, buffer * t);
  void free_buffer(lock_t &, buffer * t);

  buffer * get_first_buffer(lock_t &);
  buffer * get_last_buffer(lock_t &);
};
