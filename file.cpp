// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <file.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

void file::open(std::string path) {
  m_fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 00660);
  std::cout << "FD " << m_fd << std::endl;
  if (m_fd == -1)
    perror("open failed: ");
  
  ::write(m_fd, "head", 4);
}
