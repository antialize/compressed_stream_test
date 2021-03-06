///////////////////////////////////////////////////////////////////////////////
/// \file file_utils.h  Helper functions wrapping a loop around pread/pwrite
///////////////////////////////////////////////////////////////////////////////

#pragma once
#include <unistd.h>

ssize_t _pread(int fd, void *buf, size_t count, off_t offset);
ssize_t _pwrite(int fd, const void *buf, size_t count, off_t offset);
