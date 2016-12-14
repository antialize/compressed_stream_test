// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_utils.h>
#include <cstdio>

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
