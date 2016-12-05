#include <iostream>
#include <unistd.h>
#include <fcntl.h>

#include "file_stream_impl.h"

ssize_t _read(int fd, void * buf, size_t size) {
	ssize_t rt = 0;
	while (rt < size) {
		ssize_t r = ::read(fd, buf, size);
		if (r < 0) {
			perror("read");
			exit(1);
		} else if (r == 0) {
			std::cerr << "Unexpected EOF!\n";
			exit(1);
		}
		rt += r;
	}
	return rt;
}

void print_header(size_t i, const block_header & header, bool head, ssize_t off) {
	std::cout << "Header " << i << " (" << (head? "head": "tail") << ") @ " << off << ":\n"
			  << "\tLogical offset: " << header.logical_offset << "\n"
			  << "\tPhysical size: " << header.physical_size << "\n"
			  << "\tLogical size: " << header.logical_size << "\n"
			  << "\n";
}

block_header read_and_print_header(int fd, size_t i, bool head) {
	ssize_t off = ::lseek(fd, 0, SEEK_CUR);
	block_header header;
	_read(fd, &header, sizeof header);
	print_header(i, header, head, off);
	return header;
}

void dump_file(const char * fname) {
	int fd = ::open(fname, O_RDONLY, 00660);

	if (fd == -1) {
		perror("open");
		exit(1);
	}

	ssize_t size = ::lseek(fd, 0, SEEK_END);
	::lseek(fd, 0, SEEK_SET);

	ssize_t off = 0;

	block_header h1, h2;
	size_t header_size = sizeof(block_header);

	file_size_t logical_offset = 0;

	for (size_t i = 0; off != size; i++) {
		h1 = read_and_print_header(fd, i, true);
		if (h1.logical_offset != logical_offset) {
			std::cerr << "Wrong logical offset!\n";
			exit(1);
		}

		off += h1.physical_size - header_size;
		if (off >= size) {
			std::cerr << "Trying to seek beyond file!\n";
			exit(1);
		}
		::lseek(fd, off, SEEK_SET);
		h2 = read_and_print_header(fd, i, false);
		off += header_size;

		if (memcmp(&h1, &h2, header_size)) {
			std::cerr << "Header contents differs!\n";
			exit(1);
		}

		logical_offset += h1.logical_size;
	}

	::close(fd);
}

int main(int argc, const char * argv[]) {
	if (argc != 2) {
		std::cerr << "Usage: dump_file filename\n";
		return EXIT_FAILURE;
	}

	dump_file(argv[1]);

	return EXIT_SUCCESS;
}
