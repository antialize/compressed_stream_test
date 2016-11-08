#include <iostream>
#include <unistd.h>
#include <fcntl.h>

#include "file_stream_impl.h"

block_size_t print_header(int fd, size_t i, bool head) {
	block_header header;
	ssize_t rt = 0;
	while (rt < sizeof header) {
		ssize_t r = ::read(fd, &header, sizeof header);
		if (r < 0) {
			perror("read");
			exit(1);
		} else if (r == 0) {
			std::cerr << "Unexpected EOF!\n";
			exit(1);
		}
		rt += r;
	}
	ssize_t off = ::lseek(fd, 0, SEEK_CUR) - sizeof header;
	std::cout << "Header " << i << " (" << (head? "head": "tail") << ") @ " << off << ":\n"
			  << "\tLogical offset: " << header.logical_offset << "\n"
			  << "\tPhysical size: " << header.physical_size << "\n"
			  << "\tLogical size: " << header.logical_size << "\n"
			  << "\n";
	return header.physical_size;
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

	block_header header;
	size_t header_size = sizeof header;

	for (size_t i = 0; true; i++) {
		if (i != 0) {
			print_header(fd, i - 1, false);
			off += header_size;
		}
		if (off == size) break;

		auto s = print_header(fd, i, true);
		off += header_size;

		ssize_t seek_off = s - 2 * header_size;
		if (off + seek_off >= size) {
			std::cerr << "Trying to seek beyond file!\n";
			exit(1);
		}
		off = ::lseek(fd, seek_off, SEEK_CUR);
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
