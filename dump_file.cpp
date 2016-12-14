#include <iostream>
#include <iomanip>
#include <cctype>
#include <cstring>
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

void hexdump(unsigned char * buf, size_t n) {
	auto f = std::cout.flags();
	std::cout << std::hex << std::setfill('0');

	int cols = 8;
	int grouped = 2;
	for (size_t i = 0; i < n; i += cols * grouped) {
		for (int j = 0; j < cols; j++) {
			for (int k = 0; k < grouped; k++) {
				std::cout << std::setw(2) << int(buf[i + j + k]);
			}
			std::cout << ' ';
		}
		std::cout << ' ';
		for (int j = 0; j < cols * grouped; j++) {
			unsigned char c = buf[i + j];
			std::cout << (unsigned char)(std::isprint(c)? c: '.');
		}
		std::cout << '\n';
	}

	std::cout.flags(f);
}

void dump_file(const char * fname, bool dumpcontents) {
	int fd = ::open(fname, O_RDONLY, 00660);

	if (fd == -1) {
		perror("open");
		exit(1);
	}

	ssize_t size = ::lseek(fd, 0, SEEK_END);
	::lseek(fd, 0, SEEK_SET);

	ssize_t off = 0;

	file_header h;
	off += _read(fd, &h, sizeof h);

	std::cout << "File header:\n"
			  << "\tMagic: " << h.magic << (h.magic == file_header::magicConst? " (ok)": " (wrong)") << "\n"
			  << "\tVersion: " << h.version << (h.version == file_header::versionConst? " (ok)": " (wrong)") << "\n"
		      << "\tBlocks: " << h.blocks << "\n"
			  << "\tCompressed: " << h.isCompressed << "\n"
		      << "\tSerialized: " << h.isSerialized << "\n"
	          << "\n";

	block_header h1, h2;
	size_t header_size = sizeof(block_header);

	file_size_t logical_offset = 0;

	size_t i;
	for (i = 0; off != size; i++) {
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

		if (dumpcontents) {
			size_t s = h1.physical_size - 2 * header_size;
			unsigned char buf[s];
			_read(fd, buf, s);
			hexdump(buf, s);
			std::cout << '\n';
		} else {
			::lseek(fd, off, SEEK_SET);
		}

		h2 = read_and_print_header(fd, i, false);
		off += header_size;

		if (memcmp(&h1, &h2, header_size)) {
			std::cerr << "Header contents differs!\n";
			exit(1);
		}

		logical_offset += h1.logical_size;
	}

	::close(fd);

	if (h.blocks != i) {
		std::cerr << "File containts " << i << " blocks, but file header specifies " << h.blocks << "\n";
		exit(1);
	}

	std::cout << "Total logical size: " << logical_offset << '\n'
              << "Total physical size: " << size << '\n';
}

int main(int argc, const char * argv[]) {
	const char ** fname = argv + 1;

	bool dumpcontents = false;
	if (strcmp(argv[1], "-h") == 0) {
		dumpcontents = true;
		fname++;
	}

	if (fname != argv + argc - 1) {
		std::cerr << "Usage: dump_file [-h] filename\n";
		return EXIT_FAILURE;
	}

	dump_file(*fname, dumpcontents);

	return EXIT_SUCCESS;
}
