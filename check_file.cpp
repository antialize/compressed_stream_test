#include <check_file.h>

#include <iostream>
#include <iomanip>
#include <cctype>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>

#include "file_stream_impl.h"
#include "file_utils.h"

ssize_t _read(int fd, void *buf, size_t count) {
	off_t offset = lseek(fd, 0, SEEK_CUR);
	ssize_t ret = _pread(fd, buf, count, offset);
	if (ret > 0)
		lseek(fd, ret, SEEK_CUR);
	return ret;
}

void print_header(size_t i, const block_header & header, bool head, ssize_t off) {
	std::cout << "Header " << i << " (" << (head? "head": "tail") << ") @ " << off << ":\n"
	          << "\tLogical offset: " << header.logical_offset << "\n"
	          << "\tPhysical size: " << header.physical_size << "\n"
	          << "\tLogical size: " << header.logical_size << "\n"
	          << "\n";
}

block_header read_and_print_header(int fd, size_t i, bool head, bool should_print) {
	ssize_t off = ::lseek(fd, 0, SEEK_CUR);
	block_header header;
	_read(fd, &header, sizeof header);
	if (should_print)
		print_header(i, header, head, off);
	return header;
}

void hexdump(unsigned char * buf, size_t n) {
	auto f = std::cout.flags();
	std::cout << std::hex << std::setfill('0');

	int cols = 8;
	int grouped = 2;
	int perline = cols * grouped;

	for (size_t i = 0; i < n; i += perline) {
		std::cout << std::setw(8) << i << ":  ";
		for (int j = 0; j < perline; j++) {
			if (j > 0 && j % grouped == 0) std::cout << ' ';

			if (i + j < n)
				std::cout << std::setw(2) << int(buf[i + j]);
			else
				std::cout << "  ";
		}
		std::cout << "  ";
		for (int j = 0; j < perline && i + j < n; j++) {
			unsigned char c = buf[i + j];
			std::cout << (std::isprint(c)? c: (unsigned char)'.');
		}
		std::cout << '\n';
	}

	std::cout.flags(f);
}

bool check_file(const char * fname, bool log, bool dumpcontents) {
	int fd = ::open(fname, O_RDONLY, 00660);

	if (fd == -1) {
		perror("open");
		return false;
	}

	ssize_t size = ::lseek(fd, 0, SEEK_END);
	::lseek(fd, 0, SEEK_SET);

	ssize_t off = 0;

	file_header h;
	off += _read(fd, &h, sizeof h);

	if (log) {
		std::cout << "File header:\n"
		          << "\tMagic: " << h.magic << (h.magic == file_header::magicConst ? " (ok)" : " (wrong)") << "\n"
		          << "\tVersion: " << h.version << (h.version == file_header::versionConst ? " (ok)" : " (wrong)")
		          << "\n"
		          << "\tBlocks: " << h.blocks << "\n"
		          << "\tUser data size: " << h.user_data_size << "\n"
		          << "\tMax user data size: " << h.max_user_data_size << "\n"
		          << "\tCompressed: " << h.isCompressed << "\n"
		          << "\tSerialized: " << h.isSerialized << "\n"
		          << "\n";
	}

	unsigned char * buf = new unsigned char[h.max_user_data_size];
	off += _read(fd, buf, h.max_user_data_size);
	if (dumpcontents && h.max_user_data_size != 0) {
		if (h.user_data_size != 0) {
			std::cout << "User data:\n";
			hexdump(buf, h.user_data_size);
		}
		if (h.max_user_data_size > h.user_data_size) {
			std::cout << "\nUnused user data:\n";
			hexdump(buf + h.user_data_size, h.max_user_data_size - h.user_data_size);
		}
		std::cout << '\n';
	}
	delete[] buf;

	block_header h1, h2;
	size_t header_size = sizeof(block_header);

	file_size_t logical_offset = 0;

	size_t i;
	for (i = 0; off != size; i++) {
		h1 = read_and_print_header(fd, i, true, log);
		if (h1.logical_offset != logical_offset) {
			std::cerr << "Wrong logical offset!\n";
			return false;
		}

		off += h1.physical_size - header_size;
		if (off >= size) {
			std::cerr << "Trying to seek beyond file!\n";
			std::cerr << "Expected file size was at least " << (off + header_size) << " < " << size << "\n";
			return false;
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

		h2 = read_and_print_header(fd, i, false, log);
		off += header_size;

		if (memcmp(&h1, &h2, header_size)) {
			std::cerr << "Header contents differs!\n";
			return false;
		}

		logical_offset += h1.logical_size;
	}

	::close(fd);

	if (h.blocks != i) {
		std::cerr << "File containts " << i << " blocks, but file header specifies " << h.blocks << "\n";
		return false;
	}

	if (log) {
		std::cout << "Total logical size: " << logical_offset << '\n'
		          << "Total physical size: " << size << '\n';
	}

	return true;
}
