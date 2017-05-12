// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <iostream>
#include <chrono>
#include <file_stream.h>
#include <unistd.h>
#include <sys/stat.h>

/**
 * Speed test matrix:
 * - Compression on/off
 * - Readahead on/off
 * - Block size: 1/16, 1/8, ..., 4 MiB
 * - Item type:
 *   - int
 *   - std::string
 *   - struct { int32_t key; char data[60]; }
 *
 * - Test:
 *   - Write single
 *   - Write single chunked
 *   - Read single
 *   - Read back single
 *   - k-way merge, k = 2, 4, ..., 1024
 *   - k-way merge using one file and multiple streams
 *   - 2-way distribute
 *   - Binary search (direct, uncompressed)
 *
 * Tricks:
 * - No SSD, No swap
 * - Limit RAM to 1-2 GB
 * - Clear cache between tests
 */

#define TEST_DIR "/tmp/tpie_new_speed_test/"

size_t file_size = 1024 * 1024 * 1024;
size_t blocks = file_size / block_size();

bool compression = true;
bool readahead = true;
int item_type;
int test;

template <typename F>
F open_file(open_flags::open_flags flags) {
	static int file_ctr = 0;
	F f;
	f.open(TEST_DIR + std::to_string(file_ctr++), flags);
	return f;
}

template <typename T, typename F>
void write_single(open_flags::open_flags flags) {
	F f = open_file<F>(flags);

	size_t writes = file_size / sizeof(T);
	T item{};
	for (size_t i = 0; i < writes; i++) f.write(item++);
}

template <typename T, typename F>
void read_single(open_flags::open_flags flags) {
	F f = open_file<F>(flags);

	size_t reads = file_size / sizeof(T);
	for (size_t i = 0; i < reads; i++) f.read();
};

template <typename T, typename F>
void run_test(int n, open_flags::open_flags flags) {
	system("rm -rf " TEST_DIR);
	mkdir(TEST_DIR, 0755);

	switch (n) {
	case 0: write_single<T, F>(flags); break;
	case 1: read_single<T, F>(flags); break;
	}
}

struct key_struct {
	int32_t key;
	char data[60];
};




int main(int argc, char ** argv) {
	compression = (bool)std::atoi(argv[1]);
	readahead = (bool)std::atoi(argv[2]);
	item_type = std::atoi(argv[3]);
	test = std::atoi(argv[4]);

	std::cerr << "Test info:\n"
			  << "  Block size:  " << block_size() << "\n"
			  << "  Compression: " << compression << "\n"
			  << "  Readahead:   " << readahead << "\n"
			  << "  Item type:   " << item_type << "\n"
			  << "  Test:        " << test << "\n";

	int flags = 0;
	if (!compression) flags = flags | open_flags::no_compress;
	if (!readahead) flags = flags | open_flags::no_readahead;

	open_flags::open_flags oflags = open_flags::open_flags(flags);

	file_stream_init(4);

	auto start = std::chrono::steady_clock::now();

	switch (item_type) {
	case 0:
		run_test<int, file_stream<int>>(test, oflags);
		break;
	/*case 1:
		run_test<std::string, serialized_file_stream<std::string>>(test, oflags);
		break;
	case 2:
		run_test<key_struct, file_stream<key_struct>>(test, oflags);
		break;*/
	}

	auto end = std::chrono::steady_clock::now();
	std::chrono::duration<double> duration = end - start;

	std::cerr << "Duration: " << duration.count() << "s\n";

	file_stream_term();
}
