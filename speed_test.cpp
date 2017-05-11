// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <iostream>
#include <chrono>
#include <file_stream.h>
#include <unistd.h>

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

#define FILE_NAME "/tmp/speed.tst"

void write_ints(size_t amount, std::underlying_type<open_flags::open_flags>::type flags = 0) {
	file<int> f;
	f.open(FILE_NAME, flags);

	{
		auto s = f.stream();
		for (int i = 0; i < amount; i++) {
			s.write(i);
		}
	}
}

void read_ints(size_t amount, std::underlying_type<open_flags::open_flags>::type flags = 0) {
	file<int> f;
	f.open(FILE_NAME, flags);

	{
		auto s = f.stream();
		for (int i = 0; i < amount; i++) {
			s.read();
		}
	}
}

int main() {
	file_stream_init(4);

	for (open_flags::open_flags flags : {open_flags::default_flags, open_flags::no_compress}) {
		std::cerr << "With" << (flags == open_flags::no_compress? "out": "") << " compression:\n";

		size_t N = 10 * 1000 * 1000;

		unlink(FILE_NAME);

		auto start = std::chrono::steady_clock::now();
		write_ints(N, flags);
		auto end = std::chrono::steady_clock::now();

		std::chrono::duration<double> duration = end - start;

		std::cerr << "Time to write " << N << " ints sequentially: " << duration.count() << "s\n";

		start = std::chrono::steady_clock::now();
		read_ints(N, flags);
		end = std::chrono::steady_clock::now();

		duration = end - start;

		std::cerr << "Time to read " << N << " ints sequentially: " << duration.count() << "s\n";
	}

	unlink(FILE_NAME);

	file_stream_term();
}
