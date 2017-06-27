// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

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

#include <file_stream.h>

#define TEST_DIR "/hdd/tmp/tpie_new_speed_test/"
#define TEST_NEW_STREAMS

#include <speed_test_common.h>

int main(int argc, char ** argv) {
	speed_test_init(argc, argv);

	file_stream_init(4);

	auto start = std::chrono::steady_clock::now();

	switch (cmd_options.item_type) {
	case 0:
		run_test<int_generator, file_stream<int>>();
		break;
	case 1:
		run_test<string_generator, serialized_file_stream<std::string>>();
		break;
	case 2:
		run_test<keyed_generator, file_stream<keyed_generator::keyed_struct>>();
		break;
	default:
		die("item_type out of range");
	}

	auto end = std::chrono::steady_clock::now();
	std::chrono::duration<double> duration = end - start;

	std::cerr << "Duration: " << duration.count() << "s\n";

	file_stream_term();
}
