// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include <iostream>
#include <fstream>
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

#define TEST_DIR "/hdd/tmp/tpie_new_speed_test/"

size_t file_size = 1ull * 1024 * 1024 * 1024;
size_t blocks = file_size / block_size();

std::vector<std::string> words;

void die(std::string message) {
	std::cerr << message << "\n";
	std::abort();
}

struct string_generator {
	size_t i = 0;
	size_t j = 0;

	std::string operator++(int) {
		auto tmp = words[i] + "_" + words[j];

		i++;
		if (i == words.size()) {
			i = 0;
			j++;
			if (j == words.size()) {
				j = 0;
			}
		}

		return tmp;
	}
};

struct keyed_generator {
	struct keyed_struct {
		uint32_t key;
		char data[60];
	};

	keyed_struct current;

	keyed_generator() {
		current.key = 0;
		for (char i = 0; i < sizeof(current.data); i++) {
			current.data[i] = 'A' + i;
		}
	}

	keyed_struct operator++(int) {
		auto tmp = current;
		current.key += 1;
		return tmp;
	}
};

template <typename T, typename F>
struct speed_test_t {
	int file_ctr = 0;
	open_flags::open_flags m_flags;

	virtual ~speed_test_t() = default;

	virtual void init() {}
	virtual void setup() {}
	virtual void run() {}

	F open_file(open_flags::open_flags flags) {
		std::string fname =
			TEST_DIR +
				std::string(typeid(*this).name()) + "_" +
				std::to_string(flags & (open_flags::no_readahead | open_flags::no_compress)) + "_" +
				std::to_string(block_size()) + "_" +
				std::to_string(file_ctr++);
		F f;
		f.open(fname, flags);
		return f;
	}

	F open_file() {
		return open_file(m_flags);
	}
};

template <typename T, typename F>
struct write_single : speed_test_t<T, F> {
	F f;

	void init() override {
		f = this->open_file();
	}

	void setup() override {

	}

	void run() override {
		size_t writes = file_size / sizeof(T);
		T item{};
		for (size_t i = 0; i < writes; i++) f.write(item++);
	}
};


template <typename T, typename F>
struct read_single : speed_test_t<T, F> {
	F f;

	void init() override {
		f = this->open_file();
	}

	void setup() override {
		size_t writes = file_size / sizeof(T);
		T item{};
		for (size_t i = 0; i < writes; i++) f.write(item++);
	}

	void run() override {
		size_t reads = file_size / sizeof(T);
		for (size_t i = 0; i < reads; i++) f.read();
	}
};

template <typename T, typename F>
void run_test(bool setup, int n, open_flags::open_flags flags) {
	system("mkdir -p " TEST_DIR);

	speed_test_t<T, F> * test = nullptr;

	switch (n) {
	case 0: test = new write_single<T, F>(); break;
	case 1: test = new read_single<T, F>(); break;
	default: die("test index out of range");
	}

	if (setup) {
		flags |= open_flags::truncate;
	}

	test->m_flags = flags;
	test->init();
	if (setup) {
		test->setup();
	} else {
		test->run();
	}

	delete test;
}

int main(int argc, char ** argv) {
	if (argc != 6) {
		std::cerr << "Usage: " << argv[0] << " compression readahead item_type test setup\n";
		return EXIT_FAILURE;
	}
	bool compression = (bool)std::atoi(argv[1]);
	bool readahead = (bool)std::atoi(argv[2]);
	int item_type = std::atoi(argv[3]);
	int test = std::atoi(argv[4]);
	bool setup = (bool)std::atoi(argv[5]);

	std::cerr << "Test info:\n"
			  << "  Block size:  " << block_size() << "\n"
			  << "  Compression: " << compression << "\n"
			  << "  Readahead:   " << readahead << "\n"
			  << "  Item type:   " << item_type << "\n"
			  << "  Test:        " << test << "\n"
			  << "  Action:      " << (setup? "Setup": "Run test") << "\n";

	int flags = 0;
	if (!compression) flags = flags | open_flags::no_compress;
	if (!readahead) flags = flags | open_flags::no_readahead;

	open_flags::open_flags oflags = open_flags::open_flags(flags);

	{
		std::ifstream word_stream("/usr/share/dict/words");
		std::string w;
		while (word_stream >> w) {
			words.push_back(w);
		}
	}

	file_stream_init(4);

	auto start = std::chrono::steady_clock::now();

	switch (item_type) {
	case 0:
		run_test<int, file_stream<int>>(setup, test, oflags);
		break;
	case 1:
		run_test<string_generator, serialized_file_stream<std::string>>(setup, test, oflags);
		break;
	case 2:
		run_test<keyed_generator, file_stream<keyed_generator::keyed_struct>>(setup, test, oflags);
		break;
	default:
		die("item_type out of range");
	}

	auto end = std::chrono::steady_clock::now();
	std::chrono::duration<double> duration = end - start;

	std::cerr << "Duration: " << duration.count() << "s\n";

	file_stream_term();
}
