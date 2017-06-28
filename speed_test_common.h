#pragma once

#ifndef TEST_NEW_STREAMS
#ifndef   TEST_OLD_STREAMS
#error       "Please define TEST_NEW_STREAMS or TEST_OLD_STREAMS"
#endif
#endif

#include <iostream>
#include <fstream>
#include <chrono>
#include <queue>

#include <boost/filesystem/operations.hpp>

//size_t file_size = 1ull * 1024 * 1024 * 1024;
size_t file_size = 1ull * 1024 * 1024;
size_t blocks = file_size / block_size();

std::vector<std::string> words;

struct {
	bool compression;
	bool readahead;
	int item_type;
	int test;
	bool setup;
	int K;
} cmd_options;

void speed_test_init(int argc, char ** argv) {
	if (argc < 6 || argc > 7) {
		std::cerr << "Usage: " << argv[0] << " compression readahead item_type test setup [extra param (K)]\n";
		std::exit(EXIT_FAILURE);
	}
	bool compression = (bool)std::atoi(argv[1]);
	bool readahead = (bool)std::atoi(argv[2]);
	int item_type = std::atoi(argv[3]);
	int test = std::atoi(argv[4]);
	bool setup = (bool)std::atoi(argv[5]);
	int K = (argc == 7)? std::atoi(argv[6]): 0;

	std::cerr << "Test info:\n"
			  << "  Block size:  " << block_size() << "\n"
			  << "  Compression: " << compression << "\n"
			  << "  Readahead:   " << readahead << "\n"
			  << "  Item type:   " << item_type << "\n"
			  << "  Test:        " << test << "\n"
			  << "  Action:      " << (setup? "Setup": "Run") << "\n";

	if (K) {
		std::cerr << "  Parameter:   " << K << "\n";
	}

	cmd_options = {compression, readahead, item_type, test, setup, K};

	{
		std::ifstream word_stream("/usr/share/dict/words");
		std::string w;
		while (word_stream >> w) {
			words.push_back(w);
		}
	}
}

void die(std::string message) {
	std::cerr << message << "\n";
	std::abort();
}

void skip() {
	std::cerr << "SKIP\n";
	std::_Exit(EXIT_SUCCESS);
}

struct int_generator {
	using item_type = int;

	size_t i = 0;

	int next() {
		int tmp = i;
		i++;
		return tmp;
	}
};

struct string_generator {
	using item_type = std::string;

	size_t i = 0;
	size_t j = 0;

	std::string next() {
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

		bool operator==(const keyed_struct & o) const {
			return key == o.key;
		}

		bool operator<(const keyed_struct & o) const {
			return key < o.key;
		}
	};

	using item_type = keyed_struct;

	keyed_struct current;

	keyed_generator() {
		current.key = 0;
		for (char i = 0; i < sizeof(current.data); i++) {
			current.data[i] = 'A' + i;
		}
	}

	keyed_struct next() {
		auto tmp = current;
		current.key += 1;
		return tmp;
	}
};

template <typename T, typename FS>
struct speed_test_t {
	size_t item_size = sizeof(typename T::item_type);
	size_t total_items = file_size / item_size;

#ifdef TEST_NEW_STREAMS
#endif

	int file_ctr = 0;

	virtual ~speed_test_t() = default;

	virtual void init() {}
	virtual void setup() {}
	virtual void run() {}

	std::string get_fname() {
		return TEST_DIR +
			std::string(typeid(*this).name()) + "_" +
			std::to_string(cmd_options.compression) + "_" +
			std::to_string(cmd_options.readahead) + "_" +
			std::to_string(block_size()) + "_" +
			std::to_string(cmd_options.K) + "_" +
			std::to_string(file_ctr++);
	}

#ifdef TEST_NEW_STREAMS
	static open_flags::open_flags get_flags() {
		open_flags::open_flags flags = open_flags::default_flags;
		if (!cmd_options.compression) flags |= open_flags::no_compress;
		if (!cmd_options.readahead) flags |= open_flags::no_readahead;
		return flags;
	}

	template <typename>
	struct file_type;

	template <bool S>
	struct file_type<file_stream_base<typename T::item_type, S>> {
		using type = file_base<typename T::item_type, S>;
	};

	using F = typename file_type<FS>::type;

	void open_file(F & f, size_t max_user_data_size = 0) {
		f.open(get_fname(), get_flags(), max_user_data_size);
	}
#endif

	void open_file_stream(FS & f, size_t max_user_data_size = 0) {
		// Truncate files during setup
		if (cmd_options.setup) {
			boost::filesystem::remove(get_fname());
			file_ctr--;
		}
#ifdef TEST_NEW_STREAMS
		f.open(get_fname(), get_flags(), max_user_data_size);
#else
		f.open(get_fname(), access_type::access_read_write, max_user_data_size, access_normal);
#endif
	}
};

template <typename T, typename FS>
struct write_single : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {

	}

	void run() override {
		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}
};

template <typename T, typename FS>
struct write_single_chunked : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {

	}

	void run() override {
		const size_t N = 1024;
		T gen;
		typename T::item_type items[N];
		for (size_t j = 0; j < this->total_items / N; j++) {
			for (size_t i = 0; i < N; i++) {
				items[i] = gen.next();
			}
#ifdef TEST_NEW_STREAMS
			f.write(items, N);
#else
			f.write(&*items, items + N);
#endif
		}
	}
};

template <typename T, typename FS>
struct read_single : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {
		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}

	void run() override {
		for (size_t i = 0; i < this->total_items; i++) f.read();
	}
};

template <typename T, typename FS>
struct read_back_single : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {
		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}

	void run() override {
#ifdef TEST_NEW_STREAMS
		f.seek(0, whence::end);
#else
		f.seek(0, FS::end);
#endif
		for (size_t i = 0; i < this->total_items; i++) f.read_back();
	}
};

template <typename T, typename FS>
struct merge : speed_test_t<T, FS> {
	FS output;
	FS * inputs;

	void init() override {
		if (cmd_options.K <= 0) {
			die("Need positive parameter K for merge test");
		}
		this->open_file_stream(output);

		inputs = new FS[cmd_options.K];

		for (size_t i = 0; i < cmd_options.K; i++) {
			this->open_file_stream(inputs[i]);
		}
	}

	~merge() {
		delete[] inputs;
	}

	void setup() override {
		T gen;
		for (size_t i = 0; i < this->total_items / cmd_options.K; i++) {
			for (size_t j = 0; j < cmd_options.K; j++) {
				inputs[i].write(gen.next());
			}
		}
	}

	void run() override {
		using item_t = std::pair<typename T::item_type, size_t>;
		std::priority_queue<item_t, std::vector<item_t>, std::greater<item_t>> pq;
		for (size_t i = 0; i < cmd_options.K; i++) {
			pq.push({inputs[i].read(), i});
		}

		while (!pq.empty()) {
			auto p = pq.top();
			pq.pop();

			output.write(p.first);
			auto & input = inputs[p.second];
			if (input.can_read()) {
				pq.push({input.read(), p.second});
			}
		}
	}
};

#ifdef TEST_NEW_STREAMS
template <typename T, typename FS>
struct merge_single_file : speed_test_t<T, FS> {
	FS output;
	typename speed_test_t<T, FS>::F input;

	struct stream_info {
		stream_position pos;
		size_t items;
	};

	void init() override {
		if (cmd_options.K <= 0) {
			die("Need positive parameter K for merge test");
		}
		this->open_file_stream(output);
		this->open_file(input, sizeof(stream_info) * cmd_options.K);
	}

	void setup() override {
		T gen;
		auto * inputs = new FS[cmd_options.K];
		for (size_t i = 0; i < cmd_options.K; i++) {
			this->open_file_stream(inputs[i]);
		}
		// Write to individual files then concat
		for (size_t i = 0; i < this->total_items / cmd_options.K; i++) {
			for (size_t j = 0; j < cmd_options.K; j++) {
				inputs[j].write(gen.next());
			}
		}

		auto input_stream = input.stream();

		auto * stream_infos = new stream_info[cmd_options.K];
		for (size_t i = 0; i < cmd_options.K; i++) {
			stream_infos[i].pos = input_stream.get_position();
			stream_infos[i].items = 0;

			inputs[i].seek(0, whence::set);
			while (inputs[i].can_read()) {
				input_stream.write(inputs[i].read());
				stream_infos[i].items++;
			}
		}

		delete[] inputs;

		input.write_user_data(stream_infos, sizeof(stream_info) * cmd_options.K);

		delete[] stream_infos;
	}

	void run() override {
		auto * stream_infos = new stream_info[cmd_options.K];
		input.read_user_data(stream_infos, sizeof(stream_info) * cmd_options.K);

		std::vector<decltype(input.stream())> streams;
		for (size_t i = 0; i < cmd_options.K; i++) {
			auto s = input.stream();
			s.set_position(stream_infos[i].pos);
			streams.push_back(std::move(s));
		}

		using item_t = std::pair<typename T::item_type, size_t>;
		std::priority_queue<item_t, std::vector<item_t>, std::greater<item_t>> pq;
		for (size_t i = 0; i < cmd_options.K; i++) {
			pq.push({streams[i].read(), i});
		}

		while (!pq.empty()) {
			auto p = pq.top();
			pq.pop();

			output.write(p.first);
			auto & stream = streams[p.second];
			if (--stream_infos[p.second].items > 0) {
				pq.push({stream.read(), p.second});
			}
		}

		delete[] stream_infos;
	}
};
#endif

template <typename T, typename FS>
struct distribute : speed_test_t<T, FS> {
	FS outputs[2];
	FS input;

	void init() override {
		this->open_file_stream(outputs[0]);
		this->open_file_stream(outputs[1]);
		this->open_file_stream(input);
	}

	void setup() override {
		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			input.write(gen.next());
		}
	}

	void run() override {
		for (size_t i = 0; i < this->total_items; i++) {
			outputs[(i % 3) % 2].write(input.read());
		}
	}
};

template <typename T, typename FS>
struct binary_search : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f, this->item_size);

#ifdef TEST_NEW_STREAMS
		bool direct = f.direct();
#else
		bool direct = typeid(FS) == typeid(uncompressed_stream<typename T::item_type>);
#endif
		if (!direct) {
			skip();
		}
	}

	void setup() override {
		T gen;
		bool needle_set = false;
		for (size_t i = 0; i < this->total_items; i++) {
			auto item = gen.next();
			f.write(item);
			if (!needle_set && i >= this->total_items * 0.7) {
				f.write_user_data(item);
				needle_set = true;
			}
		}
	}

	void run() override {
		typename T::item_type needle;
		f.read_user_data(needle);

		size_t lo_i = 0;
		size_t hi_i = f.size() - 1;

		while (lo_i <= hi_i) {
			auto mid_i = (lo_i + hi_i) / 2;
			f.seek(mid_i);
			auto mid = f.read();

			if (mid == needle) {
				return;
			}

			if (mid < needle) {
				lo_i = mid_i + 1;
			} else {
				hi_i = mid_i - 1;
			}
		}

		die("Needle not found");
	}
};

template <typename T, typename FS>
void run_test() {
	int r = system("mkdir -p " TEST_DIR);
	if (r != 0) die("Couldn't create TEST_DIR");

	speed_test_t<T, FS> * test = nullptr;

	switch (cmd_options.test) {
	case 0: test = new write_single<T, FS>(); break;
	case 1: test = new write_single_chunked<T, FS>(); break;
	case 2: test = new read_single<T, FS>(); break;
	case 3: test = new read_back_single<T, FS>(); break;
	case 4: test = new merge<T, FS>(); break;
	case 5: {
#ifdef TEST_NEW_STREAMS
		test = new merge_single_file<T, FS>();
#else
		skip();
#endif
		break;
	}
	case 6: test = new distribute<T, FS>(); break;
	case 7: test = new binary_search<T, FS>(); break;
	default: die("test index out of range");
	}

	test->init();
	if (cmd_options.setup) {
		test->setup();
	} else {
		test->run();
	}

	delete test;
}