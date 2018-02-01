#pragma once

#ifndef TEST_NEW_STREAMS
#ifndef   TEST_OLD_STREAMS
#error       "Please define TEST_NEW_STREAMS or TEST_OLD_STREAMS"
#endif
#endif

#include <random>
#include <iostream>
#include <fstream>
#include <chrono>
#include <queue>

#include <boost/filesystem/operations.hpp>

template <typename FS>
void ensure_open_write(FS &) {};
template <typename FS>
void ensure_open_write_reverse(FS &) {};
template <typename FS>
void ensure_open_read(FS &) {};
template <typename FS>
void ensure_open_read_reverse(FS &) {};

#ifndef SPEED_TEST_FILE_SIZE_MB
#define SPEED_TEST_FILE_SIZE_MB 1
#endif

constexpr size_t MB = 1024 * 1024;
constexpr size_t file_size = SPEED_TEST_FILE_SIZE_MB * MB;

constexpr size_t no_file_size = std::numeric_limits<size_t>::max();

void die(std::string message) {
	std::cerr << message << "\n";
	std::abort();
}

void skip() {
	std::cerr << "SKIP\n";
	std::_Exit(EXIT_SUCCESS);
}

std::vector<std::string> words;

enum action_t {
	SETUP,
	RUN,
	VALIDATE,
};

struct {
	bool compression;
	bool readahead;
	int item_type;
	int test;
	action_t action;
	size_t K;
	size_t job_threads;
} cmd_options;

void speed_test_init(int argc, char ** argv) {
	if (argc < 6 || argc > 8) {
		std::cerr << "Usage: " << argv[0] << " compression readahead item_type test setup [extra param (K)] [job_threads]\n";
		std::exit(EXIT_FAILURE);
	}
	bool compression = (bool)std::atoi(argv[1]);
	bool readahead = (bool)std::atoi(argv[2]);
	int item_type = std::atoi(argv[3]);
	int test = std::atoi(argv[4]);
	int action = std::atoi(argv[5]);
	size_t K = (argc >= 7)? std::atoi(argv[6]): 0;
	size_t job_threads = (argc >= 8)? std::atoi(argv[7]): 0;

	const char * test_names[] = {
		"write_single",
		"write_single_chunked",
		"read_single",
		"read_back_single",
		"merge",
		"merge_single_file",
		"distribute",
		"binary_search"
	};
	const char * item_names[] = {
		"int",
		"std::string",
		"keyed_struct"
	};
	const char * action_names[] = {
		"Setup",
		"Run",
		"Validate"
	};

	std::cerr << "Test info:\n"
			  << "  File size:   " << file_size << "\n"
			  << "  Block size:  " << block_size() << "\n"
			  << "  Compression: " << compression << "\n"
			  << "  Readahead:   " << readahead << "\n"
			  << "  Item type:   " << item_type << " (" << item_names[item_type] << ")\n"
			  << "  Test:        " << test << " (" << test_names[test] << ")\n"
			  << "  Action:      " << action << " (" << action_names[action] << ")\n";

	if (K) {
		std::cerr << "  Parameter:   " << K << "\n";
	}

#ifdef TEST_NEW_STREAMS
	std::cerr << "  Job threads: " << job_threads << "\n";
	if (job_threads == 0) die("Need at least one job thread");
#else
	std::cerr << "  Job threads: " << "N/A" << "\n";
	if (job_threads != 0) die("job_thread parameter must be 0 for old streams");
#endif

	cmd_options = {compression, readahead, item_type, test, action_t(action), K, job_threads};

	{
		std::string word_path = "/usr/share/dict/words";
		std::ifstream word_stream(word_path);
		if (!word_stream) die(word_path + " couldn't be read!");
		std::string w;
		while (word_stream >> w) {
			words.push_back(w);
		}
	}
}

struct int_generator {
	using item_type = long long;

	long long i = 0;

	long long next(ssize_t inc = 1) {
		long long tmp = i;
		i += inc;
		return tmp;
	}

	bool check_total_items(size_t total_items) {
		return total_items <= (1ull << 63);
	}
};

struct string_generator {
	using item_type = std::string;

	size_t N;
	size_t i = 0;

	string_generator() : N(words.size()) {}

	std::string next(ssize_t inc = 1) {
		auto tmp = words[i / N] + "-" + words[i % N];
		i += inc;
		return tmp;
	}

	bool check_total_items(size_t total_items) {
		return total_items <= N * N;
	}
};

struct keyed_generator {
	struct keyed_struct {
		uint32_t key;
		unsigned char data[60];

		bool operator==(const keyed_struct & o) const {
			return key == o.key;
		}

		bool operator!=(const keyed_struct & o) const {
			return key != o.key;
		}

		bool operator<(const keyed_struct & o) const {
			return key < o.key;
		}
	};

	using item_type = keyed_struct;

	keyed_struct current;

	keyed_generator() {
		current.key = 0;
		for (unsigned char i = 0; i < sizeof(current.data); i++) {
			current.data[i] = 'A' + i;
		}
	}

	keyed_struct next(ssize_t inc = 1) {
		auto tmp = current;
		current.key += inc;
		return tmp;
	}

	bool check_total_items(size_t total_items) {
		return total_items <= (1ull << 32);
	}
};

template <typename T, typename FS>
struct speed_test_t {
	size_t item_size = sizeof(typename T::item_type);
	size_t total_items = file_size / item_size;

	int file_ctr = 0;

	speed_test_t() {
		if (!T().check_total_items(total_items)) {
			die(std::string("Item type doesn't support ") + std::to_string(total_items) + " total items.");
		}
	}

	virtual ~speed_test_t() = default;

	virtual void init() = 0;
	virtual void setup() = 0;
	virtual void run() = 0;
	virtual bool validate() = 0;

	bool validate_sequential(FS & f) {
		ensure_open_read(f);

		if (f.size() != no_file_size && f.size() != this->total_items) return false;

		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			if (f.read() != gen.next()) {
				return false;
			}
		}

		return true;
	}

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
		// Truncate files during setup
		if (cmd_options.action == SETUP) {
			boost::filesystem::remove(get_fname());
			file_ctr--;
		}
		f.open(get_fname(), get_flags(), max_user_data_size);
	}
#endif

	void open_file_stream(FS & f, size_t max_user_data_size = 0) {
		// Truncate files during setup
		if (cmd_options.action == SETUP) {
			boost::filesystem::remove(get_fname());
			file_ctr--;
		}
#ifdef TEST_NEW_STREAMS
		f.open(get_fname(), get_flags(), max_user_data_size);
#else
		f.open(get_fname(), cmd_options.compression? open::compression_all: open::defaults, max_user_data_size);
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
		ensure_open_write(f);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}

	bool validate() override {
		return this->validate_sequential(f);
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
		ensure_open_write(f);

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

	bool validate() override {
		return this->validate_sequential(f);
	}
};

template <typename T, typename FS>
struct read_single : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {
		ensure_open_write(f);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}

	void run() override {
		ensure_open_read(f);

		for (size_t i = 0; i < this->total_items; i++) f.read();
	}

	bool validate() override {
		return this->validate_sequential(f);
	}
};

template <typename T, typename FS>
struct read_back_single : speed_test_t<T, FS> {
	FS f;

	void init() override {
		this->open_file_stream(f);
	}

	void setup() override {
		ensure_open_write_reverse(f);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) f.write(gen.next());
	}

	void run() override {
		ensure_open_read_reverse(f);

#ifdef TEST_NEW_STREAMS
		f.seek(0, whence::end);
#else
		f.seek(0, FS::end);
#endif
		for (size_t i = 0; i < this->total_items; i++) f.read_back();
	}

	bool validate() override {
		ensure_open_read_reverse(f);

#ifdef TEST_NEW_STREAMS
		f.seek(0, whence::end);
#else
		f.seek(0, FS::end);
#endif

		if (f.size() != no_file_size && f.size() != this->total_items) return false;

		T gen;
		gen.next(this->total_items - 1);
		for (size_t i = 0; i < this->total_items; i++) {
			if (f.read_back() != gen.next(-1)) {
				return false;
			}
		}

		return true;
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
		for (size_t i = 0; i < cmd_options.K; i++) {
			ensure_open_write(inputs[i]);
		}

		std::mt19937_64 rng;
		std::uniform_int_distribution<size_t> dist(0, cmd_options.K - 1);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			inputs[dist(rng)].write(gen.next());
		}
	}

	void run() override {
		ensure_open_write(output);

		for (size_t i = 0; i < cmd_options.K; i++) {
			ensure_open_read(inputs[i]);
		}

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

	bool validate() override {
		return this->validate_sequential(output);
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
		auto * inputs = new FS[cmd_options.K];
		for (size_t i = 0; i < cmd_options.K; i++) {
			this->open_file_stream(inputs[i]);
		}

		// Write to individual files then concat
		std::mt19937_64 rng;
		std::uniform_int_distribution<size_t> dist(0, cmd_options.K - 1);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			inputs[dist(rng)].write(gen.next());
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

	bool validate() override {
		return this->validate_sequential(output);
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
		ensure_open_write(input);

		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			input.write(gen.next());
		}
	}

	void run() override {
		ensure_open_read(input);
		ensure_open_write(outputs[0]);
		ensure_open_write(outputs[1]);

		for (size_t i = 0; i < this->total_items; i++) {
			outputs[(i % 3) % 2].write(input.read());
		}
	}

	bool validate() override {
		ensure_open_read(outputs[0]);
		ensure_open_read(outputs[1]);

		if (outputs[0].size() != no_file_size && outputs[1].size() != no_file_size && outputs[0].size() + outputs[1].size() != this->total_items) return false;

		typename T::item_type item1, item2;

		item1 = outputs[0].read();
		item2 = outputs[1].read();

		T gen;
		for (size_t i = 0; i < this->total_items; i++) {
			auto n = gen.next();
			if (n == item1) {
				if (outputs[0].can_read())
					item1 = outputs[0].read();
			} else if (n == item2) {
				if (outputs[1].can_read())
					item2 = outputs[1].read();
			} else {
				return false;
			}
		}

		return true;
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

	bool validate() override {
		return true;
	}
};

// Disable binary_search test for old serialization streams
#ifdef TEST_OLD_STREAMS
template <typename T>
struct serialization_adapter;

template <typename T>
struct binary_search<T, serialization_adapter<typename T::item_type>> : speed_test_t<T, serialization_adapter<typename T::item_type>> {
	void init() override {skip();}
	void setup() override {}
	void run() override {}
	bool validate() override {return true;}
};
#endif

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
	switch (cmd_options.action) {
	case SETUP: test->setup(); break;
	case RUN: test->run(); break;
	case VALIDATE: {
		bool result = test->validate();

		if (!result) {
			die("Validation failed");
		}

		break;
	}
	}

	delete test;
}
