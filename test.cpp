// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <test.h>
#include <file_stream.h>
#include <iostream>
#include <random>
#include <map>
#include <set>
#include <csignal>
#include <unistd.h>
#include <sstream>

open_flags::open_flags compression_flag = open_flags::default_flags;

int flush_test() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);

	auto s1 = f.stream();
	auto s2 = f.stream();
	s1.write(42);
	ensure(42, s2.read(), "read");

	for (int i=0; i < 10000; ++i)
		s2.write(i);

	for (int i=0; i < 10000; ++i)
		ensure(i, s1.read(), "read");

	return EXIT_SUCCESS;
}

int write_read() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();
	for (int i=0; i < 10000; ++i) s.write(i);
	s.seek(0);
	for (int i=0; i < 10000; ++i) ensure(i, s.read(), "read");
	return EXIT_SUCCESS;
}

int size_test() {
	file<uint8_t> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();

	file_size_t B = block_size();

	for(int i = 0; i < 10 * B; i++)
		s.write((uint8_t)i);

	ensure(10 * B, f.size(), "size");

	s.seek(0, whence::set);
	for(int i = 0; i < 9 * B; i++)
		ensure((uint8_t)i, s.read(), "read");

	ensure(10 * B, f.size(), "size");

	s.seek(0, whence::end);
	for(int i = 0; i < B; i++)
		s.write((uint8_t)i);

	ensure(11 * B, f.size(), "size");

	return EXIT_SUCCESS;
}

int write_seek_write_read() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);

	auto s = f.stream();

	int cnt = 0;

	for (int j = 0; j < 100; j++) {
		for (int i = 0; i < 1500; i++) {
			s.write(cnt++);
		}

		s.seek(0, whence::set);
		s.seek(0, whence::end);
	}

	s.seek(0, whence::set);

	for (int i = 0; i < 1500 * 100; i++) {
		ensure(i, s.read(), "read");
	}

	return EXIT_SUCCESS;
}

int open_close() {
	file<int> f;
	block_size_t b;
	f.open(TMP_FILE, compression_flag);
	{
		auto s = f.stream();
		b = s.logical_block_size();
		for (int i = 0; i < 100 * b; i++)
			s.write(i);
	}

	f.close();
	f.open(TMP_FILE, open_flags::truncate | compression_flag);

	auto s = f.stream();
	s.seek(0, whence::end);
	s.write(1337);
	s.seek(0, whence::set);
	ensure(1337, s.read(), "read");

	for (int i = 0; i < 100 * b; i++)
		s.write(i);

	f.close();

	f.open(TMP_FILE, compression_flag);
	auto s2 = f.stream();
	ensure(1337, s2.read(), "read");
	for (int i = 0; i < 100 * b; i++)
		ensure(s2.read(), i, "read");

	f.close();
	for(int i = 0; i < 10; i++) {
		f.open(TMP_FILE, compression_flag);
		f.close();
	}

	return EXIT_SUCCESS;
}

int seek_start_seek_end() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	{
		auto s = f.stream();
		s.seek(0, whence::set);
	}
	{
		auto s = f.stream();
		s.seek(0, whence::end);
	}
	return EXIT_SUCCESS;
}

int write_end() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);

	auto s1 = f.stream();
	auto s2 = f.stream();
	auto b = s1.logical_block_size();
	for(int i = 0; i < 20 * b + 1; i++) {
		s1.write(i);
	}

	for(int i = 0; i < 20 * b + 1; i++) {
		s2.read();
	}

	s1.write(1337);

	return EXIT_SUCCESS;
}

int open_close_dead_stream() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	{
		auto s = f.stream();
		f.close();
		f.open(TMP_FILE, compression_flag);
	}
	auto s = f.stream();
	for (int i = 0; i < 10000; i++)
		s.write(i);

	return EXIT_SUCCESS;
}

int peek_test() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();

	for (int i = 0; i < 10000; i++)
		s.write(i);

	s.seek(0, whence::set);
	for (int i = 0; i < 10000; i++) {
		ensure(s.peek(), i, "peek");
		ensure(s.read(), i, "read");
	}

	return EXIT_SUCCESS;
}

int peek_back_test() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();
	auto b = s.logical_block_size();
	int w = 20 * b;
	for (int i = 0; i < w; i++)
		s.write(i);

	for (int i = w - 1; i >= 0; i--) {
		ensure(i, s.peek_back(), "peek_back");
		ensure(i, s.read_back(), "read_back");
	}

	return EXIT_SUCCESS;
}

int read_back_test() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();
	auto b = s.logical_block_size();
	for (int i = 0; i < 20 * b; i++)
		s.write(i);

	s.seek(0, whence::set);

	for (int i = 0; i < 10 * b; i++)
		ensure(i, s.read(), "read");

	s.seek(0, whence::end);

	for (int i = 20 * b; i < 22 * b; i++)
		s.write(i);

	for (int i = 22 * b - 1; i >= 0; i--)
		ensure(i, s.read_back(), "read_back");

	return EXIT_SUCCESS;
}

int serialized_string() {
	block_size_t m = max_serialized_block_size();
	int N = 8;

	auto get_string = [=](double i){ return std::string(size_t(m * (0.2 + (i / N) * 0.4)), 'A'); };

	{
		serialized_file<std::string> f;
		f.open(TMP_FILE, compression_flag);
		auto s = f.stream();

		for (int i = 0; i < N; i++) {
			s.write(get_string(i));
		}
	}

	{
		serialized_file<std::string> f;
		f.open(TMP_FILE, compression_flag);
		auto s = f.stream();

		for (int i = 0; i < N; i++) {
			std::string str = s.read();
			std::string expected = get_string(i);
			ensure(expected.size(), str.size(), "read (size)");
			ensure(expected, str, "read");
		}
	}

	// Test get_predecessor_block edge case
	{
		serialized_file<std::string> f;
		f.open(TMP_FILE, compression_flag);
		auto s = f.stream();
		s.seek(0, whence::end);

		for (int i = N - 1; i >= 0; i--) {
			std::string str = s.read_back();
			std::string expected = get_string(i);
			ensure(expected.size(), str.size(), "read (size)");
			ensure(expected, str, "read");
		}
	}

	return EXIT_SUCCESS;
}

template <typename T, size_t repeat>
class debug_class {
private:
	bool constructed = false;
	bool moved = false;
	bool destructed = false;

	T value;

public:
	static std::atomic<size_t> live_instances;

	void set_value(T val) {
		check_alive();
		value = std::move(val);
	}

	T get_value() const {
		check_alive();
		return value;
	}

	debug_class() {
		//log_info() << "-> default ctor: " << std::endl;
		constructed = true;
		live_instances++;
	}

	void check_alive() const {
		assert(constructed);
		assert(!moved);
		assert(!destructed);
	}

	debug_class & operator=(const debug_class & o) = delete;
	debug_class(const debug_class & o) = delete;

	debug_class(debug_class && o) {
		//log_info() << "-> move ctor: " << o.value << std::endl;

		o.check_alive();

		constructed = o.constructed;
		moved = o.moved;
		destructed = o.destructed;

		value = o.value;

		o.moved = true;

		live_instances++;
	}


	debug_class & operator=(debug_class && o) {
		//log_info() << "-> move assignment: " << o.value << std::endl;

		o.check_alive();

		constructed = o.constructed;
		moved = o.moved;
		destructed = o.destructed;

		value = o.value;

		o.moved = true;

		live_instances++;

		return *this;
	}

	~debug_class() {
		//log_info() << "-> dtor: " << value << std::endl;

		assert(constructed);
		assert(!destructed);
		destructed = true;
		live_instances--;
	}
};

template <typename T, size_t repeat>
std::atomic<size_t> debug_class<T, repeat>::live_instances;

template <typename D, typename T, size_t repeat>
void serialize(D & dst, const debug_class<T, repeat> & o) {
	for (int i = 0; i < repeat; i++)
		serialize(dst, o.get_value());
}

template <typename S, typename T, size_t repeat>
void unserialize(S & src, debug_class<T, repeat> & o) {
	T val;
	unserialize(src, val);
	for (int i = 1; i < repeat; i++) {
		T val2;
		unserialize(src, val2);
		ensure(val, val2, "unserialize");
	}
	o.set_value(val);
}

int serialized_dtor() {
	constexpr size_t bytes_per_item = max_serialized_block_size() / 10;
	using T = debug_class<uint8_t, bytes_per_item>;

	int N = 100;

	ensure(size_t(0), T::live_instances.load(), "live_instances");

	{
		serialized_file<T> f;
		f.open(TMP_FILE, compression_flag);
		auto s = f.stream();
		for (int i = 0; i < N; i++) {
			T t;
			t.set_value(i);
			s.write(std::move(t));
		}

		const T & t = s.read_back();
		t.check_alive();

		ensure(true, T::live_instances.load() > 0, "live_instances");

		f.close();

		ensure(size_t(0), T::live_instances.load(), "live_instances");
	}

	{
		serialized_file<T> f;
		f.open(TMP_FILE, compression_flag);
		auto s = f.stream();
		for (int i = 0; i < N; i++) {
			const T & t = s.read();
			ensure(uint8_t(i), t.get_value(), "read");
		}
	}

	ensure(size_t(0), T::live_instances.load(), "live_instances");

	return EXIT_SUCCESS;
}

int user_data() {
	const char * user_data = "foobar";
	constexpr size_t size = 7;

	{
		file<int> f;
		f.open(TMP_FILE, compression_flag, 2 * size);
		ensure(size_t(0), f.user_data_size(), "user_data_size");
		ensure(2 * size, f.max_user_data_size(), "user_data_size");
	}
	{
		file<int> f;
		f.open(TMP_FILE, compression_flag);
		ensure(size_t(0), f.user_data_size(), "user_data_size");
		ensure(2 * size, f.max_user_data_size(), "user_data_size");

		f.write_user_data(user_data, size);
		ensure(size, f.user_data_size(), "user_data_size");

		f.write_user_data(user_data, 1);
		ensure(size, f.user_data_size(), "user_data_size");

		char user_data2[size];
		f.read_user_data(user_data2, size);
		ensure<std::string>(user_data, user_data2, "read_user_data");

		auto s = f.stream();
		s.write(105);
	}
	{
		file<int> f;
		f.open(TMP_FILE, compression_flag, 2 * size);
		ensure(size, f.user_data_size(), "user_data_size");

		char user_data2[size];
		f.read_user_data(user_data2, size);
		ensure<std::string>(user_data, user_data2, "read_user_data");

		auto s = f.stream();
		ensure(105, s.read(), "read");
	}

	return EXIT_SUCCESS;
}

int get_set_position() {
	file<int> f;
	f.open(TMP_FILE, compression_flag);
	auto s = f.stream();

	auto p1 = s.get_position();
	s.set_position(p1);

	for (int i = 0; i < 10000; i++)
		s.write(i);

	auto p2 = s.get_position();

	for (int i = 0; i < 10000; i++)
		s.write(i);

	auto p3 = s.get_position();

	s.set_position(p2);
	ensure(0, s.peek(), "peek (p2)");
	ensure(9999, s.peek_back(), "peek_back (p2)");

	s.set_position(p1);
	ensure(0, s.peek(), "peek (p1)");

	s.set_position(p3);
	ensure(9999, s.peek_back(), "peek_back (p3)");

	return EXIT_SUCCESS;
}

typedef int(*test_fun_t)();

std::string current_test;

int run_test(test_fun_t fun, int job_threads) {
	unlink(TMP_FILE);

	file_stream_init(job_threads);

	int ans = fun();

	file_stream_term();

	return ans;
}

void signal_handler(int signal) {
	psignal(signal, ("\n >>> Caught signal during run of test " + current_test).c_str());
	std::_Exit(EXIT_FAILURE);
}

int main(int argc, char ** argv) {
	std::signal(SIGABRT, signal_handler);
	std::signal(SIGINT, signal_handler);

	int default_job_threads = 4;

	std::map<std::string, test_fun_t> tests = {
		{"flush", flush_test},
		{"write_read", write_read},
		{"size", size_test},
		{"write_seek_write_read", write_seek_write_read},
		{"open_close", open_close},
		{"seek_start_seek_end", seek_start_seek_end},
		{"write_end", write_end},
		{"open_close_dead_stream", open_close_dead_stream},
		{"peek", peek_test},
		{"peek_back", peek_back_test},
		{"read_back", read_back_test},
		{"serialized_string", serialized_string},
		{"serialized_dtor", serialized_dtor},
		{"user_data", user_data},
		{"get_set_position", get_set_position},
	};

	std::stringstream usage;
	usage << "Usage: t [-h] [-C] [-t threads] test_name\n"
		  << "Available tests:\n";
	usage << "\t" << "all - Runs all tests\n";
	for (auto p : tests) {
		usage << "\t" << p.first << "\n";
	}

	int job_threads = default_job_threads;

	int opt;
	while ((opt = getopt(argc, argv, "hCt:")) != -1) {
		switch (opt) {
		case 'h':
			std::cout << usage.str();
			return EXIT_SUCCESS;
		case 'C':
			compression_flag = open_flags::no_compress;
			break;
		case 't':
			job_threads = std::stoi(optarg);
			break;
		case '?':
			return EXIT_FAILURE;
		}
	}

	std::string test = optind < argc ? argv[optind] : "";
	auto it = tests.find(test);

	std::map<std::string, test_fun_t> tests_to_run;
	if (it != tests.end()) {
		tests_to_run = {*it};
	} else if (test == "all") {
		tests_to_run = tests;
	}

	if (tests_to_run.empty() || optind + 1 != argc) {
		std::cerr << usage.str();
		return EXIT_FAILURE;
	}

	for (auto p : tests_to_run) {
		std::cout << "\n>>> Running test " << p.first << "\n\n";
		current_test = p.first;
		int ans = run_test(p.second, job_threads);
		if (ans != EXIT_SUCCESS) {
			std::cerr << "Test " << p.first << " failed\n";
			return ans;
		}
	}

	return EXIT_SUCCESS;
}
