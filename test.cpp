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

int flush_test() {
	file<int> f;
	f.open(TMP_FILE);

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
	f.open(TMP_FILE);
	stream<int> s=f.stream();
	for (int i=0; i < 10000; ++i) s.write(i);
	s.seek(0);
	for (int i=0; i < 10000; ++i) ensure(i, s.read(), "read");
	return EXIT_SUCCESS;
}

int size_test() {
	file<uint8_t> f;
	f.open(TMP_FILE);
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
	f.open(TMP_FILE);

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
	f.open(TMP_FILE);
	{
		auto s = f.stream();
		b = s.logical_block_size();
		for (int i = 0; i < 100 * b; i++)
			s.write(i);
	}

	f.close();
	f.open(TMP_FILE, open_flags::truncate);

	auto s = f.stream();
	s.seek(0, whence::end);
	s.write(1337);
	s.seek(0, whence::set);
	ensure(1337, s.read(), "read");

	for (int i = 0; i < 100 * b; i++)
		s.write(i);

	f.close();

	f.open(TMP_FILE);
	auto s2 = f.stream();
	ensure(1337, s2.read(), "read");
	for (int i = 0; i < 100 * b; i++)
		ensure(s2.read(), i, "read");

	f.close();
	for(int i = 0; i < 10; i++) {
		f.open(TMP_FILE);
		f.close();
	}

	return EXIT_SUCCESS;
}

int seek_start_seek_end() {
	file<int> f;
	f.open(TMP_FILE);
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
	f.open(TMP_FILE);

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
	f.open(TMP_FILE);
	{
		auto s = f.stream();
		f.close();
		f.open(TMP_FILE);
	}
	auto s = f.stream();
	for (int i = 0; i < 10000; i++)
		s.write(i);
}

int peek_test() {
	file<int> f;
	f.open(TMP_FILE);
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

int uncompressed_test() {
	{
		file<uint8_t> f;
		f.open(TMP_FILE, open_flags::no_compress);
		auto s = f.stream();

		for (int i = 0; i < 10000; i++)
			s.write((uint8_t)i);

		s.seek(0, whence::set);
		for (int i = 0; i < 10000; i++)
			ensure(s.read(), (uint8_t)i, "read");
	}

	{
		file<uint8_t> f;
		f.open(TMP_FILE, open_flags::no_compress);
		auto s = f.stream();

		for (int i = 0; i < 10000; i++)
			ensure(s.read(), (uint8_t)i, "read");
	}

	return EXIT_SUCCESS;
}

int peek_back_test() {
	file<int> f;
	f.open(TMP_FILE);
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
	f.open(TMP_FILE);
	auto s = f.stream();
	auto b = s.logical_block_size();
	for (int i = 0; i < 20 * b; i++)
		s.write(i);

	s.seek(0, whence::set);
	s.seek(0, whence::end);

	for (int i = 0; i < 20 * b; i++)
		s.read_back();

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
		{"uncompressed", uncompressed_test},
		{"peek_back", peek_back_test},
		{"read_back", read_back_test},
	};

	std::string test = argc > 1 ? argv[1] : "";
	auto it = tests.find(test);

	std::map<std::string, test_fun_t> tests_to_run;
	if (it != tests.end()) {
		tests_to_run = {*it};
	} else if (test == "all") {
		tests_to_run = tests;
	}

	if (tests_to_run.empty() || argc > 3) {
		std::cerr << "Usage: t testname [job_threads (default " << default_job_threads << ")]\n\n"
				  << "Available tests:\n";
		std::cerr << "\t" << "all - Runs all tests\n";
		for (auto p : tests) {
			std::cerr << "\t" << p.first << "\n";
		}
		return EXIT_FAILURE;
	}

	int job_threads = argc > 2 ? std::stoi(argv[2]) : default_job_threads;

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
