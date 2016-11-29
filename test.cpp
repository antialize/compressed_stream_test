// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream.h>
#include <iostream>
#include <log.h>
#include <random>
#include <map>
#include <set>
#include <csignal>

#define TMP_FILE "/tmp/hello.tst"

template <typename T>
class internal_stream;


template <typename T>
struct internal_file {
	internal_stream<T> stream();

	file_size_t size() const noexcept {return m_items.size();}

	void open() {}
	void close() { m_items.clear(); }

	std::vector<T> m_items;
};


template <typename T>
struct internal_stream {
	internal_stream(internal_file<T> * file, file_size_t offset, bool direct = false)
		: m_offset(offset), m_file(file), m_direct(direct) {}

	const T & peak() const noexcept {assert(can_read()); return m_file->m_items[m_offset];}
	const T & read() noexcept {assert(can_read()); return m_file->m_items[m_offset++];}
	const T & peak_back() const noexcept {assert(can_read_back()); return m_file->m_items[m_offset-1];}
	const T & back() noexcept {assert(can_read_back()); return m_file->m_items[--m_offset];}
	bool can_read() const noexcept {return m_offset != m_file->m_items.size();}
	bool can_read_back() const noexcept {return m_offset != 0;}
	file_size_t offset() const noexcept {return m_offset;}

	void write(const T & t) {
		if (m_offset == m_file->m_items.size())	{
			m_file->m_items.push_back(t);
		} else {
			assert(m_direct);
			m_file->m_items[m_offset] = t;
		}
		m_offset++;
	}

	void seek(file_size_t offset, whence w) {
		if (offset != 0 || (w != whence::set && w != whence::end)) {
			assert(m_direct);
		}
		switch (w) {
		case whence::set: m_offset = offset; break;
		case whence::cur: m_offset += offset; break;
		case whence::end: m_offset = m_file->size() + offset; break;
		}
	}

	file_size_t m_offset;
	internal_file<T> * m_file;
	bool m_direct;
};

template <typename T>
internal_stream<T> internal_file<T>::stream() {
	return internal_stream<T>(this, 0);
}

template <typename T>
void ensure(T expect, T got, const char * name) {
	if (expect == got) return;
	log_info() << "Expected " << expect << " but got " << got << " in " << name << std::endl;
	abort();
}

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

void task_title(std::string title, size_t stream = (size_t)-1) {
	auto l = log_info();
	l << "\n==> " << title;
	if (stream != -1) l << " (" << stream << ")";
	l << "\n" << std::endl;
}

int random_test() {
	enum class task {
		create_stream,
		destroy_stream,
		write_end,
		read,
		seek_start,
		get_offset,
		get_size,
		can_read,
		open_file,
		close_file
	};

	file<int> f1;
	f1.open(TMP_FILE);
	bool open = true;

	internal_file<int> f2;

	std::vector<stream<int>> s1;
	std::vector<internal_stream<int>> s2;

	std::default_random_engine rng;

	while (true) {
		std::vector<std::pair<task, size_t> > tasks;

		if (!open) {
			tasks.emplace_back(task::open_file, 1);
		} else {
			if (s1.size() < 1)
				tasks.emplace_back(task::create_stream, 5 - s1.size());
			if (s1.size()) {
				tasks.emplace_back(task::destroy_stream, s1.size());
				tasks.emplace_back(task::write_end, 20);
				tasks.emplace_back(task::read, 20);
				tasks.emplace_back(task::seek_start, 6);
				tasks.emplace_back(task::get_offset, 20);
				tasks.emplace_back(task::can_read, 20);
				tasks.emplace_back(task::get_size, 20);
			} else {
				tasks.emplace_back(task::close_file, 2);
			}
		}

		const size_t sum = std::accumulate(tasks.begin(), tasks.end(), 0, [](size_t a, std::pair<task, size_t> b) {return a+b.second;});
		size_t choice = std::uniform_int_distribution<size_t>(0, sum-1)(rng);
		for (const auto & t: tasks) {
			if (choice >= t.second) {
				choice -= t.second;
				continue;
			}
			const size_t s = std::uniform_int_distribution<size_t>(0, std::max<size_t>(1, s1.size()) -1)(rng);
			switch (t.first) {
			case task::close_file:
				task_title("Close file");
				f1.close();
				f2.close();
				open = false;
				break;
			case task::open_file:
				task_title("Open file");
				f1.open(TMP_FILE);
				f2.open();
				open = true;
				break;
			case task::create_stream:
				task_title("Create stream", s1.size());
				s1.push_back(f1.stream());
				s2.push_back(f2.stream());
				break;
			case task::destroy_stream:
				task_title("Destroy stream", s);
				s1.erase(s1.begin() + s);
				s2.erase(s2.begin() + s);
				break;
			case task::can_read:
				break;
				task_title("Can read", s);
				ensure(s1[s].can_read(), s2[s].can_read(), "can_read");
				break;
			case task::read: {
				if (s2[s].offset() == f2.size()) break;
				auto count = std::uniform_int_distribution<size_t>(1, std::min(file_size_t(1024), f2.size() - s2[s].offset()))(rng);
				task_title("Read " + std::to_string(count) + " at " + std::to_string(s1[s].offset()), s);
				for (size_t i=0; i < count; ++i) {
					ensure(s1[s].read(), s2[s].read(), "read");
				}
				break;
			}
			case task::seek_start:
				task_title("Seek start", s);
				s1[s].seek(0, whence::set);
				s2[s].seek(0, whence::set);
				break;
			case task::write_end: {
				auto count = std::uniform_int_distribution<size_t>(1, 1024)(rng);
				task_title("Write end " + std::to_string(count), s);
				s1[s].seek(0, whence::end);
				s2[s].seek(0, whence::end);
				std::uniform_int_distribution<int> d;
				size_t fs = f1.size();
				for (size_t i=0; i < count; ++i) {
					auto v = d(rng);
					s1[s].write(v);
					s2[s].write(v);
				}
				ensure(fs + count, f1.size(), "fsize");
				break;
			}
			case task::get_offset:
				break;
				task_title("Get offset", s);
				ensure(s1[s].offset(), s2[s].offset(), "offset");
				break;
			case task::get_size:
				break;
				task_title("Get size");
				ensure(f1.size(), f2.size(), "size");
				break;
			}
			break;
		}
	}
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

int write_fail() {
	file<int> f;
	f.open(TMP_FILE);
	auto s1 = f.stream();
	auto s2 = f.stream();

	for (int i = 0; i < 1000; i++)
		s1.write(i);

	s2.write(134);

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

	for (int i = 0; i < 1500; i++) {
		s.write(cnt++);
	}

	s.seek(0, whence::set);
	s.seek(0, whence::end);

	for (int i = 0; i < 1000; i++) {
		s.write(cnt++);
	}

	s.seek(0, whence::set);

	for (int i = 0; i < 2500; i++) {
		ensure(i, s.read(), "read");
	}

	return EXIT_SUCCESS;
}

int open_close() {
	file<int> f;
	f.open(TMP_FILE);
	{
		auto s = f.stream();
		auto b = s.logical_block_size();
		for (int i = 0; i < 100 * b; i++)
			s.write(i);
	}

	f.close();
	f.open(TMP_FILE);

	auto s = f.stream();
	s.seek(0, whence::end);
	s.write(1337);
	s.seek(0, whence::set);
	ensure(1337, s.read(), "read");

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


typedef int(*test_fun_t)();

std::string current_test;

int run_test(test_fun_t fun, int job_threads) {
	file_stream_init(job_threads);

	int ans = fun();

	file_stream_term();

	return ans;
}

void signal_handler(int signal) {
	psignal(signal, ("\n >>> Caught signal during run of test " + current_test).c_str());
}

int main(int argc, char ** argv) {
	std::signal(SIGABRT, signal_handler);
	std::signal(SIGINT, signal_handler);

	std::map<std::string, test_fun_t> tests = {
		{"flush", flush_test},
		{"random", random_test},
		{"write_read", write_read},
		{"write_fail", write_fail},
		{"size", size_test},
		{"write_seek_write_read", write_seek_write_read},
		{"open_close", open_close},
		{"seek_start_seek_end", seek_start_seek_end},
		{"write_end", write_end},
	};

	std::set<std::string> excluded_in_all = {"random", "write_fail"};

	std::string test = argc > 1 ? argv[1] : "";
	auto it = tests.find(test);

	int default_job_threads = 1;

	std::vector<std::pair<std::string, test_fun_t>> tests_to_run;
	if (it != tests.end()) {
		tests_to_run = {*it};
	} else {
		if (test == "all") {
			for (auto p : tests) {
				if (!excluded_in_all.count(p.first))
					tests_to_run.push_back(p);
			}
		}
	}

	if (tests_to_run.empty() || argc > 3) {
		std::cerr << "Usage: t testname [job_threads (default " << default_job_threads << ")]\n\n"
				  << "Available tests:\n";
		std::cerr << "\t" << "all - Runs all tests with a *\n";
		for (auto p : tests) {
			std::cerr << "\t" << p.first << (excluded_in_all.count(p.first)? "": " *")<< "\n";
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
