// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#include <file_stream.h>
#include <cassert>
#include <iostream>
#include <log.h>
#include <random>

template <typename T>
class internal_stream;


template <typename T>
struct internal_file {
	internal_stream<T> stream();

	uint64_t size() const noexcept {return m_items.size();}
	
	std::vector<T> m_items;
};


template <typename T>
struct internal_stream {
	internal_stream(internal_file<T> * file, uint64_t offset)
		: m_offset(offset), m_file(file) {}
	
	const T & peak() const noexcept {return m_file->m_items[m_offset];}
	const T & read() noexcept {return m_file->m_items[m_offset++];}
	const T & peak_back() const noexcept {return m_file->m_items[m_offset-1];}
	const T & back() noexcept {return m_file->m_items[--m_offset];}
	bool can_read() const noexcept {return m_offset != m_file->m_items.size();}
	bool can_read_back() const noexcept {return m_offset != 0;}
	uint64_t offset() const noexcept {return m_offset;}
	
	void write(const T & t) {
		if (m_offset == m_file->m_items.size())	m_file->m_items.push_back(t);
		else m_file->m_items[m_offset] = t;
		m_offset++;
	}

	void seek(uint64_t offset, whence w) {
		switch (w) {
		case whence::set: m_offset = offset; break;
		case whence::cur: m_offset += offset; break;
		case whence::end: m_offset = m_file->size() + offset; break;
		}
	}

	uint64_t m_offset;
	internal_file<T> * m_file;
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
	f.open("/dev/shm/hello.tst");

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

int random_test() {
	enum class task {
		create_stream,
		destroy_stream,
		write_end,
		read,
		seek_start,
		get_offset,
		get_size,
		can_read
	};


	file<int> f1;
	f1.open("/dev/shm/hello.tst");
	
	internal_file<int> f2;
	
	std::vector<stream<int>> s1;
	std::vector<internal_stream<int>> s2;

	std::default_random_engine rng;
  
	while (true) {
		std::vector<std::pair<task, size_t> > tasks;

		if (s1.size() < 5)
			tasks.emplace_back(task::create_stream, 5 - s1.size());
		if (s1.size()) {
			tasks.emplace_back(task::destroy_stream, s1.size());
			tasks.emplace_back(task::write_end, 20);
			tasks.emplace_back(task::read, 20);
			tasks.emplace_back(task::seek_start, 6);
			tasks.emplace_back(task::get_offset, 20);
			tasks.emplace_back(task::can_read, 20);
			tasks.emplace_back(task::get_size, 20);
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
			case task::create_stream:
				log_info() << "Create stream" << std::endl;
				s1.push_back(f1.stream());
				s2.push_back(f2.stream());
				break;
			case task::destroy_stream:
				log_info() << "Destroy stream" << std::endl;
				s1.erase(s1.begin() + s);
				s2.erase(s2.begin() + s);
				break;
			case task::can_read:
				log_info() << "Can read" << std::endl;
				ensure(s1[s].can_read(), s2[s].can_read(), "can_read");
				break;
			case task::read: {
				if (s2[s].offset() == f2.size()) break;
				auto count = std::uniform_int_distribution<size_t>(1, std::min(uint64_t(1024), f2.size() - s2[s].offset()))(rng);
				log_info() << "read " << count << std::endl;
				for (size_t i=0; i < count; ++i) {
					ensure(s1[s].read(), s2[s].read(), "read");
				}
				break;
			}
			case task::seek_start:
				log_info() << "seek start" << std::endl;
				s1[s].seek(0, whence::set);
				s2[s].seek(0, whence::set);
				break;
			case task::write_end: {
				auto count = std::uniform_int_distribution<size_t>(1, 1024)(rng);
				log_info() << "write end " << count << std::endl;
				s1[s].seek(0, whence::end);
				s2[s].seek(0, whence::end);
				std::uniform_int_distribution<int> d;
				for (size_t i=0; i < count; ++i) {
					auto v = d(rng);
					s1[s].write(v);
					s2[s].write(v);
				}				  
				break;
			}
			case task::get_offset:
				log_info() << "offset" << std::endl;
				ensure(s1[s].offset(), s2[s].offset(), "offset");
			case task::get_size: break;
				log_info() << "size" << std::endl;
				ensure(f1.size(), f2.size(), "size");
			}
			break;
		}
	}
	return EXIT_SUCCESS;
}

int write_read() {
	file<int> f;
	f.open("/dev/shm/hello.tst");
	stream<int> s=f.stream();
	for (int i=0; i < 10000; ++i) s.write(i);
	s.seek(0);
	for (int i=0; i < 10000; ++i) ensure(i, s.read(), "read");
	return EXIT_SUCCESS;
}

int main(int argc, char ** argv) {
  file_stream_init(4);

  std::string test = argc > 1 ? argv[1] : "";
  int ans = 0;
  if (test == "flush")
	  ans = flush_test();
  else if (test == "write_read")
	  ans = write_read();
  else
	  ans =  random_test();
  
  file_stream_term();
  return ans;
}
