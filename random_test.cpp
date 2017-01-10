#include <test.h>
#include <file_stream.h>
#include <log.h>
#include <vector>
#include <random>
#include <unistd.h>

template <typename T>
class internal_stream;

template <typename T>
struct internal_file {
	internal_stream<T> stream();

	file_size_t size() const noexcept {return m_items.size();}

	void open(open_flags::open_flags flags = open_flags::default_flags) {
		if (flags & open_flags::truncate) {
			m_items.clear();
		}
	}
	void close() {}

	std::vector<T> m_items;
};


template <typename T>
struct internal_stream {
	internal_stream(internal_file<T> * file, file_size_t offset, bool direct = false)
		: m_offset(offset), m_file(file), m_direct(direct) {}

	const T & peek() const noexcept {assert(can_read()); return m_file->m_items[m_offset];}
	const T & read() noexcept {assert(can_read()); return m_file->m_items[m_offset++];}
	const T & peek_back() const noexcept {assert(can_read_back()); return m_file->m_items[m_offset-1];}
	const T & read_back() noexcept {assert(can_read_back()); return m_file->m_items[--m_offset];}
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

void task_title(std::string title, size_t stream = (size_t)-1) {
	auto l = log_info();
	l << "\n==> " << title;
	if (stream != -1) l << " (" << stream << ")";
	l << "\n" << std::endl;
}

int random_test(int max_streams) {
	enum class task {
		create_stream,
		destroy_stream,
		write_end,
		read,
		peek,
		read_back,
		peek_back,
		seek_start,
		get_offset,
		get_size,
		can_read,
		can_read_back,
		open_default,
		open_readonly,
		open_truncate,
		close_file,
	};

	file<int> f1;
	f1.open(TMP_FILE, open_flags::truncate);
	bool open = true;
	bool readonly = false;

	internal_file<int> f2;

	std::vector<stream<int>> s1;
	std::vector<internal_stream<int>> s2;

	std::default_random_engine rng;

	while (true) {
		std::vector<std::pair<task, size_t> > tasks;

		if (!open) {
			tasks.emplace_back(task::open_default, 10);
			tasks.emplace_back(task::open_readonly, 2);
			tasks.emplace_back(task::open_truncate, 10);
		} else {
			if (s1.size() < max_streams)
				tasks.emplace_back(task::create_stream, max_streams - s1.size());
			if (s1.size()) {
				tasks.emplace_back(task::destroy_stream, s1.size());
				tasks.emplace_back(task::read, 20);
				tasks.emplace_back(task::peek, 20);
				tasks.emplace_back(task::read_back, 20);
				tasks.emplace_back(task::peek_back, 20);
				tasks.emplace_back(task::seek_start, 6);
				tasks.emplace_back(task::get_offset, 20);
				tasks.emplace_back(task::can_read, 20);
				tasks.emplace_back(task::can_read_back, 20);
				tasks.emplace_back(task::get_size, 20);

				if (!readonly) {
					tasks.emplace_back(task::write_end, 20);
				}
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
			case task::open_default:
				task_title("Open file default");
				f1.open(TMP_FILE);
				f2.open();
				readonly = false;
				open = true;
				break;
			case task::open_truncate:
				task_title("Open file truncate");
				f1.open(TMP_FILE, open_flags::truncate);
				f2.open(open_flags::truncate);
				readonly = false;
				open = true;
				break;
			case task::open_readonly:
				task_title("Open file readonly");
				f1.open(TMP_FILE, open_flags::read_only);
				f2.open(open_flags::read_only);
				open = true;
				readonly = true;
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
				task_title("Can read", s);
				ensure(s1[s].can_read(), s2[s].can_read(), "can_read");
				break;
			case task::can_read_back:
				task_title("Can read back", s);
				ensure(s1[s].can_read_back(), s2[s].can_read_back(), "can_read_back");
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
			case task::read_back: {
				if (s2[s].offset() == 0) break;
				auto count = std::uniform_int_distribution<size_t>(1, std::min(file_size_t(1024), s2[s].offset()))(rng);
				task_title("Read back " + std::to_string(count) + " at " + std::to_string(s1[s].offset()), s);
				for (size_t i=0; i < count; ++i) {
					ensure(s1[s].read_back(), s2[s].read_back(), "read_back");
				}
				break;
			}
			case task::peek: {
				if (s2[s].offset() == f2.size()) break;
				task_title("Peek at " + std::to_string(s1[s].offset()), s);
				ensure(s1[s].peek(), s2[s].peek(), "peek");
				break;
			}
			case task::peek_back: {
				if (s2[s].offset() == 0) break;
				task_title("Peek back at " + std::to_string(s1[s].offset()), s);
				ensure(s1[s].peek_back(), s2[s].peek_back(), "peek_back");
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
				auto fs = f1.size();
				for (size_t i=0; i < count; ++i) {
					auto v = d(rng);
					s1[s].write(v);
					s2[s].write(v);
				}
				ensure(fs + count, f1.size(), "fsize");
				break;
			}
			case task::get_offset:
				task_title("Get offset", s);
				ensure(s1[s].offset(), s2[s].offset(), "offset");
				break;
			case task::get_size:
				task_title("Get size");
				ensure(f1.size(), f2.size(), "size");
				break;
			}
			break;
		}
	}
	return EXIT_SUCCESS;
}

int main(int argc, char ** argv) {
	const char * usage = "Usage: random_test [-h] [-t threads] [-s streams]\n";

	int worker_threads = 4;
	int max_streams = 5;

	int opt;
	while ((opt = getopt(argc, argv, "ht:s:")) != -1) {
		switch (opt) {
		case 'h':
			std::cout << usage;
			return EXIT_SUCCESS;
		case 't':
			worker_threads = std::stoi(optarg);
			break;
		case 's':
			max_streams = std::stoi(optarg);
			break;
		case '?':
			return EXIT_FAILURE;
		}
	}

	if (optind != argc) {
		std::cerr << usage;
		return EXIT_FAILURE;
	}

	file_stream_init(worker_threads);

	int res = random_test(max_streams);

	file_stream_term();

	return res;
}