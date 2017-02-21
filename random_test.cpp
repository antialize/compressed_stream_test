#include <test.h>
#include <file_stream.h>
#include <log.h>
#include <vector>
#include <random>
#include <unistd.h>
#include <map>
#include <set>

template <typename T>
class internal_stream;

template <typename T>
struct internal_file {
	internal_stream<T> stream();

	file_size_t size() const noexcept {return m_items.size();}
	void truncate(file_size_t offset) {
		assert(offset <= size());
		m_items.resize(offset);
	}

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
	void set_offset(file_size_t offset) {assert(offset <= m_file->size()); m_offset = offset;};

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

#define RANDOM_TASKS(X) \
	X(create_stream), \
	X(destroy_stream), \
	X(write_end), \
	X(read), \
	X(peek), \
	X(read_back), \
	X(peek_back), \
	X(seek_start), \
	X(get_offset), \
	X(get_size), \
	X(can_read), \
	X(can_read_back), \
	X(open_default), \
	X(open_readonly), \
	X(open_truncate), \
	X(close_file), \
	X(get_position), \
	X(set_position), \
	X(truncate), \

enum class task {
#define X(t) t
	RANDOM_TASKS(X)
#undef X
};

std::map<std::string, task> task_names = {
#define X(t) {#t, task::t}
	RANDOM_TASKS(X)
#undef X
};

void random_test(int max_streams, bool whitelist, std::set<task> & task_list, std::default_random_engine::result_type seed, size_t total_tasks) {
	file<int> f1;
	f1.open(TMP_FILE, open_flags::truncate);
	bool open = true;
	bool readonly = false;

	internal_file<int> f2;

	std::vector<stream<int>> s1;
	std::vector<internal_stream<int>> s2;

	std::vector<stream_position> pos;

	std::default_random_engine rng(seed);

	for (size_t i = 0; i < total_tasks; i++) {
		std::vector<std::pair<task, size_t> > tasks;
		auto add_task = [&](task t, int p){ if (task_list.count(t) == whitelist) tasks.emplace_back(t, p); };

		if (!open) {
			add_task(task::open_default, 10);
			add_task(task::open_readonly, 2);
			add_task(task::open_truncate, 10);
		} else {
			if (s1.size() < max_streams)
				add_task(task::create_stream, max_streams - s1.size());
			if (s1.size()) {
				add_task(task::destroy_stream, s1.size());
				add_task(task::read, 20);
				add_task(task::peek, 20);
				add_task(task::read_back, 20);
				add_task(task::peek_back, 20);
				add_task(task::seek_start, 6);
				add_task(task::get_offset, 20);
				add_task(task::can_read, 20);
				add_task(task::can_read_back, 20);
				add_task(task::get_size, 20);

				add_task(task::get_position, 20);

				add_task(task::truncate, 20);

				if (pos.size())
					add_task(task::set_position, 20);

				if (!readonly) {
					add_task(task::write_end, 20);
				}
			} else {
				add_task(task::close_file, 2);
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
			const size_t p = std::uniform_int_distribution<size_t>(0, std::max<size_t>(1, pos.size()) -1)(rng);
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
				pos.clear();
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
			case task::get_position: {
				task_title("Get position", s);
				auto p1 = s1[s].get_position();
				auto p2 = s2[s].offset();
				ensure(p1.m_logical_offset + p1.m_index, p2, "get_position");
				pos.push_back(p1);
				break;
			}
			case task::set_position: {
				task_title("Set position", s);
				auto p1 = pos[p];
				s1[s].set_position(p1);
				auto o = s1[s].offset();
				ensure(p1.m_logical_offset + p1.m_index, o, "offset");
				s2[s].set_offset(o);
				break;
			}
			case task::truncate: {
				stream_position largest_pos = {0, 0, 0, 0};
				size_t i = 0;
				size_t largest_i;
				for (auto & s : s1) {
					auto pos = s.get_position();
					if (largest_pos < pos) {
						largest_pos = pos;
						largest_i = i;
					}
					i++;
				}
				task_title("Truncate to logical size " + std::to_string(largest_pos.m_logical_offset), largest_i);
				f1.truncate(largest_pos);
				f2.truncate(s2[largest_i].offset());
			}
			}
			break;
		}
	}
}

int main(int argc, char ** argv) {
	const char * usage = "Usage: random_test [-h] [-w] [-b] [-t threads] [-s streams] [-r seed] [-R restart_period] [task_names]...\n";

	int whitelist = -1;
	size_t worker_threads = 4;
	int max_streams = 5;
	auto seed = std::default_random_engine::default_seed;
	size_t restart_period = SIZE_MAX;

	int opt;
	while ((opt = getopt(argc, argv, "hwbt:s:r:R:")) != -1) {
		switch (opt) {
		case 'h':
			std::cout << usage;
			std::cout << "Task names:\n";
			for (auto p : task_names) {
				std::cout << "\t" << p.first << "\n";
			}
			return EXIT_SUCCESS;
		case 'w':
			whitelist = 1;
			break;
		case 'b':
			whitelist = 0;
			break;
		case 't':
			worker_threads = std::stoi(optarg);
			break;
		case 's':
			max_streams = std::stoi(optarg);
			break;
		case 'r':
			seed = std::stoull(optarg);
			break;
		case 'R':
			restart_period = std::stoull(optarg);
			break;
		case '?':
			return EXIT_FAILURE;
		}
	}

	std::set<task> task_list;
	for (int i = optind; i < argc; i++) {
		auto it = task_names.find(argv[i]);
		if (it == task_names.end()) {
			std::cerr << "Unknown task name: " << argv[i] << "\n";
			return EXIT_FAILURE;
		}
		task_list.insert(it->second);
	}

	if (whitelist == -1) {
		whitelist = task_list.size() > 0;
	}

	std::random_device rd;

	file_stream_init(worker_threads);
	while (true) {
		random_test(max_streams, whitelist, task_list, seed, restart_period);
		std::cout << '\n' << std::string(40, '=') << "\n\n";
		seed = rd();
	}
	// Unreachable
	//file_stream_term();
}
