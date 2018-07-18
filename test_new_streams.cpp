// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

#include "common.h"

#include <tpie/tpie_log.h>
#include <tpie/tpie.h>
#include <tpie/file_stream/file_stream.h>
#include <tpie/file_stream/check_file.h>
#include <tpie/serialization2.h>
#include <numeric>
#include <atomic>

using namespace tpie::new_streams;


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
	for (size_t i = 0; i < repeat; i++)
		tpie::serialize(dst, o.get_value());
}

template <typename S, typename T, size_t repeat>
void unserialize(S & src, debug_class<T, repeat> & o) {
	T val;
	tpie::unserialize(src, val);
	for (size_t i = 1; i < repeat; i++) {
		T val2;
		tpie::unserialize(src, val2);
		if (val != val2) {
			tpie::log_error() << "Unserialized two different values: " << val << " and " << val2 << std::endl;
			abort();
		}
	}
	o.set_value(val);
}


tpie::temp_file tmp_file;
void setup() {
	tmp_file.free();
}

template <tpie::open::type flags>
struct tests {

static bool flush_test() {
	file<int> f;
	f.open(tmp_file.path(), flags);

	auto s1 = f.stream();
	auto s2 = f.stream();
	s1.write(42);
	TEST_ENSURE_EQUALITY(42, s2.read(), "read");

	for (int i=0; i < 10000; ++i)
		s2.write(i);

	for (int i=0; i < 10000; ++i)
		TEST_ENSURE_EQUALITY(i, s1.read(), "read");

	return true;
}

static bool write_read() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();
	for (int i=0; i < 10000; ++i) s.write(i);
	s.seek(0);
	for (int i=0; i < 10000; ++i) TEST_ENSURE_EQUALITY(i, s.read(), "read");
	return true;
}

static bool size_test() {
	file<uint8_t> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();

	file_size_t B = block_size();

	for(size_t i = 0; i < 10 * B; i++)
		s.write((uint8_t)i);

	TEST_ENSURE_EQUALITY(10 * B, f.size(), "size");

	s.seek(0, whence::set);
	for(size_t i = 0; i < 9 * B; i++)
		TEST_ENSURE_EQUALITY((uint8_t)i, s.read(), "read");

	TEST_ENSURE_EQUALITY(10 * B, f.size(), "size");

	s.seek(0, whence::end);
	for(size_t i = 0; i < B; i++)
		s.write((uint8_t)i);

	TEST_ENSURE_EQUALITY(11 * B, f.size(), "size");

	return true;
}

static bool write_seek_write_read() {
	file<int> f;
	f.open(tmp_file.path(), flags);

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
		TEST_ENSURE_EQUALITY(i, s.read(), "read");
	}

	return true;
}

static bool open_close() {
	file<int> f;
	block_size_t b;
	f.open(tmp_file.path(), flags);

	{
		auto s = f.stream();
		b = s.logical_block_size();
		s.seek(0, whence::end);
		s.write(1337);
		s.seek(0, whence::set);
		TEST_ENSURE_EQUALITY(1337, s.read(), "read");

		for (int i = 0; i < 100 * (int)b; i++)
			s.write(i);
	}

	f.close();

	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		TEST_ENSURE_EQUALITY(1337, s.read(), "read");
		for (int i = 0; i < 100 * (int)b; i++)
			TEST_ENSURE_EQUALITY(s.read(), i, "read");
	}

	f.close();
	for(int i = 0; i < 10; i++) {
		f.open(tmp_file.path(), flags);
		f.close();
	}

	f.open(tmp_file.path(), flags);
	auto s = f.stream();

	return true;
}

static bool seek_start_seek_end() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		s.seek(0, whence::set);
	}
	{
		auto s = f.stream();
		s.seek(0, whence::end);
	}
	return true;
}

static bool write_end() {
	file<int> f;
	f.open(tmp_file.path(), flags);

	auto s1 = f.stream();
	auto s2 = f.stream();
	auto b = s1.logical_block_size();
	for(int i = 0; i < 20 * (int)b + 1; i++) {
		s1.write(i);
	}

	for(int i = 0; i < 20 * (int)b + 1; i++) {
		s2.read();
	}

	s1.write(1337);

	return true;
}

static bool open_close_dead_stream() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();
	for (int i = 0; i < 10000; i++)
		s.write(i);

	return true;
}

static bool peek_test() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();

	for (int i = 0; i < 10000; i++)
		s.write(i);

	s.seek(0, whence::set);
	for (int i = 0; i < 10000; i++) {
		TEST_ENSURE_EQUALITY(s.peek(), i, "peek");
		TEST_ENSURE_EQUALITY(s.read(), i, "read");
	}

	return true;
}

static bool peek_back_test() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();
	auto b = s.logical_block_size();
	int w = 20 * b;
	for (int i = 0; i < w; i++)
		s.write(i);

	for (int i = w - 1; i >= 0; i--) {
		TEST_ENSURE_EQUALITY(i, s.peek_back(), "peek_back");
		TEST_ENSURE_EQUALITY(i, s.read_back(), "read_back");
	}

	return true;
}

static bool read_back_test() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();
	auto b = s.logical_block_size();
	for (int i = 0; i < 20 * (int)b; i++)
		s.write(i);

	s.seek(0, whence::set);

	for (int i = 0; i < 10 * (int)b; i++)
		TEST_ENSURE_EQUALITY(i, s.read(), "read");

	s.seek(0, whence::end);

	for (int i = 20 * b; i < 22 * (int)b; i++)
		s.write(i);

	for (int i = 22 * (int)b - 1; i >= 0; i--)
		TEST_ENSURE_EQUALITY(i, s.read_back(), "read_back");

	return true;
}

static bool serialized_string() {
	block_size_t m = max_serialized_block_size();
	int N = 8;

	std::vector<std::string> strings;
	for (int i = 0; i < N; i++) {
		strings.push_back(std::string(size_t(m * (0.2 + (i / N) * 0.4)), 'A'));
	}

	{
		serialized_file<std::string> f;
		f.open(tmp_file.path(), flags);
		auto s = f.stream();

		for (int i = 0; i < N; i++) {
			s.write(strings[i]);
		}
	}

	{
		serialized_file<std::string> f;
		f.open(tmp_file.path(), flags);
		auto s = f.stream();

		for (int i = 0; i < N; i++) {
			std::string str = s.read();
			std::string expected = strings[i];
			TEST_ENSURE_EQUALITY(expected.size(), str.size(), "read (size)");
			TEST_ENSURE_EQUALITY(expected, str, "read");
		}
	}

	// Test get_predecessor_block edge case
	{
		serialized_file<std::string> f;
		f.open(tmp_file.path(), flags);
		auto s = f.stream();
		s.seek(0, whence::end);

		for (int i = N - 1; i >= 0; i--) {
			std::string str = s.read_back();
			std::string expected = strings[i];
			TEST_ENSURE_EQUALITY(expected.size(), str.size(), "read (size)");
			TEST_ENSURE_EQUALITY(expected, str, "read");
		}
	}

	return true;
}

static bool serialized_dtor() {
	constexpr size_t bytes_per_item = max_serialized_block_size() / 10;
	using T = debug_class<uint8_t, bytes_per_item>;

	int N = 100;

	TEST_ENSURE_EQUALITY(size_t(0), T::live_instances.load(), "live_instances");

	{
		serialized_file<T> f;
		f.open(tmp_file.path(), flags);
		{
			auto s = f.stream();
			for (int i = 0; i < N; i++) {
				T t;
				t.set_value(i);
				s.write(std::move(t));
			}

			const T & t = s.read_back();
			t.check_alive();

			TEST_ENSURE_EQUALITY(true, T::live_instances.load() > 0, "live_instances");
		}

		f.close();

		TEST_ENSURE_EQUALITY(size_t(0), T::live_instances.load(), "live_instances");
	}

	{
		serialized_file<T> f;
		f.open(tmp_file.path(), flags);
		auto s = f.stream();
		for (int i = 0; i < N; i++) {
			const T & t = s.read();
			TEST_ENSURE_EQUALITY(uint8_t(i), t.get_value(), "read");
		}
	}

	TEST_ENSURE_EQUALITY(size_t(0), T::live_instances.load(), "live_instances");

	{
		serialized_file<T> f;
		f.open(tmp_file.path(), flags);
		stream_position p1, p2;
		auto s = f.stream();
		auto s2 = f.stream();
		for (int i = 0; i < N; i++) {
			if (i == N / 3)
				p1 = s.get_position();
			if (i == N / 2)
				p2 = s.get_position();
			const T & t = s.read();
			TEST_ENSURE_EQUALITY(uint8_t(i), t.get_value(), "read");
		}

		s.set_position(p1);
		s2.set_position(p2);
		f.truncate(p2);
		for (int i = p1.m_logical_offset + p1.m_index; i < (int)(p2.m_logical_offset + p2.m_index); i++) {
			const T & t = s.read();
			TEST_ENSURE_EQUALITY(uint8_t(i), t.get_value(), "read");
		}
	}

	TEST_ENSURE_EQUALITY(size_t(0), T::live_instances.load(), "live_instances");

	return true;
}

static bool user_data() {
	const std::string user_data = "foobar";
	constexpr size_t size = 7;

	{
		file<int> f;
		f.open(tmp_file.path(), flags, 2 * size);
		TEST_ENSURE_EQUALITY(size_t(0), f.user_data_size(), "user_data_size");
		TEST_ENSURE_EQUALITY(2 * size, f.max_user_data_size(), "user_data_size");
	}
	{
		file<int> f;
		f.open(tmp_file.path(), flags);
		TEST_ENSURE_EQUALITY(size_t(0), f.user_data_size(), "user_data_size");
		TEST_ENSURE_EQUALITY(2 * size, f.max_user_data_size(), "user_data_size");

		f.write_user_data(user_data.c_str(), size);
		TEST_ENSURE_EQUALITY(size, f.user_data_size(), "user_data_size");

		f.write_user_data(user_data.c_str(), 1);
		TEST_ENSURE_EQUALITY(size, f.user_data_size(), "user_data_size");

		char user_data2[size];
		f.read_user_data(user_data2, size);
		TEST_ENSURE_EQUALITY(user_data, user_data2, "read_user_data");

		auto s = f.stream();
		s.write(105);
	}
	{
		file<int> f;
		f.open(tmp_file.path(), flags, 2 * size);
		TEST_ENSURE_EQUALITY(size, f.user_data_size(), "user_data_size");

		char user_data2[size];
		f.read_user_data(user_data2, size);
		TEST_ENSURE_EQUALITY(user_data, user_data2, "read_user_data");

		auto s = f.stream();
		TEST_ENSURE_EQUALITY(105, s.read(), "read");
	}

	return true;
}

static bool get_set_position() {
	file<int> f;
	f.open(tmp_file.path(), flags);
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
	TEST_ENSURE_EQUALITY(0, s.peek(), "peek (p2)");
	TEST_ENSURE_EQUALITY(9999, s.peek_back(), "peek_back (p2)");

	s.set_position(p1);
	TEST_ENSURE_EQUALITY(0, s.peek(), "peek (p1)");

	s.set_position(p3);
	TEST_ENSURE_EQUALITY(9999, s.peek_back(), "peek_back (p3)");

	return true;
}

static bool truncate() {
	file<int> f;
	f.open(tmp_file.path(), flags);

	{
		auto s = f.stream();
		auto b = s.logical_block_size();

		int ctr = 0;

		for (; ctr < 10 * (int)b + 2; ctr++)
			s.write(ctr);

		// Not on block boundary
		auto p1 = s.get_position();

		for (; ctr < 20 * (int)b; ctr++)
			s.write(ctr);

		// On block boundary
		auto p2 = s.get_position();

		for (; ctr < 30 * (int)b; ctr++)
			s.write(ctr);

		s.seek(0, whence::set);
		f.truncate(p2);

		assert(f.size() == 20 * b);

		for (int i = 0; i < 20 * (int)b; i++)
			TEST_ENSURE_EQUALITY(i, s.read(), "read");

		s.write(123546);

		s.seek(0, whence::set);
		f.truncate(p1);

		assert(f.size() == 10 * b + 2);

		for (int i = 0; i < 10 * (int)b + 2; i++)
			TEST_ENSURE_EQUALITY(i, s.read(), "read");

		s.write(789);
		auto p3 = s.get_position();
		f.truncate(p3);
		s.write(987);

		TEST_ENSURE_EQUALITY(987, s.read_back(), "read_back");
		TEST_ENSURE_EQUALITY(789, s.read_back(), "read_back");
	}

	return true;
}

static bool open_truncate_close() {
	file<uint8_t> f;
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		s.write(11);
		f.truncate(0);
	}
	f.close();

	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		for (block_size_t i = 0; i < block_size(); i++)
			s.write((uint8_t) i);

		f.truncate(s.get_position());
	}
	f.close();

	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		for (block_size_t i = 0; i < block_size(); i++)
			TEST_ENSURE_EQUALITY((uint8_t)i, s.read(), "read");
	}

	return true;
}

static bool file_stream_test() {
	file_stream<int> f;
	f.open(tmp_file.path(), flags);

	for (int i = 0; i < 1000; i++)
		f.write(i);

	f.close();
	f.open(tmp_file.path(), flags);

	for (int i = 0; i < 1000; i++)
		TEST_ENSURE_EQUALITY(i, f.read(), "read");

	return true;
}

static bool direct_file() {
	file<block_size_t> f;
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		auto b = s.logical_block_size();
		for (block_size_t i = 0; i < 10 * b; i++)
			s.write(i);

		s.seek(5 * b + 9, whence::set);
		TEST_ENSURE_EQUALITY(5 * b + 9, s.read(), "read");

		s.write(987);

		f.truncate(8 * b + 123);
		s.seek(0, whence::end);
		TEST_ENSURE_EQUALITY(8 * b + 122, s.read_back(), "read_back");
	}
	f.close();
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		auto b = s.logical_block_size();
		for (block_size_t i = 0; i < 8 * b + 123; i++)
			TEST_ENSURE_EQUALITY((i == 5 * b + 10)? 987: i, s.read(), "read");
	}

	return true;
}

static bool move_file_object() {
	file<int> f;
	f.open(tmp_file.path(), flags);

	auto s = f.stream();
	s.write(1);

	auto f2 = std::move(f);
	s.write(2);

	auto s2 = f2.stream();
	s2.seek(0, whence::end);
	s2.write(3);

	auto s3 = std::move(s);
	TEST_ENSURE_EQUALITY(3, s3.read(), "read");
	s3.write(4);

	f = std::move(f2);
	s3.write(5);
	s = std::move(s3);
	s.write(6);

	TEST_ENSURE_EQUALITY(size_t(6), f.size(), "size");

	return true;
}

class non_serializable {
};
template <typename D>
void serialize(D & dst, const non_serializable & o) = delete;

static bool test_non_serializable() {
	file<non_serializable> f;
	f.open(tmp_file.path(), flags);
	auto s = f.stream();
	s.write(non_serializable());
	s.read_back();

	return true;
}

static bool write_chunked() {
	size_t buf_size = block_size() / sizeof(int) * 2;
	int * buf = new int[buf_size];
	std::iota(buf, buf + buf_size, 0);

	file<int> f;
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();

		s.write(buf, buf_size);
		s.write(1337);
		s.write(buf, buf_size);
		s.write(4880);
		s.write(buf, buf_size / 10);

		TEST_ENSURE_EQUALITY(2 * buf_size + buf_size / 10 + 2, f.size(), "size");
	}
	f.close();
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		for (size_t i = 0; i < buf_size; i++) TEST_ENSURE_EQUALITY(buf[i], s.read(), "read");
		TEST_ENSURE_EQUALITY(1337, s.read(), "read");
		for (size_t i = 0; i < buf_size; i++) TEST_ENSURE_EQUALITY(buf[i], s.read(), "read");
		TEST_ENSURE_EQUALITY(4880, s.read(), "read");
		for (size_t i = 0; i < buf_size / 10; i++) TEST_ENSURE_EQUALITY(buf[i], s.read(), "read");
		TEST_ENSURE_EQUALITY(false, s.can_read(), "can_read");
	}

	delete[] buf;

	return true;
}

static bool test_read_only() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		s.write(1);
	}
	f.close();
	f.open(tmp_file.path(), tpie::open::read_only | flags);
	{
		auto s = f.stream();
		TEST_ENSURE_EQUALITY(1, s.read(), "read");
	}

	return true;
}

static bool direct_file2() {
	file<int> f;
	f.open(tmp_file.path(), flags);
	f.close();
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		for (int i = 0; i < (int)s.logical_block_size(); i++)
			s.write(i);
	}
	f.close();
	f.open(tmp_file.path(), flags);
	{
		auto s = f.stream();
		for (int i = 0; i < (int)s.logical_block_size(); i++)
			TEST_ENSURE_EQUALITY(i, s.read(), "read");
	}

	return true;
}

static bool read_seq() {
	int b;
	{
		file<int> f;
		f.open(tmp_file.path(), flags);
		{
			auto s = f.stream();
			b = (int) s.logical_block_size();
			for (int i = 0; i < 10 * b; i++)
				s.write(i);
		}
	}
	{
		file<int> f;
		f.open(tmp_file.path(), flags);
		{
			auto s = f.stream();
			for (int i = 0; i < 10 * b; i++)
				TEST_ENSURE_EQUALITY(i, s.read(), "read");
		}
	}
	{
		file<int> f;
		f.open(tmp_file.path(), flags);
		{
			auto s = f.stream();
			s.seek(0, whence::end);
			for (int i = 10 * b - 1; i >= 0; i--)
				TEST_ENSURE_EQUALITY(i, s.read_back(), "read");
		}
	}

	return true;
}

};


template <bool compression, bool readahead>
tpie::tests & add_tests(const std::string & suffix, tpie::tests & t) {
	constexpr tpie::open::type flags = (compression? tpie::open::compression_all: tpie::open::defaults) |
								 (readahead? tpie::open::readahead_enabled: tpie::open::defaults);
	typedef tests<flags> T;

	auto test_wrapper = [](auto f){
		return [f]() {
			bool res = f();
			if (res) {
				if (!check_file(tmp_file.path().c_str()))
					res = false;
			}
			return res;
		};
	};

	auto add_test = [&](auto f, const std::string & name) {
		t.test(test_wrapper(f), name + suffix);
	};

	add_test(T::flush_test, "flush");
	add_test(T::size_test, "size");
	add_test(T::write_seek_write_read, "write_seek_write_read");
	add_test(T::open_close, "open_close");
	add_test(T::seek_start_seek_end, "seek_start_seek_end");
	add_test(T::write_end, "write_end");
	add_test(T::open_close_dead_stream, "open_close_dead_stream");
	add_test(T::peek_test, "peek");
	add_test(T::peek_back_test, "peek_back");
	add_test(T::read_back_test, "read_back");
	add_test(T::serialized_string, "serialized_string");
	add_test(T::serialized_dtor, "serialized_dtor");
	add_test(T::user_data, "user_data");
	add_test(T::get_set_position, "get_set_position");
	add_test(T::truncate, "truncate");
	add_test(T::open_truncate_close, "open_truncate_close");
	add_test(T::file_stream_test, "file_stream");
	add_test(T::move_file_object, "move_file_object");
	add_test(T::test_non_serializable, "non_serializable");
	add_test(T::write_chunked, "write_chunked");
	add_test(T::test_read_only, "read_only");
	add_test(T::read_seq, "read_seq");

	if (!compression) {
		add_test(T::direct_file, "direct_file");
		add_test(T::direct_file2, "direct_file2");
	}

	return t;
}

int main(int argc, char ** argv) {
	tpie::tests t(argc, argv);

	t.setup(setup);

	add_tests<false, false>("_u", t);
	add_tests<true, false>("", t);
	add_tests<false, true>("_u_r", t);
	add_tests<true, true>("_r", t);

	return t;
}
