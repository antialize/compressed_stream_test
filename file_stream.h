// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <cstdint>
#include <string>
#include <string.h>
#include <cassert>

#include <tpie/serialization2.h>
using namespace tpie;

// Declare all types
class block_base;
class file_impl;
class file_base_base;
class stream_impl;
class stream_base_base;
class stream_position;

typedef uint64_t block_idx_t; // Used for numbering blocks from 0 to n-1
typedef uint64_t file_size_t; // Used for absolute physical and logical sizes and offsets from the beginning of the file
typedef uint32_t block_size_t; // Used for relative physical and logical sizes and offsets from the start of a block

template <typename T, bool serialized>
class file_base;

template <typename T, bool serialized>
class stream_base;

// Constexpr methods
constexpr block_size_t block_size() {return 1024;}

// TODO: Tweak this or calculate another way
constexpr block_size_t max_serialized_block_size() {return block_size() * 64;}

// Some free standing methods
void file_stream_init(size_t threads);
void file_stream_term();

// Give implementations of needed types
class block_base {
public:
	file_size_t m_logical_offset;
	block_size_t m_logical_size;
	block_size_t m_maximal_logical_size;
	// Undefined for non-serialized blocks
	block_size_t m_serialized_size;
	bool m_dirty;
	char m_data[block_size()];
};

struct stream_position {
	block_idx_t m_block;
	block_size_t m_index;
	file_size_t m_logical_offset;
	file_size_t m_physical_offset;
};

namespace open_flags {
enum open_flags {
	default_flags = 0,
	read_only = 1 << 0,
	truncate = 1 << 1,
	no_compress = 1 << 2,

	// Alias for other flags
	read_write = default_flags,
	write_only = read_write,
};
}

class file_base_base {
public:
	friend class file_impl;
	friend class stream_base_base;
	friend void process_run();

	file_base_base(const file_base_base &) = delete;
	file_base_base(file_base_base &&);

	// TODO more magic open methods here
	void open(const std::string & path, open_flags::open_flags flags = open_flags::default_flags, size_t max_user_data_size = 0);
	void open(const std::string & path, std::underlying_type<open_flags::open_flags>::type flags, size_t max_user_data_size = 0) {
		open(path, static_cast<open_flags::open_flags>(flags), max_user_data_size);
	}
	void close();

	bool is_open() const noexcept;
	
	bool is_readable() const noexcept;
	bool is_writable() const noexcept;

	size_t user_data_size() const noexcept;
	size_t max_user_data_size() const noexcept;
	void read_user_data(void * data, size_t count);
	void write_user_data (const void *data, size_t count);

	const std::string & path() const noexcept;

	void truncate(file_size_t offset);
	void truncate(stream_position pos);

	template <typename TT>
	void read_user_data(TT & data) {
		//if (sizeof(TT) != user_data_size()) throw io_exception("Wrong user data size");
		read_user_data(&data, sizeof(TT));
	}

	template <typename TT>
	void write_user_data(const TT & data) {
		//if (sizeof(TT) > max_user_data_size()) throw io_exception("Wrong user data size");
		write_user_data(&data, sizeof(TT));
	}

	file_size_t size() const noexcept {
		return m_last_block->m_logical_offset + m_last_block->m_logical_size;
	}

protected:
	file_base_base(bool serialized, block_size_t item_size);
	virtual ~file_base_base();
private:
	virtual void do_serialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) = 0;
	virtual void do_unserialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) = 0;
	virtual void do_destruct(char * data, block_size_t size) = 0;

	file_impl * m_impl;
	block_base * m_last_block;
};

enum class whence {set, cur, end};

class stream_base_base {
public:
	stream_base_base(const stream_base_base &) = delete;
	stream_base_base(stream_base_base &&);
	stream_base_base & operator=(stream_base_base &&);
	~stream_base_base();
	
	bool can_read() const noexcept {
		return offset() < m_file_base->size();
	}

	bool can_read_back() const noexcept {
		return offset() != 0;
	}

	void skip() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		++m_cur_index;
	}
	
	void skip_back() {
		if (m_cur_index == 0) prev_block();
		--m_cur_index;
	}

	void seek(file_size_t offset, whence w = whence::set);
	
	file_size_t offset() const noexcept {
		return m_block->m_logical_offset + m_cur_index;
	}

#ifndef NDEBUG
	block_base * get_last_block() {
		return m_file_base->m_last_block;
	}
#endif

	stream_position get_position();

	void set_position(stream_position p);
	
	friend class stream_impl;
protected:
	void next_block();
	void prev_block();
	stream_base_base(file_base_base * impl);

	block_base * m_block;
	file_base_base * m_file_base;
	stream_impl * m_impl;
	block_size_t m_cur_index;
};

template <typename T, bool serialized>
class stream_base: public stream_base_base {
public:
	constexpr block_size_t logical_block_size() const {return block_size() / sizeof(T);}

protected:
	stream_base(file_base_base * imp): stream_base_base(imp) {}
	
	friend class file_base<T, serialized>;
public:
	stream_base(stream_base &&) = default;
	stream_base & operator=(stream_base &&) = default;
	
	const T & read() {
		assert(m_file_base->is_open() && m_file_base->is_readable());
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}
	
	const T & peek() {
		assert(m_file_base->is_open() && m_file_base->is_readable());
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index];
	}

	const T & read_back() {
		assert(m_file_base->is_open() && m_file_base->is_readable());
		if (m_cur_index == 0) prev_block();
		return reinterpret_cast<const T *>(m_block->m_data)[--m_cur_index];
	}

	const T & peek_back() {
		assert(m_file_base->is_open() && m_file_base->is_readable());
		// If prev_block is called, the index is set to the logical size
		// so even if we change the block, we will still use the correct block
		// on reading forward
		if (m_cur_index == 0) prev_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index - 1];
	}

	void write(T item) {
		assert(m_file_base->is_open() && m_file_base->is_writable());

		if (m_cur_index == m_block->m_maximal_logical_size) next_block();

		if (serialized) {
			block_size_t serialized_size = get_serialized_size(item);

			if (m_block->m_serialized_size + serialized_size > max_serialized_block_size()) {
				m_block->m_maximal_logical_size = m_cur_index;
				next_block();
			}

			m_block->m_serialized_size += serialized_size;
		}

#ifndef NDEBUG
		assert(get_last_block() == m_block);
		assert(m_block->m_logical_size == m_cur_index);
#endif

		reinterpret_cast<T*>(m_block->m_data)[m_cur_index++] = std::move(item);
		m_block->m_logical_size = std::max(m_block->m_logical_size, m_cur_index); //Hopefully this is a cmove
		m_block->m_dirty = true;
	}

private:
	block_size_t get_serialized_size(const T & item) {
		struct Counter {
			block_size_t s = 0;
			void write(const char * data, size_t size) {
				s += size;
			}
		};
		Counter c;
		serialize(c, item);
		return c.s;
	}
};


template <typename T, bool serialized>
class file_base final: public file_base_base {
public:
	stream_base<T, serialized> stream() {return stream_base<T, serialized>(this);}

	file_base(): file_base_base(serialized, sizeof(T)) {}

	// We can't close the file in file_base_base's dtor
	// as the job thread might need to serialize some items before we close the file.
	~file_base() override {
		if (is_open())
			close();
	}

protected:
	void do_serialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) override {
		assert(serialized);
		if (!serialized) return;

		struct W {
			char * o;
			block_size_t s = 0;
			void write(const char * data, size_t size) {
				if(o) memcpy(o + s, data, size);
				s += size;
			}
			W(char * o) : o(o) {}
		};
		W w(out);
		for (size_t i = 0; i < in_items; i++)
			serialize(w, reinterpret_cast<const T*>(in)[i]);
		*out_size = w.s;
	}

	void do_unserialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) override {
		assert(serialized);
		if (!serialized) return;

		struct R {
			const char * i;
			block_size_t s = 0;
			void read(char * data, size_t size) {
				memcpy(data, i + s, size);
				s += size;
			}
			R(const char * i) : i(i) {}
		};
		R r(in);
		for (size_t i = 0; i < in_items; i++) {
			T * o = new(out + i * sizeof(T)) T;
			unserialize(r, *o);
		}
		*out_size = sizeof(T) * in_items;
	}
	void do_destruct(char * data, block_size_t size) override {
		assert(serialized);
		if (!serialized) return;

		for (size_t i=0; i < size; ++i)
			reinterpret_cast<T *>(data)[i].~T();
	}
};

template <typename T, bool serialized>
class file_stream_base {
public:
	void close() {return m_file.close();}
	
	bool can_read() {return m_stream.can_read();}
	void skip() {m_stream.skip();}
	
	const T & read() {return m_stream.read();}
	const T & peek() {return m_stream.peek();}
	void write(const T & item) {m_stream.write(item);}
private:
	file_base<T, serialized> m_file;
	stream_base<T, serialized> m_stream;
};

// Actual types

template <typename T>
using file = file_base<T, false>;

template <typename T>
using serialized_file = file_base<T, true>;

template <typename T>
using stream = stream_base<T, false>;

template <typename T>
using serialized_stream = stream_base<T, true>;

template <typename T>
using file_stream = file_stream_base<T, false>;

template <typename T>
using serialized_file_stream = file_stream_base<T, true>;
