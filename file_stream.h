// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :

///////////////////////////////////////////////////////////////////////////////
/// \file file_stream.h  File streams for POD and serializable items
///////////////////////////////////////////////////////////////////////////////

#pragma once
#include <memory>
#include <cstdint>
#include <string>
#include <string.h>
#include <cassert>

#include <tpie/serialization2.h>
#include <defaults.h>
using namespace tpie;

// Declare all types
class block_base;
class file_impl;
class file_base_base;
class stream_impl;
class stream_base_base;
struct stream_position;

typedef uint64_t block_idx_t; // Used for numbering blocks from 0 to n-1
typedef uint64_t file_size_t; // Used for absolute physical and logical sizes and offsets from the beginning of the file
typedef uint32_t block_size_t; // Used for relative physical and logical sizes and offsets from the start of a block

template <typename T, bool serialized>
class file_base;

template <typename T, bool serialized>
class stream_base;

#ifndef FILE_STREAM_BLOCK_SIZE
// Not used
#define FILE_STREAM_BLOCK_SIZE 1024
#endif

#define unused(x) do { (void)(x); } while(0)

// Constexpr methods
constexpr block_size_t block_size() {return FILE_STREAM_BLOCK_SIZE;}

constexpr block_size_t max_serialized_block_size() {return block_size();}

// Some free standing methods
void file_stream_init(size_t threads);
void file_stream_term();

struct block_header {
	file_size_t logical_offset;
	block_size_t physical_size;
	block_size_t logical_size;
};

// Give implementations of needed types
class block_base {
public:
	file_size_t m_logical_offset;
	block_size_t m_logical_size;
	block_size_t m_maximal_logical_size;
	// Undefined for non-serialized blocks
	block_size_t m_serialized_size;
	bool m_dirty;

	// We make room for two block_header before and after
	// the actual data
	char _buffer[block_size() + 4 * sizeof(block_header)];
	char * m_data = _buffer + 2 * sizeof(block_header);
};

struct stream_position {
	block_idx_t m_block;
	block_size_t m_index;
	file_size_t m_logical_offset;
	file_size_t m_physical_offset;

	bool operator==(const stream_position & o) const {
		return std::tie(m_block, m_index) == std::tie(o.m_block, o.m_index);
	}

	bool operator<(const stream_position & o) const {
		return std::tie(m_block, m_index) < std::tie(o.m_block, o.m_index);
	}
};

namespace open_flags {
enum open_flags {
	default_flags = 0,
	read_only = 1 << 0,
	truncate = 1 << 1,
	no_compress = 1 << 2,
	no_readahead = 1 << 3,

	// Alias for other flags
	read_write = default_flags,
	write_only = read_write,
};
#define T std::underlying_type<open_flags>::type
#define OP_IMPL(op) \
    inline open_flags operator op(open_flags f1, open_flags f2) { return static_cast<open_flags>(T(f1) op T(f2)); } \
    inline open_flags & operator op##=(open_flags & f1, open_flags f2) { return f1 = f1 op f2; }
OP_IMPL(|)
OP_IMPL(&)
OP_IMPL(^)
#undef OP_IMPL
inline open_flags operator~(open_flags f) { return static_cast<open_flags>(~T(f)); }
#undef T
}

class file_base_base {
public:
	friend class file_impl;
	friend class stream_base_base;

	file_base_base(const file_base_base &) = delete;
	file_base_base & operator=(const file_base_base &) = delete;
	file_base_base(file_base_base &&);
	file_base_base & operator=(file_base_base &&);

	// TODO more magic open methods here
	void open(const std::string & path, open_flags::open_flags flags = open_flags::default_flags, size_t max_user_data_size = 0);
	void close();

	bool is_open() const noexcept;
	
	bool is_readable() const noexcept;
	bool is_writable() const noexcept;

	bool direct() const noexcept;

	size_t user_data_size() const noexcept;
	size_t max_user_data_size() const noexcept;
	void read_user_data(void * data, size_t count);
	void write_user_data(const void *data, size_t count);

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

	file_size_t size() const noexcept;

protected:
	file_base_base(bool serialized, block_size_t item_size);
	virtual ~file_base_base();
private:
	virtual void do_serialize(const char * /*in*/, block_size_t /*in_items*/, char * /*out*/, block_size_t * /*out_size*/) {}
	virtual void do_unserialize(const char * /*in*/, block_size_t /*in_items*/, char * /*out*/, block_size_t * /*out_size*/) {}
	virtual void do_destruct(char * /*data*/, block_size_t /*size*/) {}

	void impl_changed();

	file_impl * m_impl;
};

enum class whence {set, cur, end};

class stream_base_base {
public:
	stream_base_base(const stream_base_base &) = delete;
	stream_base_base & operator=(const stream_base_base &) = delete;
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
		assert(m_file_base->is_open() && can_read());
		if (m_cur_index == m_block->m_logical_size) next_block();
		++m_cur_index;
	}
	
	void skip_back() {
		assert(m_file_base->is_open() && can_read_back());
		if (m_cur_index == 0) prev_block();
		--m_cur_index;
	}

	void seek(file_size_t offset, whence w = whence::set);
	
	file_size_t offset() const noexcept {
		return m_block->m_logical_offset + m_cur_index;
	}

#ifndef NDEBUG
	block_base * get_last_block();
#endif

	stream_position get_position();

	void set_position(stream_position p);
	
	friend class stream_impl;
	friend class file_base_base;
protected:
	void serialize_block_overflow(block_size_t serialized_size);
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

	friend class file_base<T, serialized>;
protected:
	stream_base(file_base_base * imp): stream_base_base(imp) {}

public:
	const T & read() {
		assert(m_file_base->is_open() && m_file_base->is_readable() && can_read());
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}

	const T & peek() {
		assert(m_file_base->is_open() && m_file_base->is_readable() && can_read());
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index];
	}

	const T & read_back() {
		assert(m_file_base->is_open() && m_file_base->is_readable() && can_read_back());
		if (m_cur_index == 0) prev_block();
		return reinterpret_cast<const T *>(m_block->m_data)[--m_cur_index];
	}

	const T & peek_back() {
		assert(m_file_base->is_open() && m_file_base->is_readable() && can_read_back());
		// If prev_block is called, the index is set to the logical size
		// so even if we change the block, we will still use the correct block
		// on reading forward
		if (m_cur_index == 0) prev_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index - 1];
	}
	
	void write(T item) {
		assert(m_file_base->is_open() && m_file_base->is_writable());
		if (m_cur_index == m_block->m_maximal_logical_size) next_block();

		if constexpr (serialized) {
			struct Counter {
				block_size_t s = 0;
				void write(const char *, size_t size) {
					s += size;
				}
			};
			Counter c;
			serialize(c, item);
			if (this->m_block->m_serialized_size + c.s > max_serialized_block_size()) serialize_block_overflow(c.s);
			this->m_block->m_serialized_size += c.s;

		}

		assert(m_file_base->direct() || get_last_block() == m_block);
		assert(m_file_base->direct() || m_block->m_logical_size == m_cur_index);

		new (&reinterpret_cast<T*>(m_block->m_data)[m_cur_index++]) T(std::move(item));
		m_block->m_logical_size = std::max(m_block->m_logical_size, m_cur_index); //Hopefully this is a cmove
		m_block->m_dirty = true;
	}

	void write(T * items, size_t n) {
		if constexpr (serialized) {
			for (size_t i = 0; i < n; i++)
				write(items[i]);
		} else {
			assert(this->m_file_base->is_open() && this->m_file_base->is_writable());
			size_t written = 0;
			while (written < n) {
				if (this->m_cur_index == this->m_block->m_maximal_logical_size) this->next_block();
				block_size_t remaining = static_cast<block_size_t>(
					std::min<size_t>(this->m_block->m_maximal_logical_size - this->m_cur_index, n - written));
				memcpy(this->m_block->m_data + this->m_cur_index * sizeof(T), items + written, remaining * sizeof(T));
				this->m_cur_index += remaining;
				this->m_block->m_logical_size = std::max(this->m_block->m_logical_size, this->m_cur_index); //Hopefully this is a cmove
				this->m_block->m_dirty = true;
				written += remaining;
			}
		}
	}
};

template <typename T, bool serialized>
class file_base final: public file_base_base {
	static_assert(sizeof(T) <= block_size(), "Size of item must be lower than the block size");
	static_assert(serialized || std::is_trivially_copyable<T>::value, "Non-serialized stream must have trivially copyable items");

public:
	stream_base<T, serialized> stream() {return stream_base<T, serialized>(this);}
	file_base(): file_base_base(serialized, sizeof(T)) {}
	file_base(const file_base &) = delete;
	file_base & operator=(const file_base &) = delete;
	file_base(file_base &&) = default;
	file_base & operator=(file_base &&) = default;

	// We can't close the file in file_base_base's dtor
	// as the job thread might need to serialize some items before we close the file.
	~file_base() override {
		if (is_open())
			close();
	}

	void do_serialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) override {
		if constexpr (serialized) {
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
		} else {
			unused(in);
			unused(in_items);
			unused(out);
			unused(out_size);
		}
	}

	void do_unserialize(const char * in, block_size_t in_items, char * out, block_size_t * out_size) override {
		if constexpr (serialized) {
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
		} else {
			unused(in);
			unused(in_items);
			unused(out);
			unused(out_size);
		}
	}
	
	void do_destruct(char * data, block_size_t size) override {
		if constexpr (serialized) {
			for (size_t i=0; i < size; ++i)
				reinterpret_cast<T *>(data)[i].~T();
		} else {
			unused(data);
			unused(size);			
		}
	}
};

template <typename T, bool serialized>
class file_stream_base {
public:
	file_stream_base() = default;
	file_stream_base(const file_stream_base &) = delete;
	file_stream_base & operator=(const file_stream_base &) = delete;
	file_stream_base(file_stream_base &&) = default;
	file_stream_base & operator=(file_stream_base &&) = default;

	// == file_base_base functions ==
	void open(const std::string & path, open_flags::open_flags flags = open_flags::default_flags, size_t max_user_data_size = 0) {
		m_file.open(path, flags, max_user_data_size);
		m_stream = std::unique_ptr<stream_base<T, serialized>>(new stream_base<T, serialized>(m_file.stream()));
	}

	void close() {
		m_stream.reset(nullptr);
		m_file.close();
	}

	bool is_open() const noexcept {return m_file.is_open();}
	bool is_readable() const noexcept {return m_file.is_readable();}
	bool is_writable() const noexcept {return m_file.is_writable();}
	bool direct() const noexcept {return m_file.direct();}
	size_t user_data_size() const noexcept {return m_file.user_data_size();}
	size_t max_user_data_size() const noexcept {return m_file.max_user_data_size();}
	void read_user_data(void * data, size_t count) {m_file.read_user_data(data, count);}
	void write_user_data(const void *data, size_t count) {m_file.write_user_data(data, count);}
	const std::string & path() const noexcept {return m_file.path();}
	void truncate(file_size_t offset) {return m_file.truncate(offset);}
	void truncate(stream_position pos) {return m_file.truncate(pos);}
	template <typename TT>
	void read_user_data(TT & data) {m_file.read_user_data(data);}
	template <typename TT>
	void write_user_data(const TT & data) {m_file.write_user_data(data);}
	file_size_t size() const noexcept {return m_file.size();}

	// == stream_base_base functions ==
	bool can_read() const noexcept {return m_stream->can_read();}
	bool can_read_back() const noexcept {return m_stream->can_read_back();}
	void skip() {m_stream->skip();}
	void skip_back() {m_stream->skip_back();}
	void seek(file_size_t offset, whence w = whence::set) {m_stream->seek(offset, w);}
	file_size_t offset() const noexcept {return m_stream->offset();}
	stream_position get_position() {return m_stream->get_position();}
	void set_position(stream_position p) {m_stream->set_position(p);}

	// == stream_base functions ==
	const T & read() {return m_stream->read();}
	const T & peek() {return m_stream->peek();}
	const T & read_back() {return m_stream->read_back();}
	const T & peek_back() {return m_stream->peek_back();}
	void write(T item) {m_stream->write(item);}
	void write(T * items, size_t n) {m_stream->write(items, n);}

private:
	file_base<T, serialized> m_file;
	std::unique_ptr<stream_base<T, serialized>> m_stream;
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
