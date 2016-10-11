// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <cstdint>
#include <string>
#include <string.h>

// Declare all types
class block_base;
class file_impl;
class file_base_base;
class stream_impl;
class stream_base_base;
class stream_position;

typedef uint64_t block_idx_t;
typedef uint64_t block_offset_t;
typedef uint32_t block_size_t;

template <typename T, bool serialized>
class file_base;

template <typename T, bool serialized>
class stream_base;

// Constexpr methods
constexpr uint32_t block_size() {return 1024;}

// Some free standing methods
void file_stream_init(int threads); 
void file_stream_term();

// Give implementations of needed types
class block_base {
public:
	uint64_t m_logical_offset;
	uint32_t m_logical_size;
	uint32_t m_maximal_logical_size;
	bool m_dirty;
	char m_data[block_size()];
};

struct stream_position {
	block_idx_t m_block;
	block_size_t m_index;
	block_offset_t m_logical_offset;
	block_offset_t m_physical_offset;
};

class file_base_base {
public:
	friend class file_impl;
	friend class stream_base_base;
	
	file_base_base(const file_base_base &) = delete;
	file_base_base(file_base_base &&);
	~file_base_base();
	
	// TODO more magic open methods here
	void open(const std::string & path);
	void close();

	bool is_open() const noexcept;
	
	bool is_readable() const noexcept;
	bool is_writable() const noexcept;

	void read_user_data(void * data, size_t count);
	size_t user_data_size() const noexcept;
	size_t max_user_data_size() const noexcept;
	void write_user_data (const void *data, size_t count);
	const std::string & path() const noexcept;
	
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

	uint64_t size() const noexcept {
		if (m_last_block == nullptr) return m_logical_size;
		return m_last_block->m_logical_offset + m_last_block->m_logical_size;
	}

protected:
	file_base_base(bool serialized, uint32_t item_size);
private:
	virtual void do_serialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) = 0;
	virtual void do_unserialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) = 0;
	virtual void do_destruct(char * data, uint32_t size) = 0;
	
	file_impl * m_impl;
	block_base * m_last_block;
	uint64_t m_logical_size;	
};

enum class whence {set, cur, end};

class stream_base_base {
public:
	stream_base_base(const stream_base_base &) = delete;
	stream_base_base(stream_base_base &&);
	stream_base_base & operator=(stream_base_base &&);
	~stream_base_base();
	
	bool can_read() {
		#warning "Implement can_read"
		return false;
	}

	bool can_read_back() const noexcept {
		return offset() != 0;
	}

	void skip() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		++m_cur_index;
	}
	
	void skip_back() {
		#warning "Implement skip back"
	}

	void seek(uint64_t offset, whence w = whence::set);
	
	uint64_t offset() const noexcept {
		return m_block->m_logical_offset + m_cur_index;
	}

	void truncate(uint64_t offset);
	void truncate(stream_position pos);	
	
	stream_position get_position();

	void set_position(stream_position);
	
	friend class stream_impl;
protected:
	void next_block();
	stream_base_base(file_base_base * impl);

	block_base * m_block;
	stream_impl * m_impl;
	uint32_t m_cur_index;
};

template <typename T, bool serialized>
class stream_base: public stream_base_base {
protected:
	constexpr uint32_t logical_block_size() {return block_size() / sizeof(T);}
	stream_base(file_base_base * imp): stream_base_base(imp) {}
	
	friend class file_base<T, serialized>;
public:
	stream_base(stream_base &&) = default;
	stream_base & operator=(stream_base &&) = default;
	
	const T & read() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}
	
	const T & peek() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}

	const T & read_back() {
		#warning "Implement read_back"
	}

	const T & peak_back() {
		#warning "Implement peak_back"
	}
	
	void write(T item) {
		//TODO handle serialized write here
		if (m_cur_index == m_block->m_maximal_logical_size) next_block();
		reinterpret_cast<T*>(m_block->m_data)[m_cur_index++] = std::move(item);
		m_block->m_logical_size = std::max(m_block->m_logical_size, m_cur_index); //Hopefully this is a cmove
		m_block->m_dirty = true;
	}
};


template <typename T, bool serialized>
class file_base final: public file_base_base {
public:
	stream_base<T, serialized> stream() {return stream_base<T, serialized>(this);}

	file_base(): file_base_base(serialized, sizeof(T)) {}
	
protected:
	void do_serialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) override {}
	void do_unserialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) override {}
	void do_destruct(char * data, uint32_t size) override {}
};

template <typename T>
class file_base<T, true> final: public file_base_base {
public:
	stream_base<T, true> stream() {return stream_base<T, true>(this);}

	file_base(): file_base_base(true, sizeof(T)) {}
protected:
	virtual void do_serialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) {
		struct W {
			char * o;
			void write(const char * data, size_t size) {
				memcpy(o, data, size);
				o += size;
			}
			W(char * o): o(o) {}
		};
		W w(out);
		for (size_t i=0; i < in_size; ++i)
			serialize(w, static_cast<T*>(in)[i]);
	}
	
	virtual void do_unserialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) {
		struct R {
			const char * i;
			void read(char * data, size_t size) {
				memcpy(i, data, size);
				i += size;
			}
			R(const char *i): i(i) {}
		};
		R r(in);
		for (size_t i=0; i < out_size; ++i)
			unserialize(r, static_cast<T *>(out)[i]); //TODO placement new data here
	}

	
	virtual void do_destruct(char * data, uint32_t size) {
		for (size_t i=0; i < size; ++i)
			static_cast<T *>(data)[i].~T();
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
using serialized_straem = stream_base<T, true>;

template <typename T>
using file_stream = file_stream_base<T, false>;

template <typename T>
using serialized_file_stream = file_stream_base<T, true>;
