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

class file_base_base {
public:
	friend class file_imlp;
	friend class stream_base_base;
	
	file_base_base();
	file_base_base(const file_base_base &) = delete;
	file_base_base(file_base_base &&);
	~file_base_base();
	
	// TODO more magic open methods here
	void open(const std::string & path);

	void close();
	bool is_readable () const noexcept;
	bool is_writable () const noexcept;

	void read_user_data(void * data, size_t count);
	size_t user_data_size();
	size_t max_user_data_size();
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
	
private:
	virtual void do_serialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) = 0;
	virtual void do_unserialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) = 0;
	virtual void do_destruct(char * data, uint32_t size) = 0;
	
	file_impl * m_impl;
	block_base * m_last_block;
	uint64_t m_logical_size;	
};

class stream_base_base {
public:
	stream_base_base(const stream_base_base &) = delete;
	stream_base_base(stream_base_base &&);
	~stream_base_base();
	
	bool can_read() {return false;}
	
	void skip() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		++m_cur_index;
	}
	
	void seek(uint64_t offset);


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
	
	const T & read() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}
	
	const T & peek() {
		if (m_cur_index == m_block->m_logical_size) next_block();
		return reinterpret_cast<const T *>(m_block->m_data)[m_cur_index++];
	}

	void write(T item) {
		//TODO handle serialized write here
		if (m_cur_index == logical_block_size()) next_block();
		m_block->m_data[m_cur_index++] = std::move(item);
		m_block->m_logical_size = std::max(m_block->m_logical_size, m_cur_index); //Hopefully this is a cmove
		m_block->m_dirty = true;
	}
};


template <typename T, bool serialized>
class file_base final: public file_base_base {
public:
	stream_base<T, serialized> stream() {return stream_base<T, serialized>(this);}
protected:
	void do_serialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) override {}
	void do_unserialize(const char * in, uint32_t in_size, char * out, uint32_t out_size) override {}
	void do_destruct(char * data, uint32_t size) override {}
};

template <typename T>
class file_base<T, true> final: public file_base_base {
public:
	stream_base<T, true> stream() {return stream_base<T, true>(this);}
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
		for (size_t i=0; i < size; ++i)
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
