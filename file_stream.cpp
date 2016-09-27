#include <file_stream.h>

class file_impl {

};

class stream_impl {

};


file_base_base::file_base_base() {};
file_base_base::~file_base_base() {};

stream_base_base::~stream_base_base() {};

stream_base_base::stream_base_base(stream_impl * impl) {}


stream_impl * produce_stream_impl(file_base_base *) {
  return nullptr;
}

file_impl * produce_fiile_impl() {
  return nullptr;
}
