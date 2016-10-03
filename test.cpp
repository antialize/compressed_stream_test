#include <file_stream.h>
#include <cassert>
#include <iostream>
#include <file_stream_impl.h>

int main(int argc, char ** argv) {
  file_stream_init(4);
  
  file<int> f;
  f.open("/dev/shm/hello.tst");
  stream<int> s=f.stream();

  for (int i=0; i < 10000; ++i) {
    s.write(i);
  }

  s.seek(0);
  
  for (int i=0; i < 10000; ++i) {
    int v = s.read();
    log_info() << "READ " << v << std::endl;
    assert(v == i);
  }

  log_info() << "TERM IT ALL" << std::endl;

  file_stream_term();
}
