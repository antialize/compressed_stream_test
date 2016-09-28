#include <file_stream.h>
#include <cassert>

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
    assert(v = i);
  }

  file_stream_term();
}
