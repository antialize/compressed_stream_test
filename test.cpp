#include <file_stream.h>

int main(int argc, char ** argv) {
  file<int> f;
  stream<int> s=f.stream();
}
