LDFLAGS=-lpthread -lsnappy
CXXFLAGS=-march=native -ggdb -std=c++14
.PHONY: all clean

all: fs

clean:
	$(RM) fs *~
