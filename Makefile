LDFLAGS=-lpthread -lsnappy
CXXFLAGS=-march=native -ggdb -std=c++14 -I. -Wall

OFILES = $(patsubst %.cpp,%.o, $(wildcard *.cpp))
HFILES = $(wildcard *.h)

.PHONY: all clean run

all: fs

fs: ${OFILES}
	g++ -o fs ${OFILES} $(LDFLAGS)

test: test.o file_stream.h file_stream.o
	g++ -o test test.o file_stream.o $(LDFLAGS)

clean:
	$(RM) fs ${OFILES} *~

run: fs
	./fs

gdb: fs
	gdb ./fs

