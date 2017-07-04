#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

int main() {
	size_t MB = 1024 * 1024;
	size_t size = 100 * MB;
	size_t page = getpagesize();
	size_t total = 0;
	while (1) {
		unsigned char * p = malloc(size);
		total += size / MB;
		//printf("%zu\n", total);
		if (!p) {
			//puts("Couldn't allocate more memory");
			return 0;
		}
		for (size_t i = 0; i < size; i += page) {
			p[i] = rand() % 256;
		}
	}
}
