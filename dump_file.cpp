#include <check_file.h>
#include <cstring>
#include <cstdlib>
#include <iostream>

int main(int argc, const char * argv[]) {
	const char ** fname = argv + 1;

	bool dumpcontents = false;
	if (strcmp(argv[1], "-h") == 0) {
		dumpcontents = true;
		fname++;
	}

	if (fname != argv + argc - 1) {
		std::cerr << "Usage: dump_file [-h] filename\n";
		return EXIT_FAILURE;
	}

	if (!check_file(*fname, true, dumpcontents))
		return EXIT_FAILURE;

	return EXIT_SUCCESS;
}
