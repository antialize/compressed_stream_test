#include <log.h>

#define TMP_FILE "/tmp/hello.tst"

template <typename T>
void ensure(T expect, T got, const char * name) {
	if (expect == got) return;
	log_info() << "Expected " << expect << " but got " << got << " in " << name << std::endl;
	abort();
}
