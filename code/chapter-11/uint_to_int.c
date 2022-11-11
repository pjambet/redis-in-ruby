#include <stdio.h>
#include <inttypes.h>

int main() {
	int64_t i = 1;
	int64_t j = -3;
	printf("i: %" PRId64 "\n", i);
	printf("j: %" PRId64 "\n", j);
	printf("i: %" PRIu64 "\n", (uint64_t)i);
	printf("j: %" PRIu64 "\n", (uint64_t)j);
	return 0;
}
