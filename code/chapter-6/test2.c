#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>

#define ROTL(x, b) (uint64_t)(((x) << (b)) | ((x) >> (64 - (b))))

int main() {

  uint64_t v0 = 0x736f6d6570736575ULL;
  uint64_t v1 = 0x646f72616e646f6dULL;

  printf("%" PRIu64 "\n", v1);
  printf("%" PRIu64 "\n", ROTL(v1, 13));

  return 0;
}
