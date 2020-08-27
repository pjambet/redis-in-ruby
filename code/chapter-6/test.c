#include <stdio.h>
//#include <stdint.h>
#include <limits.h>

int main() {

//#if defined(__X86_64__) || defined(__x86_64__) || defined (__i386__) || defined (__aarch64__) || defined (__arm64__)
//printf("YO!");
//#endif

  printf("%li\n", LONG_MAX);
  printf("%lu\n", LONG_MAX);
  printf("%li\n", LONG_MAX + 1);
  printf("%lu\n", LONG_MAX + 1);
  return 0;
}
