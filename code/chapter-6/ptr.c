#include <stdio.h>
int main() {
   void *ptr;
   printf("The size of pointer value : %d\n", sizeof(ptr));
   printf("And after cast: %d\n", sizeof((unsigned char*)ptr));
   return 0;
}
