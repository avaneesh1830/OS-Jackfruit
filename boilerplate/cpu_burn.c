#include <stdio.h>
int main() {
    printf("CPU burn starting\n");
    fflush(stdout);
    volatile long i = 0;
    while(1) { i++; if(i % 100000000 == 0) { printf("iter %ld\n", i); fflush(stdout); } }
    return 0;
}
