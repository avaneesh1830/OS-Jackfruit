#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int main() {
    printf("Memory test starting\n");
    fflush(stdout);
    size_t chunk = 4 * 1024 * 1024; // 4MB chunks
    int count = 0;
    while(1) {
        char *p = malloc(chunk);
        if (!p) { printf("malloc failed at %d chunks\n", count); break; }
        memset(p, 0xAB, chunk);
        count++;
        printf("Allocated %d MB\n", count * 4);
        fflush(stdout);
        sleep(1);
    }
    sleep(10);
    return 0;
}
