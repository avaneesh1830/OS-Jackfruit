#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
    size_t chunk_mb = (argc > 1) ? strtoul(argv[1], NULL, 10) : 8;
    size_t chunk_bytes = chunk_mb * 1024 * 1024;
    int count = 0;

    while (1) {
        char *mem = malloc(chunk_bytes);
        if (!mem) { printf("malloc failed after %d\n", count); break; }
        memset(mem, 'A', chunk_bytes);
        count++;
        printf("allocation=%d total=%zuMB\n", count, (size_t)count * chunk_mb);
        fflush(stdout);
        sleep(1);
    }
    return 0;
}
