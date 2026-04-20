# =============================================================================
#  Makefile — Multi-Container Runtime
#
#  Targets:
#    make            — build everything (engine + LKM + workloads)
#    make engine     — user-space binary only
#    make module     — kernel module (monitor.ko) only
#    make workloads  — test workload binaries only
#    make ci         — CI-safe compile check (engine only, no kernel headers)
#    make clean      — remove all build artefacts
# =============================================================================

# --------------------------------------------------------------------------- #
#  Toolchain                                                                   #
# --------------------------------------------------------------------------- #

CC      := gcc
CFLAGS  := -Wall -Wextra -O2 -g \
           -D_GNU_SOURCE \
           -pthread \
           -Wno-unused-result

# Kernel build system
KDIR    := /lib/modules/$(shell uname -r)/build
KBUILD  := $(MAKE) -C $(KDIR) M=$(CURDIR)

# --------------------------------------------------------------------------- #
#  Source / output names                                                       #
# --------------------------------------------------------------------------- #

ENGINE_SRC   := engine.c
ENGINE_BIN   := engine

WORKLOAD_SRC := cpu_burn.c io_bound.c mem_hog.c
WORKLOAD_BIN := cpu_burn io_bound mem_hog

# obj-m tells the kernel Makefile which module to build
obj-m := monitor.o

# --------------------------------------------------------------------------- #
#  Top-level targets                                                           #
# --------------------------------------------------------------------------- #

.PHONY: all engine module workloads ci clean

all: engine module workloads

# --------------------------------------------------------------------------- #
#  User-space engine                                                           #
# --------------------------------------------------------------------------- #

engine: $(ENGINE_SRC) monitor_ioctl.h
	$(CC) $(CFLAGS) -o $(ENGINE_BIN) $(ENGINE_SRC) -lpthread

# --------------------------------------------------------------------------- #
#  Kernel module                                                                #
# --------------------------------------------------------------------------- #

module: monitor_ioctl.h
	$(KBUILD) modules

# --------------------------------------------------------------------------- #
#  Workload binaries                                                           #
# --------------------------------------------------------------------------- #

workloads: $(WORKLOAD_BIN)

cpu_burn: cpu_burn.c
	$(CC) $(CFLAGS) -o $@ $<

io_bound: io_bound.c
	$(CC) $(CFLAGS) -o $@ $<

mem_hog: mem_hog.c
	$(CC) $(CFLAGS) -o $@ $<

# --------------------------------------------------------------------------- #
#  CI target (GitHub Actions — no kernel headers, no sudo needed)             #
# --------------------------------------------------------------------------- #

ci: $(ENGINE_SRC) monitor_ioctl.h
	$(CC) $(CFLAGS) -o $(ENGINE_BIN) $(ENGINE_SRC) -lpthread
	@echo "CI build OK"

# --------------------------------------------------------------------------- #
#  Clean                                                                       #
# --------------------------------------------------------------------------- #

clean:
	rm -f $(ENGINE_BIN) $(WORKLOAD_BIN)
	$(KBUILD) clean 2>/dev/null || true
	rm -f *.o *.ko *.mod *.mod.c *.order *.symvers .*.cmd
	rm -rf .tmp_versions
