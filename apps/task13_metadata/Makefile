# Build trong container deepstream_dev:  make -C /workspace/apps/task13_metadata
# Cần: libgstreamer1.0-dev (xem scripts/setup_build_deps.sh)
CUDA_VER?=12.4
NVDS_VERSION:=7.0
APP:= task13-meta-app

DS_ROOT:=/opt/nvidia/deepstream/deepstream-$(NVDS_VERSION)
LIB_INSTALL_DIR?=$(DS_ROOT)/lib/

SRCS:= $(wildcard *.c)
OBJS:= $(SRCS:.c=.o)
PKGS:= gstreamer-1.0

CFLAGS+= -O2 -Wall -I$(DS_ROOT)/sources/includes -I/usr/local/cuda-$(CUDA_VER)/include
CFLAGS+= $(shell pkg-config --cflags $(PKGS))
LIBS:= $(shell pkg-config --libs $(PKGS))
LIBS+= -L$(LIB_INSTALL_DIR) -lnvdsgst_meta -lnvds_meta \
	-Wl,-rpath,$(LIB_INSTALL_DIR)

all: $(APP)

%.o: %.c Makefile
	$(CC) -c -o $@ $(CFLAGS) $<

$(APP): $(OBJS) Makefile
	$(CC) -o $(APP) $(OBJS) $(LIBS)

clean:
	rm -rf $(OBJS) $(APP)
