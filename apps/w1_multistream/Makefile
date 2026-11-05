# Build trong container deepstream_dev:  make -C /workspace/apps/w1_multistream
# Cần: libgstreamer1.0-dev, libhiredis-dev (xem scripts/setup_build_deps.sh)
CUDA_VER?=12.4
NVDS_VERSION:=7.0
APP:= w1-app
ANNOTATE:= w1-annotate

DS_ROOT:=/opt/nvidia/deepstream/deepstream-$(NVDS_VERSION)
LIB_INSTALL_DIR?=$(DS_ROOT)/lib/

OBJS:= w1_app.o
PKGS:= gstreamer-1.0 hiredis

CFLAGS+= -O2 -Wall -I$(DS_ROOT)/sources/includes -I/usr/local/cuda-$(CUDA_VER)/include
CFLAGS+= $(shell pkg-config --cflags $(PKGS))
# hiredis hệ thống (0.14) qua đường dẫn tuyệt đối, tránh lẫn bản libhiredis 1.1 trong lib DeepStream
LIBS:= $(shell pkg-config --libs gstreamer-1.0) /usr/lib/x86_64-linux-gnu/libhiredis.so -lm
LIBS+= -L$(LIB_INSTALL_DIR) -lnvdsgst_meta -lnvds_meta -lnvbufsurface -lnvds_batch_jpegenc \
	-Wl,-rpath,$(LIB_INSTALL_DIR)

all: $(APP) $(ANNOTATE)

%.o: %.c Makefile
	$(CC) -c -o $@ $(CFLAGS) $<

$(APP): $(OBJS) Makefile
	$(CC) -o $(APP) $(OBJS) $(LIBS)

$(ANNOTATE): w1_annotate.o Makefile
	$(CC) -o $(ANNOTATE) w1_annotate.o $(shell pkg-config --libs gstreamer-1.0) -lm \
		-L$(LIB_INSTALL_DIR) -lnvdsgst_meta -lnvds_meta -Wl,-rpath,$(LIB_INSTALL_DIR)

clean:
	rm -rf *.o $(APP) $(ANNOTATE)
