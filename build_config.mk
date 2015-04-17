CC=gcc
CXX=g++
MAKE=make
LEVELDB_PATH=/home/qwang/workspace/ssdb/deps/leveldb-1.14.0
JEMALLOC_PATH=/home/qwang/workspace/ssdb/deps/jemalloc-3.3.1
SNAPPY_PATH=/home/qwang/workspace/ssdb/deps/snappy-1.1.0
CFLAGS=
CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare
CFLAGS += 
CFLAGS += -I "/home/qwang/workspace/ssdb/deps/leveldb-1.14.0/include"
CLIBS=
CLIBS += -pthread
CLIBS += "/home/qwang/workspace/ssdb/deps/leveldb-1.14.0/libleveldb.a"
CLIBS += "/home/qwang/workspace/ssdb/deps/snappy-1.1.0/.libs/libsnappy.a"
CLIBS += "/home/qwang/workspace/ssdb/deps/jemalloc-3.3.1/lib/libjemalloc.a"
CFLAGS += -I "/home/qwang/workspace/ssdb/deps/jemalloc-3.3.1/include"