MODULE_big = pg_duckdb_translator
EXTENSION = pg_duckdb_translator
DATA = pg_duckdb_translator--1.0.sql
PGFILEDESC = "pg_duckdb_translator - translate pg query to duckdb logicalplan"


# Paths to DuckDB build (auto-detect relative to this contrib folder)
# By default assume sibling dir `../duckdb` under `contrib`.
DUCKDB_INC ?= $(abspath $(CURDIR)/../duckdb-1.4.2/src/include)
DUCKDB_LIB_DIR ?= $(abspath $(CURDIR)/../duckdb-1.4.2/build/release/src)
DUCKDB_LIB ?= -L$(DUCKDB_LIB_DIR) -lduckdb

CXX ?= g++
CC ?= gcc
CXXFLAGS ?= -std=c++14 -I$(DUCKDB_INC) -I../../include 
LDFLAGS ?= $(DUCKDB_LIB) -lstdc++ -lpthread



SOURCES_CC = pg_duckdb_translator.cc pg_duckdb_mapper.cc
SOURCES_C = pg_duckdb_sqlfuncs.c
# No runtime tests or proto sources included by default; keep test artifacts
# out of the extension build. Development tests should live under a
# separate `test/` or `examples/` directory.

OBJS_CC = $(SOURCES_CC:.cc=.o)
OBJS_C = $(SOURCES_C:.c=.o)

# Aggregate OBJS for top-level/shared-library rules so the linker
# invocation will include our object files. Without this, the
# generated `.so` can be produced without any .o inputs.
OBJS = $(OBJS_CC) $(OBJS_C)



%.o: %.cc
	$(CXX) -I$(abspath $(CURDIR)/../duckdb-1.4.2/src/include) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

%.o: %.c
	$(CC) $(CPPFLAGS) -c $< -o $@ -fPIC



REGRESS = tpch_schema tpch_q1


ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
CXXFLAGS += -std=c++14 
override PG_CPPFLAGS := $(PG_CPPFLAGS) -I$(abspath $(CURDIR)/../duckdb-1.4.2/src/include) 
CXXFLAGS += -std=c++14 
DUCKDB_LIBDIR ?= $(abspath $(CURDIR)/../duckdb-1.4.2/build/release/src)
SHLIB_LINK += -L$(DUCKDB_LIBDIR) -lduckdb -lstdc++ -lpthread
SHLIB_LINK += -Wl,--export-dynamic
include $(PGXS)
else
subdir = contrib/pg_duckdb_translator
top_builddir = ../..
override CPPFLAGS := $(CPPFLAGS) -I$(abspath $(CURDIR)/../duckdb-1.4.2/src/include) 
LDFLAGS += -Wl,--export-dynamic
# When building without PGXS, ensure we link against DuckDB and libstdc++
DUCKDB_LIBDIR ?= $(abspath $(CURDIR)/../duckdb-1.4.2/build/release/src)
SHLIB_LINK += -L$(DUCKDB_LIBDIR) -lduckdb -lstdc++ -lpthread
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
