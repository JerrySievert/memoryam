MODULE_big = memoryam
EXTENSION = memoryam
DATA = memoryam.control memoryam--0.0.1.sql

SRCS = src/memoryam.cpp src/store.cpp src/udf.cpp
OBJS = $(subst .cpp,.o, $(SRCS))

SHLIB_LINK += -std=c++17 -lstdc++ -O3
PG_CXXFLAGS += -std=c++17 -O3

REGRESS = create_extension types transactions updates

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
