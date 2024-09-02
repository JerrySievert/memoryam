MODULE_big = memoryam
EXTENSION = memoryam
DATA = memoryam.control memoryam--0.0.1.sql

SRCS = src/memoryam.cpp src/store.cpp src/udf.cpp
OBJS = $(subst .cpp,.o, $(SRCS))

SHLIB_LINK += -std=c++17 -lstdc++ -g
PG_CXXFLAGS += -g -std=c++17

REGRESS = create_extension types transactions

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
