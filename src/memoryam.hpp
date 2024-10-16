#pragma once

extern "C" {
#include "postgres.h"

#include "access/relscan.h"
}

#include <vector>

#ifdef DEBUG
#define DEBUG( ) elog(NOTICE, "ENTER %s:%d - %s", __FILE__, __LINE__, __func__)
#else
#define DEBUG( )                                                               \
  {}
#endif
