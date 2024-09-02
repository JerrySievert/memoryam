#pragma once

extern "C" {
#include "postgres.h"

#include "access/relscan.h"
}

#include <vector>

#define DEBUG( ) elog(NOTICE, "ENTER %s:%d - %s", __FILE__, __LINE__, __func__)
