#include <stdexcept>
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "access/xact.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
}

#include "store.hpp"

extern "C" {
Datum memoryam_relation_details(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(memoryam_relation_details);

Datum memoryam_storage_details(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(memoryam_storage_details);
}

Datum memoryam_relation_details(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  int call_cntr;
  int max_calls;
  TupleDesc tupdesc;
  AttInMetadata *attinmeta;

  /* If this is the first call in, set up the data. */
  if (SRF_IS_FIRSTCALL( )) {
    MemoryContext oldcontext;

    Oid relid         = PG_GETARG_OID(0);
    Relation relation = RelationIdGetRelation(relid);

    /* Function context for persistance. */
    funcctx = SRF_FIRSTCALL_INIT( );

    /* Use the SRF memory context */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* Retrieve the table. */
    Table *table = database.retrieve_table(relation);
    if (table == nullptr) {
      ereport(ERROR, errcode(ERRCODE_INVALID_TABLE_DEFINITION),
              errmsg("Relation is not a memoryam table"));
    }

    funcctx->user_fctx = table;
    funcctx->max_calls = table->row_metadata.size( );

    try {
      funcctx->max_calls +=
          table->transaction_inserts[ GetCurrentTransactionId( ) ].size( );
    } catch (std::out_of_range error) {
      // We can ignore this.
    }

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context "
                             "that cannot accept type record")));
    }

    attinmeta          = TupleDescGetAttInMetadata(tupdesc);
    funcctx->attinmeta = attinmeta;

    relation_close(relation, NoLock);

    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP( );

  call_cntr    = funcctx->call_cntr;
  max_calls    = funcctx->max_calls;
  Table *table = (Table *)funcctx->user_fctx;

  if (call_cntr < max_calls) {
    Datum result;
    bool in_transaction = false;

    /* If we have surpassed the main row metadata, then we are into the
     * transaction rows. */
    if (call_cntr >= table->row_metadata.size( )) {
      call_cntr      = call_cntr - table->row_metadata.size( );
      in_transaction = true;
    }

    /* We are returning 5 columns: row number, minimum transaction, maximum
     * transaction, whether considered deleted, and whether part of the
     * transaction. */
    Datum values[ 5 ] = { 0, 0, 0, 0, 0 };
    bool nulls[ 5 ]   = { 0, 0, 0, 0, 0 };

    get_call_result_type(fcinfo, NULL, &tupdesc);

    if (in_transaction) {
      bool is_deleted_in_transaction =
          table->transaction_inserts[ GetCurrentTransactionId( ) ][ call_cntr ]
                  .xmax != 0 &&
          table->transaction_inserts[ GetCurrentTransactionId( ) ][ call_cntr ]
                  .xmax <= GetCurrentTransactionId( );

      values[ 0 ] = Int64GetDatum(call_cntr);
      values[ 1 ] = Int32GetDatum(
          table->transaction_inserts[ GetCurrentTransactionId( ) ][ call_cntr ]
              .xmin);
      values[ 2 ] = Int32GetDatum(
          table->transaction_inserts[ GetCurrentTransactionId( ) ][ call_cntr ]
              .xmax);
      values[ 3 ] = BoolGetDatum(is_deleted_in_transaction);
      values[ 4 ] = BoolGetDatum(true);
    } else {
      bool is_deleted =
          table->row_metadata[ call_cntr ].xmax != 0 &&
          table->row_metadata[ call_cntr ].xmax <= GetCurrentTransactionId( );
      values[ 0 ] = Int64GetDatum(call_cntr);
      values[ 1 ] = Int32GetDatum(table->row_metadata[ call_cntr ].xmin);
      values[ 2 ] = Int32GetDatum(table->row_metadata[ call_cntr ].xmax);
      values[ 3 ] = BoolGetDatum(is_deleted ||
                                 table->row_deleted_in_transaction(
                                     call_cntr, GetCurrentTransactionId( )));
      values[ 4 ] = BoolGetDatum(false);
    }

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    result          = HeapTupleGetDatum(tuple);

    SRF_RETURN_NEXT(funcctx, result);
  } else {
    SRF_RETURN_DONE(funcctx);
  }
}

Datum memoryam_storage_details(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  int call_cntr;
  int max_calls;
  TupleDesc tupdesc;
  AttInMetadata *attinmeta;

  /* If this is the first call in, set up the data. */
  if (SRF_IS_FIRSTCALL( )) {
    MemoryContext oldcontext;

    /* Function context for persistance. */
    funcctx = SRF_FIRSTCALL_INIT( );

    /* Use the SRF memory context */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    std::vector<Table *> *tables = new std::vector<Table *>( );

    for (const auto &[ key, value ] : database.tables) {
      tables->push_back(value);
    }

    funcctx->user_fctx = (void *)tables;
    funcctx->max_calls = tables->size( );

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context "
                             "that cannot accept type record")));
    }

    attinmeta          = TupleDescGetAttInMetadata(tupdesc);
    funcctx->attinmeta = attinmeta;

    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP( );

  call_cntr                    = funcctx->call_cntr;
  max_calls                    = funcctx->max_calls;
  std::vector<Table *> *tables = (std::vector<Table *> *)funcctx->user_fctx;

  if (call_cntr < max_calls) {
    Datum result;

    /* We are returning 4 columns: table name, total row count, deleted row
     * count, transaction count */
    Datum values[ 4 ] = { 0, 0, 0, 0 };
    bool nulls[ 4 ]   = { 0, 0, 0, 0 };

    get_call_result_type(fcinfo, NULL, &tupdesc);

    Table *table = (*tables)[ call_cntr ];

    values[ 0 ] = CStringGetTextDatum(table->name.c_str( ));
    values[ 1 ] = Int64GetDatum(table->row_metadata.size( ));
    values[ 2 ] = Int64GetDatum(
        table->deleted_count_for_transaction(GetCurrentTransactionId( )));
    values[ 3 ] = Int64GetDatum(table->transaction_count( ));

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    result          = HeapTupleGetDatum(tuple);

    SRF_RETURN_NEXT(funcctx, result);
  } else {
    delete tables;
    SRF_RETURN_DONE(funcctx);
  }
}
