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

/**
 * @brief UDF to give specifics about a #Relation that has
 * been stored as a `memoryam` table.  This is called by
 * `SELECT` statement in PostgreSQL, not directly.
 */
Datum memoryam_relation_details(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx = nullptr;
  TupleDesc tupdesc        = nullptr;

  // if this is the first call in, set up the data
  if (SRF_IS_FIRSTCALL( )) {
    Oid relid         = PG_GETARG_OID(0);
    Relation relation = RelationIdGetRelation(relid);

    // retrieve the table, if it isn't found it isn't a MemoryAM table
    Table *table = database.retrieve_table(relation);
    if (table == nullptr) {
      ereport(ERROR, errcode(ERRCODE_INVALID_TABLE_DEFINITION),
              errmsg("Relation is not a memoryam table"));
    }

    // close the relation, we don't need it further
    relation_close(relation, NoLock);

    // set our function context to the first call
    funcctx = SRF_FIRSTCALL_INIT( );

    // wse the SRF memory context, if there are ever allocations
    // then this would matter
    MemoryContext oldcontext =
        MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // set the function context to be the table, we'll use the call
    // counter as our row number, as it is a #uint64
    funcctx->user_fctx = table;
    funcctx->max_calls = table->row_metadata.size( );

    // make sure we're only called in the context of a composite type
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context "
                             "that cannot accept type record")));
    }

    // check the call context to make sure that we're being called in a
    // context that we are expecting:
    //   int64 - row_number
    //   int32 - xmin
    //   int32 - xmax
    //   bool - is_deleted
    if (tupdesc->attrs[ 0 ].atttypid != INT8OID ||
        tupdesc->attrs[ 1 ].atttypid != INT4OID ||
        tupdesc->attrs[ 2 ].atttypid != INT4OID ||
        tupdesc->attrs[ 3 ].atttypid != BOOLOID) {
      ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("memoryam_relation_details called in an unknown context"));
    }

    // switch back to our original memory context
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP( );

  // initialize our call counts
  int call_cntr = funcctx->call_cntr;
  int max_calls = funcctx->max_calls;

  // and our #Table instance
  Table *table = (Table *)funcctx->user_fctx;

  // if we haven't hit our maximum number of calls (table->row_metadata.size())
  // then we can return another row, or set
  if (call_cntr < max_calls) {
    // we are returning 5 columns: row number, minimum transaction, maximum
    // transaction, whether considered deleted, and whether a change in the
    // current transaction
    Datum values[ 4 ] = { 0, 0, 0, 0 };
    bool nulls[ 4 ]   = { 0, 0, 0, 0 };

    // check to see whether this row would be considered deleted
    bool is_deleted =
        (table->row_metadata[ call_cntr ].xmax != 0 &&
         table->row_metadata[ call_cntr ].xmax <= GetCurrentTransactionId( )) ||
        table->row_deleted_in_transaction(call_cntr,
                                          GetCurrentTransactionId( ));

    // set the value for each column of our return set
    values[ 0 ] = Int64GetDatum(call_cntr);
    values[ 1 ] = Int32GetDatum(table->row_metadata[ call_cntr ].xmin);
    values[ 2 ] = Int32GetDatum(table->row_metadata[ call_cntr ].xmax);
    values[ 3 ] = BoolGetDatum(is_deleted);

    // create and form the tuple to return
    get_call_result_type(fcinfo, NULL, &tupdesc);

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    Datum result    = HeapTupleGetDatum(tuple);

    // tell the caller that we are returning a #Datum containing a #HeapTuple
    SRF_RETURN_NEXT(funcctx, result);
  } else {
    // otherwise, we tell the caller that we are done
    SRF_RETURN_DONE(funcctx);
  }
}

/**
 * @brief UDF to give specifics about all #Relation #Table
 * entries stored, including the table name, row count, number
 * of deleted rows (in this transaction), and the number of open
 * transactions being tracked.
 */
Datum memoryam_storage_details(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx = nullptr;
  TupleDesc tupdesc        = nullptr;

  // if this is the first call in, set up the data
  if (SRF_IS_FIRSTCALL( )) {
    MemoryContext oldcontext;

    // function context for persistance
    funcctx = SRF_FIRSTCALL_INIT( );

    // use the SRF memory context
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    // make sure we're only called in the context of a composite type
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context "
                             "that cannot accept type record")));
    }

    // check the call context to make sure that we're being called in a
    // context that we are expecting:
    //   text - table name
    //   int64 - row count
    //   int64 - deleted count
    //   int64 - transaction count
    if (tupdesc->attrs[ 0 ].atttypid != TEXTOID ||
        tupdesc->attrs[ 1 ].atttypid != INT8OID ||
        tupdesc->attrs[ 2 ].atttypid != INT8OID ||
        tupdesc->attrs[ 3 ].atttypid != INT8OID) {
      ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR),
              errmsg("memoryam_storage_details called in an unknown context"));
    }

    // make a vector to store the Oid of each table to display
    std::vector<Oid> *tables = new std::vector<Oid>( );

    for (const auto &[ key, value ] : database.tables) {
      tables->push_back(key);
    }

    funcctx->user_fctx = (void *)tables;
    funcctx->max_calls = tables->size( );

    MemoryContextSwitchTo(oldcontext);
  }

  // do our per-call setup, which gets us ready for each call
  funcctx = SRF_PERCALL_SETUP( );

  int call_cntr            = funcctx->call_cntr;
  int max_calls            = funcctx->max_calls;
  std::vector<Oid> *tables = (std::vector<Oid> *)funcctx->user_fctx;

  if (call_cntr < max_calls) {
    // if there are still results to return, build a #HeapTuple for it
    Datum result;

    // we are returning 4 columns: table name, total row count, deleted row
    // count, transaction count
    Datum values[ 4 ] = { 0, 0, 0, 0 };
    bool nulls[ 4 ]   = { 0, 0, 0, 0 };

    Table *table = database.tables[ (*tables)[ call_cntr ] ];

    values[ 0 ] = CStringGetTextDatum(table->name.c_str( ));
    values[ 1 ] = Int64GetDatum(table->row_metadata.size( ));
    values[ 2 ] = Int64GetDatum(
        table->deleted_count_for_transaction(GetCurrentTransactionId( )));
    values[ 3 ] = Int64GetDatum(table->transaction_count( ));

    // create the heap tuple for return
    get_call_result_type(fcinfo, NULL, &tupdesc);

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    result          = HeapTupleGetDatum(tuple);

    // return the next result
    SRF_RETURN_NEXT(funcctx, result);
  } else {
    // clean up our temporary storage
    delete tables;
    // and return that we are done
    SRF_RETURN_DONE(funcctx);
  }
}
