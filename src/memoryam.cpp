extern "C" {
#include "postgres.h"

#include "access/detoast.h"
#include "access/multixact.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class_d.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
}

#include "memoryam.hpp"
#include "store.hpp"

Database database;

static Datum *detoast_values(TupleDesc tupleDesc, Datum *orig_values,
                             bool *isnull) {
  int natts = tupleDesc->natts;
  struct varlena *new_value;

  /* copy on write to optimize for case where nothing is toasted */
  Datum *values = orig_values;

  for (int i = 0; i < tupleDesc->natts; i++) {
    if (!isnull[ i ] && tupleDesc->attrs[ i ].attlen == -1 &&
        VARATT_IS_EXTENDED(values[ i ])) {
      /* make a copy */
      if (values == orig_values) {
        values = (Datum *)palloc0(sizeof(Datum) * natts);
        memcpy(values, orig_values, sizeof(Datum) * natts);
      }

      /* will be freed when per-tuple context is reset */
      new_value   = (struct varlena *)DatumGetPointer(values[ i ]);
      new_value   = detoast_attr(new_value);
      values[ i ] = PointerGetDatum(new_value);
    }
  }

  return values;
}

static std::vector<AttrNumber> needed_column_list(TupleDesc tupdesc,
                                                  Bitmapset *attr_needed) {
  std::vector<AttrNumber> column_list;

  for (int column_number = 0; column_number < tupdesc->natts; column_number++) {
    if (tupdesc->attrs[ column_number ].attisdropped) {
      continue;
    }

    if (bms_is_member(column_number, attr_needed)) {
      AttrNumber varattno = column_number;
      column_list.push_back(varattno);
    }
  }

  return column_list;
}

static void memoryam_relation_set_new_filelocator(
    Relation rel, const RelFileLocator *newrlocator, char persistence,
    TransactionId *freezeXid, MultiXactId *minmulti) {
  if (persistence != RELPERSISTENCE_TEMP) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Only temporary tables are currently supported")));
  }

  /*
   * If existing and new relfilenode are different, that means the existing
   * storage was dropped and we also need to clean up the metadata and
   * state. If they are equal, this is a new relation object and we don't
   * need to clean anything.
   */
  if (rel->rd_locator.relNumber != newrlocator->relNumber) {
    database.drop_table(rel);
  }

  *freezeXid = RecentXmin;
  *minmulti  = GetOldestMultiXactId( );

  database.create_table(rel);
}

static const TupleTableSlotOps *memoryam_slot_callbacks(Relation relation) {
  return &TTSOpsVirtual;
}

/* Scan related functions */
static TableScanDesc memoryam_begin_scan(Relation relation, Snapshot snapshot,
                                         int nkeys, struct ScanKeyData *key,
                                         ParallelTableScanDesc parallel_scan,
                                         uint32 flags) {

  MemoryScanDesc *scan = (MemoryScanDesc *)palloc0(sizeof(MemoryScanDesc));

  scan->rs_base.rs_rd       = relation;
  scan->rs_base.rs_snapshot = snapshot;
  scan->rs_base.rs_nkeys    = 0;
  scan->rs_base.rs_key      = key;
  scan->rs_base.rs_flags    = flags;
  scan->rs_base.rs_parallel = parallel_scan;

  scan->cursor           = 0;
  scan->in_local_changes = false;

  /* Set up the columns needed for the scan. */
  for (AttrNumber scan_key = 0; scan_key < relation->rd_att->natts;
       scan_key++) {
    scan->needed_columns.push_back(scan_key);
  }

  return (TableScanDesc)scan;
}

static void memoryam_end_scan(TableScanDesc scan) { pfree(scan); }

static void memoryam_rescan(TableScanDesc scan, struct ScanKeyData *key,
                            bool set_params, bool allow_strat, bool allow_sync,
                            bool allow_pagemode) {
  DEBUG( );
}

static bool memoryam_getnextslot(TableScanDesc scan, ScanDirection direction,
                                 TupleTableSlot *slot) {
  MemoryScanDesc *memory_scan = (MemoryScanDesc *)scan;

  Table *table = database.retrieve_table(memory_scan->rs_base.rs_rd);

  bool found = table->next_value(memory_scan, slot);
  return found;
}

static bool memoryam_fetch_row_version(Relation relation,
                                       ItemPointer item_pointer,
                                       Snapshot snapshot,
                                       TupleTableSlot *slot) {
  DEBUG( );
  // ereport(ERROR, (errmsg("memoryam_fetch_row_version is not implemented")));
  Table *table = database.retrieve_table(relation);
  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  bool in_transaction_changes = (item_pointer->ip_posid & (1 << 15));

  size_t row_number = item_pointer_to_row_number(*item_pointer);
  elog(NOTICE, "row_number: %ld, in_transaction_changes: %s", row_number,
       in_transaction_changes ? "true" : "false");

  if (!table->row_visible_in_snapshot(*item_pointer, snapshot)) {
    elog(NOTICE, "row not visible in snapshot");
    return false;
  }

  int natts              = relation->rd_att->natts;
  Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);
  TransactionId xact     = snapshot->xmax;
  if (xact == 0) {
    elog(NOTICE, "xact was 0, setting to %d", GetCurrentTransactionId( ));
    xact = GetCurrentTransactionId( );
  }

  std::vector<AttrNumber> needed_columns =
      needed_column_list(slot->tts_tupleDescriptor, attr_needed);
  bool ret = table->read_row(*item_pointer, xact, slot, needed_columns);
  if (!ret) {
    elog(NOTICE, "returning false");
    return false;
  }
  //  ColumnarReadRowByRowNumber(*readState, rowNumber,
  //							   slot->tts_values,
  // slot->tts_isnull); 	MemoryContextSwitchTo(old_context);
  slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *item_pointer;
	elog(NOTICE, "column 0 value: %d", DatumGetInt32(slot->tts_values[0]));
	in_transaction_changes = (slot->tts_tid.ip_posid & (1 << 15));
	row_number = item_pointer_to_row_number(slot->tts_tid);
  elog(NOTICE, "fetch_row return row_number: %ld, in_transaction_changes: %s",
       row_number, in_transaction_changes ? "true" : "false");

	if (TTS_EMPTY(slot))
		ExecStoreVirtualTuple(slot);

  return true;
}

/* Size related functions */
static uint64 memoryam_relation_size(Relation rel, ForkNumber forkNumber) {
  DEBUG( );
  return 4096 * BLCKSZ;
}

static void memoryam_estimate_rel_size(Relation rel, int32 *attr_widths,
                                       BlockNumber *pages, double *tuples,
                                       double *allvisfrac) {
  DEBUG( );
  *pages      = 4;
  *tuples     = 3;
  *allvisfrac = 1.0;
}

static bool memoryam_relation_needs_toast_table(Relation rel) { return false; }

/* Storage related functions */
static void memoryam_tuple_insert(Relation rel, TupleTableSlot *slot,
                                  CommandId cid, int options,
                                  struct BulkInsertStateData *bistate) {
  DEBUG( );
  Table *table = database.retrieve_table(rel);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", rel->rd_id));
  }

  Datum *values;

  slot_getallattrs(slot);
  values = detoast_values(slot->tts_tupleDescriptor, slot->tts_values,
                          slot->tts_isnull);

  ItemPointerData tid = table->insert_row(rel, values, slot->tts_isnull);

  slot->tts_tid = tid;

  slot->tts_tableOid = RelationGetRelid(rel);
  size_t row_number = item_pointer_to_row_number(tid);
  bool in_transaction_changes = (tid.ip_posid & (1 << 15));
  elog(NOTICE, "insert return row_number: %ld, in_transaction_changes: %s",
       row_number, in_transaction_changes ? "true" : "false");
  elog(NOTICE, "column 0 value: %d", DatumGetInt32(slot->tts_values[0]));
}

static void memoryam_tuple_insert_speculative(
    Relation rel, TupleTableSlot *slot, CommandId cid, int options,
    struct BulkInsertStateData *bistate, uint32 specToken) {
  DEBUG( );
  ereport(ERROR,
          (errmsg("memoryam_tuple_insert_speculative is not implemented")));
}

static void memoryam_tuple_complete_speculative(Relation rel,
                                                TupleTableSlot *slot,
                                                uint32 specToken,
                                                bool succeeded) {
  DEBUG( );
  ereport(ERROR,
          (errmsg("memoryam_tuple_complete_speculative is not implemented")));
}

static void memoryam_multi_insert(Relation rel, TupleTableSlot **slots,
                                  int nslots, CommandId cid, int options,
                                  struct BulkInsertStateData *bistate) {
  DEBUG( );
  ereport(ERROR, (errmsg("memoryam_tuple_insert is not implemented")));
}

static TM_Result memoryam_tuple_delete(Relation rel, ItemPointer tip,
                                       CommandId cid, Snapshot snapshot,
                                       Snapshot crosscheck, bool wait,
                                       TM_FailureData *tmfd,
                                       bool changingPart) {
  Table *table = database.retrieve_table(rel);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", rel->rd_id));
  }

  return table->delete_row(rel, *tip);
}

static TM_Result memoryam_tuple_update(Relation relation, ItemPointer tip,
                                       TupleTableSlot *slot, CommandId cid,
                                       Snapshot snapshot, Snapshot crosscheck,
                                       bool wait, TM_FailureData *tmfd,
                                       LockTupleMode *lockmode,
                                       TU_UpdateIndexes *update_indexes) {
  DEBUG( );
  Table *table = database.retrieve_table(relation);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  TM_Result deletion_result = table->delete_row(relation, *tip);
  if (deletion_result == TM_Deleted) {
    return TM_Deleted;
  }

 	memoryam_tuple_insert(relation, slot, cid, 0, NULL);
#if 0
  Datum *values;

  slot_getallattrs(slot);
  values = detoast_values(slot->tts_tupleDescriptor, slot->tts_values,
                          slot->tts_isnull);

  ItemPointerData tid = table->insert_row(rel, values, slot->tts_isnull);
  slot->tts_tid       = tid;

  slot->tts_tableOid          = RelationGetRelid(rel);
  bool in_transaction_changes = (tid.ip_posid & (1 << 15));

  size_t row_number = item_pointer_to_row_number(tid);
  elog(NOTICE, "update return row_number: %ld, in_transaction_changes: %s",
       row_number, in_transaction_changes ? "true" : "false");
#endif
  *update_indexes = TU_All;
  elog(NOTICE, "column 0 value: %d", DatumGetInt32(slot->tts_values[0]));
  size_t row_number = item_pointer_to_row_number(slot->tts_tid);
  bool in_transaction_changes = (slot->tts_tid.ip_posid & (1 << 15));

  elog(NOTICE, "update return row_number: %ld, in_transaction_changes: %s",
       row_number, in_transaction_changes ? "true" : "false");

  return TM_Ok; // TM_Updated;
}

static void memoryam_finish_bulk_insert(Relation rel, int options) {
  DEBUG( );
  ereport(ERROR, (errmsg("memoryam_finish_bulk_insert is not implemented")));
}

static TM_Result memoryam_tuple_lock(Relation relation,
                                     ItemPointer item_pointer,
                                     Snapshot snapshot, TupleTableSlot *slot,
                                     CommandId cid, LockTupleMode mode,
                                     LockWaitPolicy wait_policy, uint8 flags,
                                     TM_FailureData *tmfd) {
  DEBUG( );
  // ereport(ERROR, (errmsg("memoryam_fetch_row_version is not implemented")));
  Table *table = database.retrieve_table(relation);
  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  bool in_transaction_changes = (item_pointer->ip_posid & (1 << 15));

  size_t row_number = item_pointer_to_row_number(*item_pointer);
  elog(NOTICE, "row_number: %ld, in_transaction_changes: %s", row_number,
       in_transaction_changes ? "true" : "false");

  if (!table->row_visible_in_snapshot(*item_pointer, snapshot)) {
    elog(NOTICE, "row not visible in snapshot");
    return TM_Deleted;
  }

  int natts              = relation->rd_att->natts;
  Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);
  TransactionId xact     = snapshot->xmax;
  if (xact == 0) {
    elog(NOTICE, "xact was 0, setting to %d", GetCurrentTransactionId( ));
    xact = GetCurrentTransactionId( );
  }

  std::vector<AttrNumber> needed_columns =
      needed_column_list(slot->tts_tupleDescriptor, attr_needed);
  bool ret = table->read_row(*item_pointer, xact, slot, needed_columns);

  slot->tts_tableOid = RelationGetRelid(relation);
  slot->tts_tid = *item_pointer;
  ExecStoreVirtualTuple(slot);

  return TM_Ok;
}

static void memoryam_vacuum_rel(Relation rel, VacuumParams *params,
                                BufferAccessStrategy bstrategy) {

  DEBUG( );
  ereport(INFO, (errmsg("memoryam_vacuum_rel is not implemented")));
}

const TableAmRoutine memoryam_methods = {
  .type = T_TableAmRoutine,

  .slot_callbacks = memoryam_slot_callbacks,

  .scan_begin       = memoryam_begin_scan,
  .scan_end         = memoryam_end_scan,
  .scan_rescan      = memoryam_rescan,
  .scan_getnextslot = memoryam_getnextslot,

  .tuple_fetch_row_version = memoryam_fetch_row_version,

  .tuple_insert               = memoryam_tuple_insert,
  .tuple_insert_speculative   = memoryam_tuple_insert_speculative,
  .tuple_complete_speculative = memoryam_tuple_complete_speculative,
  .multi_insert               = memoryam_multi_insert,
  .tuple_delete               = memoryam_tuple_delete,
  .tuple_update               = memoryam_tuple_update,
  .tuple_lock                 = memoryam_tuple_lock,
  .finish_bulk_insert         = memoryam_finish_bulk_insert,

  .relation_set_new_filelocator = memoryam_relation_set_new_filelocator,
  .relation_vacuum              = memoryam_vacuum_rel,

  .relation_size              = memoryam_relation_size,
  .relation_needs_toast_table = memoryam_relation_needs_toast_table,

  .relation_estimate_size = memoryam_estimate_rel_size,

};

static ExecutorEnd_hook_type prev_ExecutorEnd        = nullptr;
static ExecutorEnd_hook_type prev_ExecutorFinish     = nullptr;
static ProcessUtility_hook_type prev_ProcessUtility  = nullptr;
static object_access_hook_type prev_ObjectAccessHook = nullptr;

static void memoryam_ExecutorEnd(QueryDesc *queryDesc) {
  if (prev_ExecutorEnd) {
    prev_ExecutorEnd(queryDesc);
  } else {
    standard_ExecutorEnd(queryDesc);
  }
}

static void memoryam_ExecutorFinish(QueryDesc *queryDesc) {
  if (prev_ExecutorFinish) {
    prev_ExecutorFinish(queryDesc);
  } else {
    standard_ExecutorFinish(queryDesc);
  }
}

static void memoryam_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                    bool readOnlyTree,
                                    ProcessUtilityContext context,
                                    ParamListInfo params,
                                    QueryEnvironment *queryEnv,
                                    DestReceiver *dest, QueryCompletion *qc) {
  if (prev_ProcessUtility) {
    prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                        queryEnv, dest, qc);
  } else {
    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                            queryEnv, dest, qc);
  }
}

static void memoryam_subxact_callback(SubXactEvent event,
                                      SubTransactionId mySubid,
                                      SubTransactionId parentSubid, void *arg) {
  elog(NOTICE,
       "memoryam_subxact_callback: %d (%d / %d), transaction id %d / %d", event,
       mySubid, parentSubid, GetCurrentTransactionId( ),
       GetCurrentSubTransactionId( ));
#if 0
  switch (event) {
  case SUBXACT_EVENT_START_SUB:
  case SUBXACT_EVENT_COMMIT_SUB: {
    /* nothing to do */
    break;
  }

  case SUBXACT_EVENT_ABORT_SUB: {
    DiscardWriteStateForAllRels(mySubid, parentSubid);
    CleanupReadStateCache(mySubid);
    break;
  }

  case SUBXACT_EVENT_PRE_COMMIT_SUB: {
    FlushWriteStateForAllRels(mySubid, parentSubid);
    CleanupReadStateCache(mySubid);
    break;
  }
  }
#endif
}

static void memoryam_xact_callback(XactEvent event, void *arg) {
  MemoryContext old_context = MemoryContextSwitchTo(database.memory_context);

  switch (event) {
  case XACT_EVENT_COMMIT:
  case XACT_EVENT_PARALLEL_COMMIT:
  case XACT_EVENT_PREPARE: {
    /* nothing to do */
    break;
  }

  case XACT_EVENT_ABORT:
  case XACT_EVENT_PARALLEL_ABORT: {
    database.apply_transaction_changes_rollback(GetCurrentTransactionId( ));
    break;
  }

  case XACT_EVENT_PRE_COMMIT:
  case XACT_EVENT_PARALLEL_PRE_COMMIT:
  case XACT_EVENT_PRE_PREPARE: {
    database.apply_transaction_changes_commit(GetCurrentTransactionId( ));
    break;
  }
  }

  MemoryContextSwitchTo(old_context);
}

static void memoryam_object_access_hook(ObjectAccessType access, Oid classId,
                                        Oid objectId, int subId, void *arg) {
  if (prev_ObjectAccessHook) {
    prev_ObjectAccessHook(access, classId, objectId, subId, arg);
  }

  /* dispatch to the proper action */
  if (access == OAT_DROP && classId == RelationRelationId &&
      !OidIsValid(subId)) {
    LockRelationOid(objectId, AccessShareLock);

    Relation relation = table_open(objectId, AccessExclusiveLock);

    database.drop_table(relation);

    table_close(relation, NoLock);
  }
}

extern "C" {

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(memoryam_tableam_handler);
Datum memoryam_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&memoryam_methods);
}

void _PG_init(void) {
  prev_ExecutorEnd      = ExecutorEnd_hook;
  ExecutorEnd_hook      = memoryam_ExecutorEnd;
  prev_ExecutorFinish   = ExecutorFinish_hook;
  ExecutorFinish_hook   = memoryam_ExecutorFinish;
  prev_ProcessUtility   = ProcessUtility_hook;
  ProcessUtility_hook   = memoryam_ProcessUtility;
  prev_ObjectAccessHook = object_access_hook;
  object_access_hook    = memoryam_object_access_hook;

  RegisterXactCallback(memoryam_xact_callback, NULL);
  RegisterSubXactCallback(memoryam_subxact_callback, NULL);

  /* Set up the working MemoryContext for data storage. */
  database.memory_context = AllocSetContextCreate(
      TopMemoryContext, "MemoryAM Storage", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
}

} // extern "C"
