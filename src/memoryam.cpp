/**
 * We use extern here because as a c++ extension, we still want the
 * PostgreSQL functions to be accessible in our namespace.  We do this
 * again when we need to export symbols directly to PostgreSQL as well.
 */
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
#include "optimizer/plancat.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
}

#include "store.hpp"

Database database;
MemoryContext read_context = nullptr;

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

/**
 * @brief Entry point for creating a new table
 *
 * Here a new table is created and added to the #Database
 * instance.  We only support temporary tables, so our setup
 * is very simple and requires no locks.
 *
 * @param relation #Relation to be acted on
 * @param new relation file locator #RelFileLocator
 * @param persistence flag on how to behave
 * @param pointer to a xact #TransactionId to freeze at
 * @param minmulti #MultiXactId minimum and maximum transaction id's
 */
static void memoryam_relation_set_new_filelocator(
    Relation relation, const RelFileLocator *newrlocator, char persistence,
    TransactionId *freeze_xid, MultiXactId *minmulti) {
  // we only operate on temporary tables, so error out on any
  // creation that is not temporary
  if (persistence != RELPERSISTENCE_TEMP) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Only temporary tables are currently supported")));
  }

  // if existing and new relfilenode are different, that means the existing
  // storage was dropped and we also need to clean up the metadata and
  // state. If they are equal, this is a new relation object and we don't
  // need to clean anything.
  if (relation->rd_locator.relNumber != newrlocator->relNumber) {
    database.drop_table(relation);
  }

  // most recent minimum transaction id is a-ok
  *freeze_xid = RecentXmin;
  // as is the oldest multi transaction id
  *minmulti = GetOldestMultiXactId( );

  // again, in our case no need for locks, we can just create it
  database.create_table(relation);
}

/**
 * @brief Determines what kind of TupleTableSlotOps to use, we just
 * return `TTSOpsVirtual`
 */
static const TupleTableSlotOps *memoryam_slot_callbacks(Relation relation) {
  return &TTSOpsVirtual;
}

/**
 * @brief Begins a memoryam sequential scan, setting up the required
 * #MemoryScanDesc which stores the current states of the scan.
 *
 * Starts a scan,
 */
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

  scan->cursor = 0;

  /* Set up the columns needed for the scan. */
  for (AttrNumber scan_key = 0; scan_key < relation->rd_att->natts;
       scan_key++) {
    scan->needed_columns.push_back(scan_key);
  }

  return (TableScanDesc)scan;
}

/**
 * @brief Ends a memory scan, freeing any allocated memory
 */
static void memoryam_end_scan(TableScanDesc scan) { pfree(scan); }

static void memoryam_rescan(TableScanDesc scan, struct ScanKeyData *key,
                            bool set_params, bool allow_strat, bool allow_sync,
                            bool allow_pagemode) {
  // we do nothing here currently
}

/**
 * @brief Reads the next value visible in the sequential scan, returning
 * `true` if there is a row, and `false` if no more exist
 *
 * Attempts to read the next value in the #MemoryScanDesc,
 * @returns whether a row can be read as a #bool value
 */
static bool memoryam_getnextslot(TableScanDesc scan, ScanDirection direction,
                                 TupleTableSlot *slot) {
  MemoryScanDesc *memory_scan = (MemoryScanDesc *)scan;

  Table *table = database.retrieve_table(memory_scan->rs_base.rs_rd);

  bool found = table->next_value(memory_scan, slot);
  return found;
}

/**
 * @brief Fetch a version of a row by its #ItemPointer, making sure that
 * it is visible
 *
 * If a row is found to match the #ItemPointer and #Snapshot, it is
 * loaded into the #TupleTableSlot provided, and `true` is returned.
 * @returns whether the row has been fetched as a #bool
 */
static bool memoryam_fetch_row_version(Relation relation,
                                       ItemPointer item_pointer,
                                       Snapshot snapshot,
                                       TupleTableSlot *slot) {
  Table *table = database.retrieve_table(relation);
  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  if (!table->row_visible_in_snapshot(*item_pointer, snapshot)) {
    return false;
  }

  int natts              = relation->rd_att->natts;
  Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);
  TransactionId xact     = snapshot->xmax;
  if (xact == 0) {
    xact = GetCurrentTransactionId( );
  }

  if (read_context == nullptr) {
    read_context = AllocSetContextCreate(
        TopMemoryContext, "MemoryAM Read Storage", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  }

  MemoryContext old_context = MemoryContextSwitchTo(read_context);
  std::vector<AttrNumber> needed_columns =
      needed_column_list(slot->tts_tupleDescriptor, attr_needed);

  bool ret = table->read_row(*item_pointer, xact, slot, needed_columns);
  if (!ret) {
    MemoryContextSwitchTo(old_context);
    return false;
  }

  slot->tts_tableOid = RelationGetRelid(relation);

  MemoryContextSwitchTo(old_context);

  return true;
}

/* Size related functions */
static uint64 memoryam_relation_size(Relation rel, ForkNumber forkNumber) {
  return 4096 * BLCKSZ;
}

/**
 * @brief Returns an estimation of the #Relation size
 */
static void memoryam_estimate_rel_size(Relation relation, int32 *attr_widths,
                                       BlockNumber *pages, double *tuples,
                                       double *allvisfrac) {
  // find the #Table that is associated with this relation #Relation
  Table *table = database.retrieve_table(relation);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  *pages      = 1;
  *tuples     = table->row_metadata.size( );
  *allvisfrac = 1.0;
  get_rel_data_width(relation, attr_widths);
}

/**
 * @brief Determine whether a #Relation needs toast tables
 *
 * @param relation #Relation
 * @returns false as we do not use toast tables
 */
static bool memoryam_relation_needs_toast_table(Relation relation) {
  return false;
}

/**
 * @brief A call for an insertion of data into a memoryam #Table defined
 * by a row
 *
 * Insert a row of data into a #Table, detoasting any values and having
 * them allocated in the memory context of the table.  In return, we fill
 * the #TupleTableSlot provided with the saved tuple, along with its
 * potential #ItemPointerData as to where it might live if this transation
 * is committed.
 */
static void memoryam_tuple_insert(Relation relation, TupleTableSlot *slot,
                                  CommandId cid, int options,
                                  struct BulkInsertStateData *bistate) {
  // find the #Table that is associated with this relation #Relation
  Table *table = database.retrieve_table(relation);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  // retrieve all of the attributes, we will use them all during insert
  slot_getallattrs(slot);

  // get an allocation of all of the #Datum to be a copy including those
  // that have been toasted
  Datum *values = detoast_values(slot->tts_tupleDescriptor, slot->tts_values,
                                 slot->tts_isnull);

  // insert the row into the storage engine, which gives us #ItemPointerData
  // that points to the internal row number
  ItemPointerData tid = table->insert_row(relation, values, slot->tts_isnull);

  // set the ItemPointerData and the table relation ID
  slot->tts_tid      = tid;
  slot->tts_tableOid = RelationGetRelid(relation);
}

static void memoryam_tuple_insert_speculative(
    Relation rel, TupleTableSlot *slot, CommandId cid, int options,
    struct BulkInsertStateData *bistate, uint32 specToken) {
  ereport(ERROR,
          (errmsg("memoryam_tuple_insert_speculative is not implemented")));
}

static void memoryam_tuple_complete_speculative(Relation rel,
                                                TupleTableSlot *slot,
                                                uint32 specToken,
                                                bool succeeded) {
  ereport(ERROR,
          (errmsg("memoryam_tuple_complete_speculative is not implemented")));
}

static void memoryam_multi_insert(Relation rel, TupleTableSlot **slots,
                                  int nslots, CommandId cid, int options,
                                  struct BulkInsertStateData *bistate) {
  ereport(ERROR, (errmsg("memoryam_tuple_insert is not implemented")));
}

/**
 * @brief Deletes a row in the table
 *
 * When passed an #ItemPointer, deletes the row associated with it
 * and returns the result.
 * @returns result #TM_Result of deletion, either TM_Ok or TM_Deleted
 */
static TM_Result memoryam_tuple_delete(Relation relation, ItemPointer tip,
                                       CommandId cid, Snapshot snapshot,
                                       Snapshot crosscheck, bool wait,
                                       TM_FailureData *tmfd,
                                       bool changingPart) {
  Table *table = database.retrieve_table(relation);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  // call the delete_row method for the table
  return table->delete_row(*tip);
}

/**
 * @brief Updates a tuple, deleting the old and inserting a new one
 *
 * @returns Result of the update as #TM_Result
 */
static TM_Result memoryam_tuple_update(Relation relation, ItemPointer tip,
                                       TupleTableSlot *slot, CommandId cid,
                                       Snapshot snapshot, Snapshot crosscheck,
                                       bool wait, TM_FailureData *tmfd,
                                       LockTupleMode *lockmode,
                                       TU_UpdateIndexes *update_indexes) {
  Table *table = database.retrieve_table(relation);

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  TM_Result deletion_result = table->delete_row(*tip);
  if (deletion_result == TM_Deleted) {
    return TM_Deleted;
  }

  memoryam_tuple_insert(relation, slot, cid, 0, NULL);

  // we don't really have indexes currently, but we would normally tell
  // PostgreSQL to update any
  *update_indexes = TU_All;

  return TM_Ok;
}

static void memoryam_finish_bulk_insert(Relation rel, int options) {
  ereport(ERROR, (errmsg("memoryam_finish_bulk_insert is not implemented")));
}

static TM_Result memoryam_tuple_lock(Relation relation,
                                     ItemPointer item_pointer,
                                     Snapshot snapshot, TupleTableSlot *slot,
                                     CommandId cid, LockTupleMode mode,
                                     LockWaitPolicy wait_policy, uint8 flags,
                                     TM_FailureData *tmfd) {
  Table *table = database.retrieve_table(relation);
  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find pointer for id %d", relation->rd_id));
  }

  if (!table->row_visible_in_snapshot(*item_pointer, snapshot)) {
    return TM_Deleted;
  }

  int natts              = relation->rd_att->natts;
  Bitmapset *attr_needed = bms_add_range(NULL, 0, natts - 1);
  TransactionId xact     = snapshot->xmax;
  if (xact == 0) {
    xact = GetCurrentTransactionId( );
  }
  if (read_context == nullptr) {
    read_context = AllocSetContextCreate(
        TopMemoryContext, "MemoryAM Read Storage", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
  }

  MemoryContext old_context = MemoryContextSwitchTo(read_context);
  std::vector<AttrNumber> needed_columns =
      needed_column_list(slot->tts_tupleDescriptor, attr_needed);

  table->read_row(*item_pointer, xact, slot, needed_columns);

  slot->tts_tableOid = RelationGetRelid(relation);
  slot->tts_tid      = *item_pointer;

  MemoryContextSwitchTo(old_context);

  return TM_Ok;
}

static void memoryam_vacuum_rel(Relation rel, VacuumParams *params,
                                BufferAccessStrategy bstrategy) {

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

static void memoryam_subxact_callback(SubXactEvent event,
                                      SubTransactionId mySubid,
                                      SubTransactionId parentSubid, void *arg) {
  ereport(ERROR, (errmsg("memoryam_subxact_callback is not implemented")));
}

/**
 * @brief Callback for when a transaction is to be dealt with
 */
static void memoryam_xact_callback(XactEvent event, void *arg) {
  switch (event) {
  case XACT_EVENT_COMMIT:
  case XACT_EVENT_PARALLEL_COMMIT:
  case XACT_EVENT_PREPARE: {
    database.apply_transaction_changes_commit(GetCurrentTransactionId( ));
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
    // nothing to do in this case
    break;
  }
  }
}

/**
 * @brief Access hook for object access
 *
 * Intercepts calls to the object access hook to give us information that
 * we might care about.  In general, the only #ObjectAccessType that we
 * care about is `OAT_DROP`, which tells us to drop a table.  When this
 * occurs, we tell the storage engine to drop the table.
 */
static void memoryam_object_access_hook(ObjectAccessType access, Oid classId,
                                        Oid objectId, int subId, void *arg) {
  if (prev_ObjectAccessHook) {
    prev_ObjectAccessHook(access, classId, objectId, subId, arg);
  }

  // We care about dropping of tables here, if this is a drop
  // then we call the drop_table which tries to deal with it if it can
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
  prev_ExecutorEnd    = ExecutorEnd_hook;
  ExecutorEnd_hook    = memoryam_ExecutorEnd;
  prev_ExecutorFinish = ExecutorFinish_hook;
  ExecutorFinish_hook = memoryam_ExecutorFinish;

  // set up our object access hook, we only care for OAT_DROP
  prev_ObjectAccessHook = object_access_hook;
  object_access_hook    = memoryam_object_access_hook;

  // register our transaction callback
  RegisterXactCallback(memoryam_xact_callback, NULL);
  RegisterSubXactCallback(memoryam_subxact_callback, NULL);
}

} // extern "C"
