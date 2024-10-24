#include <stdexcept>
#include <unordered_map>

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include "store.hpp"

/**
 * @brief Convert a row number to #ItemPointerData
 *
 * When passed a 64bit #size_t row number, we convert that to
 * two 32bit numbers defined as a #BlockNumber and an #OffsetNumber.
 * We store each of these as the two parts of an #ItemPointerData.
 *
 * @param row_number #size_t to convert
 * @returns #ItemPointerData the PostgreSQL represenation of a row
 */
static ItemPointerData row_number_to_item_pointer(size_t row_number) {
  ItemPointerData item_pointer = { { 0, 0 } };

  // block_number is assigned the lower 32bits of the row_number
  BlockNumber block_number = (BlockNumber)(row_number);
  // offset_number is assigned the upper 32bits of the row_number
  OffsetNumber offset_number = (OffsetNumber)(row_number >> 32);

  ItemPointerSetBlockNumber(&item_pointer, block_number);
  ItemPointerSetOffsetNumber(&item_pointer, offset_number);

  return item_pointer;
}

/**
 * @brief Convert an #ItemPointerData to a row number
 *
 * An #ItemPointerData is two 32bit values that we convert
 * back to a 64bit number.
 *
 * @param item_pointer #ItemPointerData to convert
 * @returns row_number as a #size_t
 */
static size_t item_pointer_to_row_number(ItemPointerData item_pointer) {
  BlockNumber block_number   = ItemPointerGetBlockNumber(&item_pointer);
  OffsetNumber offset_number = ItemPointerGetOffsetNumber(&item_pointer);

  // row_number is expresed as block_number for the the lower 32bits,
  // and offset_number as the top 32bits
  size_t row_number =
      size_t(size_t(block_number) | (size_t(offset_number) << 32));

  return row_number;
}

/**
 * @brief Drops a table from the #Database instance
 *
 * Deletes an instance of a #Table, freeing its memory
 * and deleting its #MemoryContext.
 *
 * @param relation #Relation to drop
 */
void Database::drop_table(Relation relation) {
  try {
    if (tables[ RelationGetRelid(relation) ] != nullptr) {
      // grab a copy of the memory context to delete separately
      MemoryContext table_memory_context =
          tables[ RelationGetRelid(relation) ]->table_memory_context;

      tables.erase(RelationGetRelid(relation));

      MemoryContextDelete(table_memory_context);
    }
  } catch (std::out_of_range error) {
    // we only care about tables we are tracking, if another table
    // is out of range, then we simply log and ignore
    ereport(DEBUG3, errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
            errmsg("unable to find table Oid %d, name %s for deletion",
                   RelationGetRelid(relation),
                   NameStr(relation->rd_rel->relname)));
  }
}

/**
 * @brief Apply any outstanding commits for a #TransactionId
 *
 * Iterates through all of the tables in the #Database and tell
 * them to commit any changes.
 *
 * @param xact #TransactionId to commit for
 */
void Database::apply_transaction_changes_commit(TransactionId xact) {
  for (auto table = tables.begin( ); table != tables.end( ); ++table) {
    if (table->second) {
      table->second->apply_transaction_changes_commit(xact);
    }
  }
}

/**
 * @brief Rollback any outstanding changes for a #TransactionId
 *
 * Iterates through all of the tables in the #Database and tell
 * them to rollback any changes.
 *
 * @param xact #TransactionId to rollback for
 */
void Database::apply_transaction_changes_rollback(TransactionId xact) {
  for (auto table = tables.begin( ); table != tables.end( ); ++table) {
    table->second->apply_transaction_changes_rollback(xact);
  }
}

/**
 * @brief Find a #Table by #Relation
 *
 * Searches the #Database by its #Relation #rel_id and returns
 * the #Table associated with it.  On error, it calls an `ereport`
 * into PostgreSQL.
 *
 * @param relation #Relation that we are searching for
 * @returns table #Table that we are searching for
 */
Table *Database::retrieve_table(Relation relation) {
  Table *table = nullptr;

  try {
    table = tables[ RelationGetRelid(relation) ];

    if (table == nullptr) {
      ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
              errmsg("unable to find table Oid %d, name %s for retrieval",
                     RelationGetRelid(relation),
                     NameStr(relation->rd_rel->relname)));
    }
  } catch (std::out_of_range error) {
    // likely a bug here if we ever hit this case, we should never try to
    // referrence a relation that does not exist or has been deleted.
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find table Oid %d, name %s for retrieval",
                   RelationGetRelid(relation),
                   NameStr(relation->rd_rel->relname)));
  }

  return table;
}

/**
 * @brief Create a table and store it in the instance of a #Database
 * by #Relation
 *
 * Creates the #Table instance that is stored in the #Database object
 * by the #Relation `rel_id`.  Also creates the #MemoryContext where
 * allocated #Datum are stored.
 *
 * @param relation of #Relation type
 */
void Database::create_table(Relation relation) {
  TupleDesc tuple_desc = RelationGetDescr(relation);
  Table *table         = new Table( );

  table->id   = RelationGetRelid(relation);
  table->name = std::string(NameStr(relation->rd_rel->relname));

  // create a memory context specifically for the table
  table->table_memory_context = AllocSetContextCreate(
      TopMemoryContext, "MemoryAM Table Storage", ALLOCSET_DEFAULT_MINSIZE,
      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

  // iterate through the columns and construct a #ColumnDefinition for each
  for (int column_number = 0; column_number < tuple_desc->natts;
       column_number++) {
    char category;
    bool is_preferred;

    // we need the category to determine if this is a composite type or not,
    // as we do not support them
    get_type_category_preferred(tuple_desc->attrs[ column_number ].atttypid,
                                &category, &is_preferred);

    if (category == TYPCATEGORY_COMPOSITE) {
      ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
              errmsg("composite types are not supported"));
    }

    // create a new #ColumnDefinition with the type information we care about
    ColumnDefinition *column_definition = new ColumnDefinition(
        tuple_desc->attrs[ column_number ].atttypid,
        tuple_desc->attrs[ column_number ].attlen,
        NameStr(tuple_desc->attrs[ column_number ].attname));
    table->column_definitions.push_back(column_definition);

    // create a vector to store each column, we reference these by row_number
    std::vector<Column *> column;
    table->rows.push_back(column);
  }

  tables[ table->id ] = table;
}

/**
 * @brief Insert a row into a #Table
 *
 * Inserts a row into the #Table, allocating the storage each #Datum
 * uses in the table's #MemoryContext.  After being appended, we also
 * set up the #Metadata and mark the row as freshly inserted in this
 * transaction by adding it to the insertion list.
 *
 * @param relation #Relation of the #Table to store the data in
 * @param values a pointer to an array of #Datum
 * @param is_nulls a pointer to an array of #bool determining if a column is
 * null
 * @returns item_pointer #ItemPointerData of the row_number stored
 */
ItemPointerData Table::insert_row(Relation relation, Datum *values,
                                  bool *is_nulls) {
  // switch to the table's MemoryContext for memory allocations
  MemoryContext original_context = MemoryContextSwitchTo(table_memory_context);

  // iterate through the columns, storing the Datum of null as we go
  for (size_t column_number = 0; column_number < column_definitions.size( );
       column_number++) {
    Column *column = new Column( );

    column->is_null = is_nulls[ column_number ];

    // if the column isn't null, then we store a copy of the Datum
    if (!column->is_null) {
      char align;
      bool by_val;
      int16_t length;

      // we need the length, and whether by value
      get_typlenbyvalalign(column_definitions[ column_number ]->type, &length,
                           &by_val, &align);

      column->by_val = by_val;
      column->length = length;

      if (column->by_val) {
        // if it is by value, we can copy its value
        column->value = values[ column_number ];
      } else {
        // otherwise we get a copy of the Datum
        column->value = datumCopy(values[ column_number ], by_val, length);
      }
    } else {
      // otherwise we rely on is_null being true, but initialize the value to 0
      column->value = 0;
    }

    // push back every column for this row
    rows[ column_number ].push_back(column);
  }

  // set up some metadata for this row, on insertion we set both
  // xmin and xmax to the current transaction ID
  RowMetadata metadata = { .xmin = GetCurrentTransactionId( ),
                           .xmax = GetCurrentTransactionId( ) };

  row_metadata.push_back(metadata);

  // get or create the transaction insert list for this transaction
  TransactionInsertList list;
  try {
    list = transaction_inserts[ GetCurrentTransactionId( ) ];
  } catch (std::out_of_range error) {
    list = TransactionInsertList( );
  }

  // and mark this row as having been inserted
  list.push_back(row_metadata.size( ) - 1);

  transaction_inserts[ GetCurrentTransactionId( ) ] = list;

  // generate the item_pointer from the new row_number
  ItemPointerData item_pointer =
      row_number_to_item_pointer(row_metadata.size( ) - 1);

  // switch back to the original memory context as we are done allocating
  MemoryContextSwitchTo(original_context);

  return item_pointer;
}

/**
 * @brief Delete a row by #ItemPointerData
 *
 * Adds the row_number to be deleted to the list of deleted row
 * in the current #TransactionId.  We reference this when we check to
 * see if a row has been deleted in transaction, and use it for committing
 * the change at the end of a transaction.
 *
 * @param item_pointer #ItemPointerData of the row to delete
 */
TM_Result Table::delete_row(ItemPointerData item_pointer) {
  size_t row_number = item_pointer_to_row_number(item_pointer);

  if (row_number > row_metadata.size( )) {
    ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("row %ld is outside of storage (%ld)", row_number,
                   row_metadata.size( )));
  }

  // if the row_number is already marked as deleted, we return `TM_Deleted`
  if ((row_metadata[ row_number ].xmax != 0 &&
       row_metadata[ row_number ].xmax <= GetCurrentTransactionId( )) ||
      row_deleted_in_transaction(row_number, GetCurrentTransactionId( ))) {
    return TM_Deleted;
  }

  // otherwise, mark the row deleted in this transaction by pushing its
  // row_number into the deleted list
  transaction_deletes[ GetCurrentTransactionId( ) ].push_back(row_number);

  return TM_Ok;
}

/**
 * @brief Check to see if a row has been deleted in a #TransactionId
 *
 * Iterates through the list of deleted row_numbers for a transaction
 * and returns whether the row_number passed is in the list.
 *
 * @param row_number #size_t of the row_number to check
 * @param xact #TransactionId to check in
 * @returns whether the row has been deleted as a #bool
 */
bool Table::row_deleted_in_transaction(size_t row_number, TransactionId xact) {
  TransactionDeleteList list;
  try {
    list = transaction_deletes[ xact ];
  } catch (std::out_of_range error) {
    // no local changes for this transaction, so we are complete and can
    // return false
    return false;
  }

  for (size_t row = 0; row < list.size( ); row++) {
    if (list[ row ] == row_number) {
      // the row_number is in this transaction so return true
      return true;
    }
  }

  return false;
}

/**
 * @brief Check to see if a row is visible in a snapshot
 *
 * Looks at the snapshot xmin and xmax to determine whether a row
 * is considered deleted.
 *
 * @param item_pointer #ItemPointerData to the row to be checked
 * @param snapshot #Snapshot to check against
 * @returns whether the row is visible as a #bool
 */
bool Table::row_visible_in_snapshot(ItemPointerData item_pointer,
                                    Snapshot snapshot) {
  size_t row_number = item_pointer_to_row_number(item_pointer);

  bool visible = false;

  if (row_number >= row_metadata.size( )) {
    ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Row %ld is outside the range for transaction", row_number));
  }

  RowMetadata row = row_metadata[ row_number ];

  // a row is considered visible if its xmin is less than or equal to the
  // snapshot xmin, its xmax is greater than or equal to the snapshot xmax,
  // and the row has not been marked deleted in the current transaction
  visible = (snapshot->xmin == 0 || row.xmin <= snapshot->xmin) &&
            (row.xmax == 0 || row.xmax >= snapshot->xmax) &&
            !row_deleted_in_transaction(row_number, GetCurrentTransactionId( ));

  return visible;
}

/**
 * @brief Reads a row by item_pointer #ItemPointerData and xact #TransactionId
 * and if it is visible fill the slot with data
 *
 * Checks to see if a row is visible, and if it is then fill the slot with
 * the columns needed for the row that is read.
 *
 * @param item_pointer #ItemPointerData to read
 * @param xact #TransactionId that we need to find
 * @param slot #TupleTableSlot to fill with the data
 * @param needed_column #std::vector of the columns needed
 * @returns whether the row can be read #bool
 */
bool Table::read_row(ItemPointerData item_pointer, TransactionId xact,
                     TupleTableSlot *slot,
                     std::vector<AttrNumber> needed_columns) {
  // if the xact is set to 0, then we set it to the current #TransactionId
  if (xact == 0) {
    xact = GetCurrentTransactionId( );
  }

  // if the row isn't visible, we can simply return false
  if (!row_visible_in_snapshot(item_pointer, GetActiveSnapshot( ))) {
    return false;
  }

  size_t row_number = item_pointer_to_row_number(item_pointer);

  // if the row_number is higher than the number of rows in our metadata,
  // then we also return false
  if (row_number >= row_metadata.size( )) {
    return false;
  }

  // make a std::vector of columns to add to the slot
  Columns columns;

  // iterate through the needed columns and add them to the columns to be
  // returned
  for (size_t iterator = 0; iterator < needed_columns.size( ); iterator++) {
    AttrNumber column_number = needed_columns[ iterator ];
    columns.push_back(rows[ column_number ][ row_number ]);
  }

  // copy the columns to the slot
  copy_columns_to_slot(columns, slot, needed_columns);

  // set the item pointer for the slot
  slot->tts_tid = item_pointer;

  return true;
}

/**
 * @brief Retrieves the next value from a table scan, filling the slot
 * provided
 *
 * Iterates to the next row that is visible, incrementing the row_number
 * until a row can be read into a slot #TupleTableSlot.  If a read can
 * occur then returns `true`, or returns `false` if we are at the end of
 * any possible scans.
 *
 * @param scan #MemoryScanDesc the current scan to use
 * @param slot #TupleTableSlot the slot to fill for the scan
 * @returns whether the next row can be read indicating whether we are done as
 * #bool
 */
bool Table::next_value(MemoryScanDesc *scan, TupleTableSlot *slot) {
  Columns columns;
  bool valid = false;

  do {
    // if we've hit the end of the row_metadata, we are done
    if (scan->cursor >= row_metadata.size( )) {
      return false;
    } else {
      // check to see if the next row is visible
      if (row_metadata[ scan->cursor ].xmin <= GetCurrentTransactionId( ) &&
          (row_metadata[ scan->cursor ].xmax == 0 ||
           row_metadata[ scan->cursor ].xmax >= GetCurrentTransactionId( )) &&
          !row_deleted_in_transaction(scan->cursor,
                                      GetCurrentTransactionId( ))) {

        // iterate through the column numbers and push back each column
        for (size_t column_number = 0;
             column_number < column_definitions.size( ); column_number++) {
          columns.push_back(rows[ column_number ][ scan->cursor ]);
        }

        // we've found a valid row so can exit the loop
        valid = true;
      }
    }

    scan->cursor++;
  } while (!valid);

  // copy all ofthe columns into the slot
  copy_columns_to_slot(columns, slot, scan->needed_columns);

  // and set the column's item pointer to the row
  slot->tts_tid = row_number_to_item_pointer(scan->cursor - 1);

  return true;
}

/**
 * @brief Get the number of deleted rows for a xact #TransactionId
 *
 * Iterates through any deleted rows for a given #TransactionId and returns
 * the total number or 0 if there are none.
 *
 * @param xact #TransactionId to check
 * @returns the number of deleted rows for the transaction as #size_t
 */
size_t Table::deleted_count_for_transaction(TransactionId xact) {
  size_t deleted_count = 0;

  for (size_t row_number = 0; row_number < row_metadata.size( ); row_number++) {
    if (row_metadata[ row_number ].xmax != 0 &&
        row_metadata[ row_number ].xmax <= xact) {
      deleted_count++;
    }
  }

  return deleted_count;
}

/**
 * @brief Returns the total number of open transactions for a table
 */
size_t Table::transaction_count( ) {
  std::unordered_map<TransactionId, size_t> transactions;

  try {
    for (const auto &[ key, value ] : transaction_inserts) {
      transactions[ key ] = 1;
    }
  } catch (std::out_of_range error) {
  }

  try {
    for (const auto &[ key, value ] : transaction_deletes) {
      transactions[ key ] = 1;
    }
  } catch (std::out_of_range error) {
  }

  return transactions.size( );
}

/**
 * @brief Apply all of the changes for a #TransactionId as a commit
 *
 * Iterates through the transactional changes of inserts and deletes
 * and act on them as needed.  Note that this is idempotent, so it is
 * ok to call it repeatedly.
 *
 * @param xact #TransactionId of the transaction to commit
 */
void Table::apply_transaction_changes_commit(TransactionId xact) {
  TransactionInsertList insert_list;
  bool handle_inserts = false;

  // if there is a list of insertions we should process them
  try {
    insert_list    = transaction_inserts[ xact ];
    handle_inserts = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No insertions to commit");
  }

  if (handle_inserts && insert_list.size( )) {
    // iterate through the insertions and set the xmax for each row
    // to 0 (ready to be read by any transaction)
    for (size_t change_row = 0; change_row < insert_list.size( );
         change_row++) {
      size_t row_number               = insert_list[ change_row ];
      row_metadata[ row_number ].xmax = 0;
    }
  }

  TransactionDeleteList delete_list;
  bool handle_deletes = false;

  // if there is a list of deletions we should process them
  try {
    delete_list    = transaction_deletes[ xact ];
    handle_deletes = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No deletions to commit");
  }

  if (handle_deletes) {
    // iterate through the deletions and update any row metadata to
    // set the xmax to the current transaction
    for (size_t change_row = 0; change_row < delete_list.size( );
         change_row++) {
      size_t row_number               = delete_list[ change_row ];
      row_metadata[ row_number ].xmax = xact;
    }
  }

  // clean up any changes for this transaction
  delete_changes_for_transaction(xact);
}

/**
 * @brief Rollback all of the changes for a #TransactionId
 *
 * Iterates through the transactional changes of inserts and act on
 * them as needed. We can safely ignore deletes as we didn't modify
 * any rows on a delete.  This is an idempotent function, calling it
 * repeatedly is acceptable.
 *
 * @param xact #TransactionId of the transaction to rolback
 */
void Table::apply_transaction_changes_rollback(TransactionId xact) {
  TransactionInsertList insert_list;
  bool handle_inserts = false;

  try {
    insert_list    = transaction_inserts[ xact ];
    handle_inserts = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No insertions to commit");
  }

  // if there are inserts, we need to iterate through them in reverse and
  // delete the row metadata and the columns associated with a row number
  if (handle_inserts && insert_list.size( )) {
    for (long change_row = insert_list.size( ) - 1; change_row >= 0;
         change_row--) {
      size_t row_number = insert_list[ change_row ];
      row_metadata.erase(row_metadata.begin( ) + row_number);

      for (size_t column_number = 0; column_number < column_definitions.size( );
           column_number++) {
        rows[ column_number ].erase(rows[ column_number ].begin( ) +
                                    row_number);
      }
    }
  }

  // clean up the lists for the transaction
  delete_changes_for_transaction(xact);
}

/**
 * @brief Delete all outstanding changes for a given #TransactionId
 *
 * Deletes the changes lists for a given #TransactionId if they exist.
 * @param xact #TransactionId to delete
 */
void Table::delete_changes_for_transaction(TransactionId xact) {
  try {
    transaction_inserts.erase(xact);
  } catch (std::out_of_range) {
  }

  try {
    transaction_deletes.erase(xact);
  } catch (std::out_of_range) {
  }
}

/**
 * @brief Copy the columns specified in the needed columns into the provided
 * slot #TupleTableSlot
 *
 * Makes a direct copy of each column into the provided #TupleTableSlot,
 * making a copy of any #Datum in the current memory context, andsets the
 * number of valid attributes for the slot.
 *
 * @param columns #Columns that we are copying
 * @param slot #TupleTableSlot that we are copying into
 * @param needed_columns #std::vector of the columns to copy
 */
void Table::copy_columns_to_slot(Columns columns, TupleTableSlot *slot,
                                 std::vector<AttrNumber> needed_columns) {
  AttrNumber storage_cursor = 0;

  for (AttrNumber column_number : needed_columns) {
    Column *column = columns[ column_number ];

    if (column == nullptr) {
      ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
              errmsg("Unable to retrieve column %d of row", column_number));
    }

    slot->tts_isnull[ storage_cursor ] = column->is_null;

    if (!column->is_null) {
      slot->tts_values[ storage_cursor ] =
          datumCopy(column->value, column->by_val, column->length);
    } else {
      slot->tts_values[ storage_cursor ] = 0;
    }

    storage_cursor++;
  }

  slot->tts_nvalid = storage_cursor;

  if (TTS_EMPTY(slot)) {
    ExecStoreVirtualTuple(slot);
  }
}

/**
 * @brief Deallocate any memory used for a #Column
 */
Column::~Column( ) {
  if (!by_val) {
    pfree(DatumGetPointer(value));
  }
}
