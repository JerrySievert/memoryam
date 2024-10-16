#include <iterator>
#include <stdexcept>
#include <unordered_map>

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "varatt.h"
}

#include "memoryam.hpp"
#include "store.hpp"

ItemPointerData row_number_to_item_pointer(size_t row_number) {
  ItemPointerData item_pointer = { { 0, 0 } };

  BlockNumber block_number = (BlockNumber)(row_number);
  ItemPointerSetBlockNumber(&item_pointer, block_number);
  ItemPointerSetOffsetNumber(&item_pointer, 0);

  return item_pointer;
}

size_t item_pointer_to_row_number(ItemPointerData item_pointer) {
  BlockNumber block_number = ItemPointerGetBlockNumber(&item_pointer);

  size_t row_number = size_t(block_number);

  return row_number;
}

void Database::drop_table(Relation relation) {
  if (database.tables[ RelationGetRelid(relation) ] != nullptr) {
    database.tables.erase(RelationGetRelid(relation));
  }
}

Table *Database::retrieve_table(Relation relation) {
  Table *table = database.tables[ RelationGetRelid(relation) ];

  if (table == nullptr) {
    ereport(ERROR, errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("unable to find table Oid %d, name %s",
                   RelationGetRelid(relation),
                   NameStr(relation->rd_rel->relname)));
  }

  return table;
}

void Database::create_table(Relation relation) {
  TupleDesc tuple_desc = RelationGetDescr(relation);
  Table *table         = new Table( );

  table->id   = RelationGetRelid(relation);
  table->name = std::string(NameStr(relation->rd_rel->relname));

  for (int i = 0; i < tuple_desc->natts; i++) {
    char category;
    bool is_preferred;

    get_type_category_preferred(tuple_desc->attrs[ i ].atttypid, &category,
                                &is_preferred);

    if (category == TYPCATEGORY_COMPOSITE) {
      ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
              errmsg("composite types are not supported"));
    }

    ColumnDefinition *column_definition = new ColumnDefinition(
        tuple_desc->attrs[ i ].atttypid, tuple_desc->attrs[ i ].attlen,
        NameStr(tuple_desc->attrs[ i ].attname));
    table->column_definitions.push_back(column_definition);

    std::vector<Column *> column;
    table->rows.push_back(column);
  }

  database.tables[ table->id ] = table;
}

ItemPointerData Table::insert_row(Relation relation, Datum *values,
                                  bool *is_nulls) {
  TupleDesc tuple_desc = RelationGetDescr(relation);

  MemoryContext original_context =
      MemoryContextSwitchTo(database.memory_context);
  std::vector<Column *> row;

  for (size_t i = 0; i < column_definitions.size( ); i++) {
    Column *column = new Column( );

    column->is_null = is_nulls[ i ];
    if (!column->is_null) {
      char align, category;
      bool by_val, is_preferred;
      int16_t length;

      get_typlenbyvalalign(column_definitions[ i ]->type, &length, &by_val,
                           &align);

      get_type_category_preferred(tuple_desc->attrs[ i ].atttypid, &category,
                                  &is_preferred);

      column->by_val = by_val;
      column->length = length;

      if (column->by_val) {
        column->value = values[ i ];
      } else {
        column->value = datumCopy(values[ i ], by_val, length);
      }
    }

    /* Push back every column for this row. */
    rows[ i ].push_back(column);
  }

  /* Write the metadata, to be dealt with at the end of the transaction. */
  RowMetadata metadata = { .xmin = GetCurrentTransactionId( ),
                           .xmax = GetCurrentTransactionId( ) };

  row_metadata.push_back(metadata);

  /* Get or create the transaction insert list for this transaction. */
  TransactionInsertList list;
  try {
    list = transaction_inserts[ GetCurrentTransactionId( ) ];
  } catch (std::out_of_range error) {
    list = TransactionInsertList( );
  }

  list.push_back(row_metadata.size( ) - 1);

  transaction_inserts[ GetCurrentTransactionId( ) ] = list;

  ItemPointerData item_pointer =
      row_number_to_item_pointer(row_metadata.size( ) - 1);

  size_t row_number = item_pointer_to_row_number(item_pointer);

  MemoryContextSwitchTo(original_context);
  // debug_row(row_number);
  return item_pointer;
}

TM_Result Table::delete_row(Relation relation, ItemPointerData item_pointer) {
  size_t row_number = item_pointer_to_row_number(item_pointer);

  if (row_number > row_metadata.size( )) {
    ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("row %ld is outside of storage (%ld)", row_number,
                   row_metadata.size( )));
  }

  if ((row_metadata[ row_number ].xmax != 0 &&
       row_metadata[ row_number ].xmax <= GetCurrentTransactionId( )) ||
      row_deleted_in_transaction(row_number, GetCurrentTransactionId( ))) {
    return TM_Deleted;
  }

  /* Otherwise, mark the row deleted in this transaction. */
  transaction_deletes[ GetCurrentTransactionId( ) ].push_back(row_number);

  return TM_Ok;
}

bool Table::row_deleted_in_transaction(size_t row_number, TransactionId xact) {
  TransactionDeleteList list;
  try {
    list = transaction_deletes[ xact ];
  } catch (std::out_of_range error) {
    /* No local changes for this transaction, so we are complete and can
     * return false. */
    return false;
  }

  for (size_t row = 0; row < list.size( ); row++) {
    if (list[ row ] == row_number) {
      return true;
    }
  }

  return false;
}

bool Table::row_visible_in_snapshot(ItemPointerData item_pointer,
                                    Snapshot snapshot) {
  DEBUG( );
  bool in_transaction_changes = (item_pointer.ip_posid & (1 << 15));

  size_t row_number = item_pointer_to_row_number(item_pointer);

  bool visible = false;

  if (row_number >= row_metadata.size( )) {
    ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Row %ld is outside the range for transaction", row_number));
  }

  RowMetadata row = row_metadata[ row_number ];

  visible = (snapshot->xmin == 0 || row.xmin <= snapshot->xmin) &&
            (row.xmax == 0 || row.xmax >= snapshot->xmax);

  try {
    TransactionDeleteList list =
        transaction_deletes[ GetCurrentTransactionId( ) ];

    for (size_t i = 0; i < list.size( ); i++) {
      if (list[ i ] == row_number) {
        visible = false;
        break;
      }
    }
  } catch (std::out_of_range) {
    // nothing to do here, can be ignored.
  }

  return visible;
}

char *datum_to_string(Datum value) {
  char *str = (char *)palloc(VARSIZE_ANY_EXHDR(value) + 1);
  memcpy(str, (char *)VARDATA(value), VARSIZE_ANY_EXHDR(value));
  str[ VARSIZE_ANY_EXHDR(value) ] = '\0';

  return str;
}

void Table::debug_row(size_t row_number) {
  std::string output = "---------------------------------------------------\n";
  output += "row_mumber: " + std::to_string(row_number) + "\n";
  output +=
      "xmin:       " + std::to_string(row_metadata[ row_number ].xmin) + "\n";
  output +=
      "xmax:       " + std::to_string(row_metadata[ row_number ].xmax) + "\n";
  output += "columns:\n";
  output += "  1 => " + std::to_string(rows[ 0 ][ row_number ]->value) + "\n";
  output += "  2 => " +
            std::string(datum_to_string(rows[ 1 ][ row_number ]->value)) + "\n";
  output += "---------------------------------------------------\n";
  elog(NOTICE, "%s", output.c_str( ));
}

bool Table::read_row(ItemPointerData item_pointer, TransactionId xact,
                     TupleTableSlot *slot,
                     std::vector<AttrNumber> needed_columns) {
  /* If we are here, then we've already decided that the row is viable. */
  DEBUG( );
  bool in_transaction_changes = (item_pointer.ip_posid & (1 << 15));

  if (xact == 0) {
    xact = GetCurrentTransactionId( );
  }

  if (!row_visible_in_snapshot(item_pointer, GetActiveSnapshot( ))) {
    return false;
  }

  size_t row_number = item_pointer_to_row_number(item_pointer);

  Columns columns;

  if (row_number >= row_metadata.size( )) {
    return false;
  }

  for (size_t iterator = 0; iterator < needed_columns.size( ); iterator++) {
    AttrNumber column_number = needed_columns[ iterator ];
    columns.push_back(rows[ column_number ][ row_number ]);
  }

  // debug_row(row_number);
  copy_columns_to_slot(columns, slot, needed_columns);
  slot->tts_tid = item_pointer;

  return true;
}

bool Table::next_value(MemoryScanDesc *scan, TupleTableSlot *slot) {
  DEBUG( );
  Columns columns;
  bool valid = false;

  do {
    if (scan->cursor >= row_metadata.size( )) {
      return false;
    } else {
      /* Check to see if the next row is visible. */
      if (row_metadata[ scan->cursor ].xmin <= GetCurrentTransactionId( ) &&
          (row_metadata[ scan->cursor ].xmax == 0 ||
           row_metadata[ scan->cursor ].xmax >= GetCurrentTransactionId( )) &&
          !row_deleted_in_transaction(scan->cursor,
                                      GetCurrentTransactionId( ))) {
        Columns cols;

        for (size_t column_number = 0;
             column_number < column_definitions.size( ); column_number++) {
          cols.push_back(rows[ column_number ][ scan->cursor ]);
        }
        columns = cols;
        valid   = true;
      }
    }

    scan->cursor++;
  } while (!valid);

  copy_columns_to_slot(columns, slot, scan->needed_columns);

  slot->tts_tid = row_number_to_item_pointer(scan->cursor - 1);

  return true;
}

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

void Table::apply_transaction_changes_commit(TransactionId xact) {
  TransactionInsertList insert_list;
  bool handle_inserts = false;

  try {
    insert_list    = transaction_inserts[ xact ];
    handle_inserts = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No insertions to commit");
  }

  if (handle_inserts && insert_list.size( )) {
    for (size_t change_row = 0; change_row < insert_list.size( );
         change_row++) {
      size_t row_number               = insert_list[ change_row ];
      row_metadata[ row_number ].xmax = 0;
    }
  }

  TransactionDeleteList delete_list;
  bool handle_deletes = false;

  try {
    delete_list    = transaction_deletes[ xact ];
    handle_deletes = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No deletions to commit");
  }

  if (handle_deletes) {
    for (size_t change_row = 0; change_row < delete_list.size( );
         change_row++) {
      size_t row_number               = delete_list[ change_row ];
      row_metadata[ row_number ].xmax = xact;
    }
  }

  delete_changes_for_transaction(xact);
}

void Table::apply_transaction_changes_rollback(TransactionId xact) {
  TransactionInsertList insert_list;
  bool handle_inserts = false;

  try {
    insert_list    = transaction_inserts[ xact ];
    handle_inserts = true;
  } catch (std::out_of_range error) {
    elog(DEBUG3, "No insertions to commit");
  }

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

  delete_changes_for_transaction(xact);
}

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

void Table::copy_columns_to_slot(Columns columns, TupleTableSlot *slot,
                                 std::vector<AttrNumber> attributes) {
  DEBUG( );
  // ExecClearTuple(slot);

  AttrNumber storage_cursor = 0;

  for (AttrNumber attribute : attributes) {
    Column *column = columns[ attribute ];

    if (column == nullptr) {
      ereport(ERROR, errcode(ERRCODE_INTERNAL_ERROR),
              errmsg("Unable to retrieve column %d of row", attribute));
    }

    slot->tts_isnull[ storage_cursor ] = column->is_null;

    if (!column->is_null) {
      slot->tts_values[ storage_cursor ] =
          datumCopy(column->value, column->by_val, column->length);
    }

    storage_cursor++;
  }
  slot->tts_nvalid = storage_cursor;

  // if (TTS_EMPTY(slot)) {
  ExecStoreVirtualTuple(slot);
  //}
}

Column::~Column( ) {
  if (!by_val) {
    pfree(DatumGetPointer(value));
  }
}

void Database::apply_transaction_changes_commit(TransactionId xact) {
  for (auto it = tables.begin( ); it != tables.end( ); ++it) {
    it->second->apply_transaction_changes_commit(xact);
  }
}

void Database::apply_transaction_changes_rollback(TransactionId xact) {
  for (auto it = tables.begin( ); it != tables.end( ); ++it) {
    it->second->apply_transaction_changes_rollback(xact);
  }
}
