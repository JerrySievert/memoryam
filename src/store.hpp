#pragma once

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "access/tableam.h"
#include "c.h"
#include "executor/tuptable.h"
}

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief The container that holds descriptor for the current
 * state of a table scan
 */
struct MemoryScanDesc {
  /**
   * @brief The base descriptor for the table scan, first element so it can
   * addressed directly
   */
  TableScanDescData rs_base;
  /**
   * @brief Current position of the table scan
   */
  size_t cursor;
  /**
   * @brief List of which columns we return as part of a table scan
   */
  std::vector<AttrNumber> needed_columns;
};

/**
 * @brief The container for a column, with the metadata we care about
 * and the value itself (or null)
 */
struct Column {
  ~Column( );
  /**
   * @brief Whether the value is null or not
   */
  bool is_null;
  /*
   * @brief Whether the value is by value, otherwise it is a reference that
   * needs to be copied
   */
  bool by_val;
  /*
   * @brief The length of the value if it is by reference
   */
  uint16_t length;
  /*
   * @brief The value itself, either by value or as a pointer to a memory
   * location that has been allocated in a memory context
   */
  Datum value;
};

/**
 * @brief The definition of a PostgreSQL column as stored in a #Table
 */
struct ColumnDefinition {
  ColumnDefinition(Oid type, uint16_t length, const char *name) {
    this->type   = type;
    this->length = length;
    this->name   = std::string(name);
  }

  /**
   * @brief The #Oid type of the #Datum that will be stored
   */
  Oid type;
  /**
   * @brief The length of the #Datum if applicable
   */
  uint16_t length;
  /**
   * @brief The column name as a #std::string
   */
  std::string name;
};

/**
 * @brief An object containing the transaction minimum and transaction
 * maximum for this row.  From there, we can calculate the visibility for
 * a transaction based MVCC compatible with HEAP tables.
 */
struct RowMetadata {
  /**
   * @brief The minimal #TransactionId that the row is visible to
   */
  TransactionId xmin;
  /**
   * @brief The maximum #TransactionId that the row is visible to
   */
  TransactionId xmax;
};

/**
 * @brief A #std::vector of #Column, with eacb index being a row_number
 */
typedef std::vector<Column *> Columns;

/**
 * @brief A #std::vector of row_numbers that have been inserted, these are
 * stored by #TransactionId
 */
typedef std::vector<size_t> TransactionInsertList;

/**
 * @brief A #std::vector of row_numbers that have been deleted, these are
 * stored by #TransactionId
 */
typedef std::vector<size_t> TransactionDeleteList;

/**
 * @brief In-memory representation of a Table encompassing the
 * #ColumnDefinition, #RowMetadata, and #Columns variables
 */
struct Table {
  /**
   * @brief #Oid of the #Relation being stored
   */
  Oid id;
  /**
   * @brief name of the #Relation that we are storing
   */
  std::string name;
  /**
   * @brief #std::vector of #Columns where actual data is stored
   */
  std::vector<Columns> rows;
  /**
   * @brief #std::vector of #ColumnDefinition that define each column
   */
  std::vector<ColumnDefinition *> column_definitions;
  /**
   * @brief #std::vector of #RowMetadata tracking the visibility of every row
   */
  std::vector<RowMetadata> row_metadata;
  /**
   * @brief #std::unordered_map of transaction inserts by #TransactionId
   */
  std::unordered_map<TransactionId, TransactionInsertList> transaction_inserts;
  /**
   * @brief #std::unordered_map of transaction deletes by #TransactionId
   */
  std::unordered_map<TransactionId, TransactionDeleteList> transaction_deletes;
  /**
   * @brief #MemoryContext that table allocations are done in
   */
  MemoryContext table_memory_context;

  ItemPointerData insert_row(Relation relation, Datum *values, bool *is_nulls);
  Datum *retrieve_column_by_row(size_t row, size_t column);
  TM_Result delete_row(ItemPointerData item_pointer);
  void apply_transaction_changes_commit(TransactionId xact);
  void apply_transaction_changes_rollback(TransactionId xact);
  bool next_value(MemoryScanDesc *scan, TupleTableSlot *slot);
  bool row_deleted_in_transaction(size_t row_number, TransactionId xact);
  bool row_visible_in_snapshot(ItemPointerData item_pointer, Snapshot snapshot);
  size_t deleted_count_for_transaction(TransactionId xact);
  size_t transaction_count( );
  bool read_row(ItemPointerData item_pointer, TransactionId xact,
                TupleTableSlot *slot, std::vector<AttrNumber> needed_columns);

private:
  void copy_columns_to_slot(Columns columns, TupleTableSlot *slot,
                            std::vector<AttrNumber> needed_columns);
  void delete_changes_for_transaction(TransactionId xact);
};

/**
 * @brief #Database to store all #Table objects that are active during a
 * session.  #Table objects are accessible by #Oid of the #Relation
 */
struct Database {
  std::unordered_map<Oid, Table *> tables;

  void drop_table(Relation relation);
  void create_table(Relation relation);
  Table *retrieve_table(Relation relation);
  void apply_transaction_changes_commit(TransactionId xact);
  void apply_transaction_changes_rollback(TransactionId xact);
};

extern Database database;
