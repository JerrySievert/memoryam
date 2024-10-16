#pragma once

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "access/tableam.h"
#include "c.h"
#include "executor/tuptable.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
}

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

struct MemoryScanDesc {
  TableScanDescData rs_base;
  size_t cursor;
  std::vector<AttrNumber> needed_columns;
};

struct Column {
  ~Column( );
  bool is_null;
  bool by_val;
  uint16_t length;
  Datum value;
};

struct ColumnDefinition {
  ColumnDefinition(Oid type, uint16_t length, const char *name) {
    this->type   = type;
    this->length = length;
    this->name   = std::string(name);
  }

  Oid type;
  uint16_t length;
  std::string name;
};

struct RowMetadata {
  TransactionId xmin;
  TransactionId xmax;
};

typedef std::vector<Column *> Columns;

struct TransactionInsert {
  TransactionId xmin;
  TransactionId xmax;
  SubTransactionId subxact;
};

struct TransactionDelete {
  TransactionId xmax;
  SubTransactionId subxact;
  size_t row_number;
  bool in_local_changes;
};

typedef std::vector<size_t> TransactionInsertList;
typedef std::vector<size_t> TransactionDeleteList;

struct Table {
  Oid id;
  std::string name;
  std::vector<Columns> rows;
  std::vector<ColumnDefinition *> column_definitions;
  std::vector<RowMetadata> row_metadata;
  std::unordered_map<TransactionId, TransactionInsertList> transaction_inserts;
  std::unordered_map<TransactionId, TransactionDeleteList> transaction_deletes;

  ItemPointerData insert_row(Relation relation, Datum *values, bool *is_nulls);
  Datum *retrieve_column_by_row(size_t row, size_t column);
  TM_Result delete_row(Relation relation, ItemPointerData item_pointer);
  void apply_transaction_changes_commit(TransactionId xact);
  void apply_transaction_changes_rollback(TransactionId xact);
  bool next_value(MemoryScanDesc *scan, TupleTableSlot *slot);
  bool row_deleted_in_transaction(size_t row_number, TransactionId xact);
  bool row_visible_in_snapshot(ItemPointerData item_pointer, Snapshot snapshot);
  size_t deleted_count_for_transaction(TransactionId xact);
  size_t transaction_count( );
  bool read_row(ItemPointerData item_pointer, TransactionId xact,
                TupleTableSlot *slot, std::vector<AttrNumber> needed_columns);

  void debug_row(size_t);

private:
  void copy_columns_to_slot(Columns columns, TupleTableSlot *slot,
                            std::vector<AttrNumber> attributes);
  void delete_changes_for_transaction(TransactionId xact);
};

struct Database {
  std::unordered_map<RelFileNumber, Table *> tables;

  void drop_table(Relation relation);
  void create_table(Relation relation);
  Table *retrieve_table(Relation relation);
  void apply_transaction_changes_commit(TransactionId xact);
  void apply_transaction_changes_rollback(TransactionId xact);

  MemoryContext memory_context;
};

extern Database database;

extern ItemPointerData row_number_to_item_pointer(size_t row_number);
extern size_t item_pointer_to_row_number(ItemPointerData item_pointer);
