# An Overview of MemoryAM's Storage Engine

The storage engine that MemoryAM uses is extremely simple, supporting the basics of reading from and writing
to a table. It does include basic transaction isolation.

The storage engine is written in C++ to simplify hash tables, vectors, and other in-memory storage mechanisms.
This is not completely necessary, but it does make the code easier to read.

## Basic Principles

We store tables as temporary tables in memory for a single PostgreSQL connection/process. This allows
for us to not need to worry about syncronizing between multiple backends, nor worry about WAL, disk storage,
or full isolation.

The storage engine attempts to gather what information it needs, so that calls into PostgreSQL itself are
minimized. It is likely that any calls back into PostgreSQL can be eliminated completely to completely
decouple all storage from compute.

### Storing

The top-level storage is a `Database` class. It is responsible for `Table` creation and deletion, as
well as applying any changes when a transaction is committed or rolled back.

A `Table` consists of a unique identifier, a list of columns, `RowMetadata`, a list of columns, and a
list of transactional changes for transaction isolation and MVCC support.

### MVCC

The storage engine supports basic MVCC by storing `RowMetadata` that consists of the minimum transaction
and maximum transaction for each row. This provides some basic visibility to allow for multiple versions
of the same row to exist, but to isolate these at a transaction level.

### Transactions

Transactions are isolated by creating a list of transactions by transaction ID for each `Table`. These lists
are separated into insertion lists and deletion lists. Upon disposition of the transaction (commit or
rollback), these lists are iterated and any transactional changes are either discarded or committed.
