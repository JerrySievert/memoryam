# Lifecycle of a MemoryAM Temporary Table in PostgreSQL

MemoryAM is designed as a temporary table storage. This gives us some
additional freedoms that we would not be afforded if we were implementing
a full TableAM implementation, including no need to be stored on disk,
and no need to handle simultaneous reads and writes.

In addition, being all in memory allows us to make certain design decisions
that greatly simplify development and implementation, such as not
supporting indexes.

## Extension Creation

```SQL
CREATE EXTENSION memoryam;
```

While extension creation starts for the user as soon as they make the
call to `CREATE EXTENSION`, its lifecycle actually starts when the
dynamic shared library of our extension is first loaded into PostgreSQL's
memory, and specifically when our `_PG_init()` function is first called.
It is at this point that we set up any hooks and callbacks that are
provided: in this case the `XactCallback`.

```C
// register our transaction callback
RegisterXactCallback(memoryam_xact_callback, NULL);
```

During extension creation, our transaction callback is called twice,
the first with the `XACT_EVENT_PRE_COMMIT` `XactEvent`, which we ignore in
all cases.

```C
case XACT_EVENT_PRE_COMMIT:
case XACT_EVENT_PARALLEL_PRE_COMMIT:
case XACT_EVENT_PRE_PREPARE: {
  // nothing to do in this case
  break;
}
```

The second callback is called with the event `XACT_EVENT_COMMIT`, which
we generally act on by applying any outstanding changes in the current
transaction. Since at this point there are no tables and thus no
transactions, this does nothing harmful nor helpful.

```C
case XACT_EVENT_COMMIT:
case XACT_EVENT_PARALLEL_COMMIT:
case XACT_EVENT_PREPARE: {
  database.apply_transaction_changes_commit(GetCurrentTransactionId( ));
  break;
}
```

## Table Creation

Once the extension has been registered, the next step in the lifecycle is
to create a table, in our case a temporary table using `memoryam`.

```SQL
CREATE TEMPORARY TABLE t1 (i int, t text) USING memoryam;
```

Doing this gives us our first call into our TableAM implementation:

```C
static void memoryam_relation_set_new_filelocator(
    Relation relation, const RelFileLocator *newrlocator, char persistence,
    TransactionId *freeze_xid, MultiXactId *minmulti)
```

This is where we do a basic sanity check, making sure it's a temporary
table, checking to see if there any older instances and telling the
storage engine to create a new table.

As PostgreSQL steps through the rest of the table creation process,
our `memoryam_object_access_hook` that we set up during `_PG_init()`
gets called with a `OAT_POST_CREATE` `ObjectAccessType`.

```C
// set up our object access hook, we only care for OAT_DROP
prev_ObjectAccessHook = object_access_hook;
object_access_hook    = memoryam_object_access_hook;
```

We can safely ignore these calls as the only reason why we are hooked
into this call is to make sure that we drop tables correctly.

Our next call is to `memoryam_relation_needs_toast_table`, which is
always `false` in our case. We manage our own storage.

Finally, we have to manage the final disposition of the table. If
we were called outside of a transaction or a `COMMIT` of the transaction
that we are created in is called, then we receive a callback to
`memoryam_xact_callback` of types `XACT_EVENT_PRE_COMMIT`, which we
can safely ignore, and `XACT_EVENT_COMMIT`, at which point we apply any
changes from the transaction by calling `apply_transaction_changes_commit`.

Alternately, if the transaction is rolled back, such as via `ROLLBACK`
being called, then our `memoryam_xact_callback` gets called with
`XACT_EVENT_ABORT` which means that we can delete our open transaction
if there is one by calling `apply_transaction_changes_rollback`.

## Table Insertion

A table insertion is generally via an `INSERT` statement.

```SQL
INSERT INTO t1 (i, t) VALUES (1, 'hello');
```

This first calls our `memoryam_slot_callbacks`, which simply returns that
we are essentially a virtual tuple containing a `Datum` and a field marking
whether the column is null or not.

After this, our insertion function `memoryam_tuple_insert` is called,
with a copy of the data to insert. From here we call the storage engine's
`insert_row` method for the table. This inserts the current row to be ready
for insertion into the current transaction. We return by setting the
#TupleTableSlot for the tuple and setting its #ItemPointerData to point
to the newly inserted row.

The `ExecutorFinish` and `ExecutorEnd` hooks are then called, which we
safely ignore at this point, followed by the `commit` or `rollback` calls
into `memoryam_xact_callback` which allows us to act on the changes correctly.

## Querying the Data

Adding data to our storage is only useful if we are able to view and analyze
that data. Let's look at the patch of a simple `SELECT`:

```SQL
SELECT * FROM t1;
```

Our query begins with a call to `memoryam_estimate_rel_size`, which is, as
named, a method to get details about the relation size. This is followed by
`memoryam_slot_callbacks`, and finally `memoryam_begin_scan` to set up the
scan itself.

Moving forward, we have calls into `memoryam_getnextslot`, which calls the
storage engine's `next_value` method. This will check the visibility of each
row, and when a row is visible, it will read a copy of the row into the
provided `TupleTableSlot` and return `true`. If no more rows are visible
or can be read, then `false` is returned, telling PostgreSQL that the scan is
complete.

Upon completion of the scan, the `ExecutorFinish` and `ExecutorEnd` hooks
are called, followed by `memoryam_end_scan` which cleans up any memory
allocated for the scan, including the `MemoryScanDesc` that was allocated
and filled during the call to `memoryam_begin_scan`.

As with other calls, transaction disposition occurs, which handles any
changes made during the transaction.

## Deletions

Being able to insert and view data is great, but what about deletions?

```SQL
DELETE FROM t1 WHERE i = 1;
```

As with a `SELECT`, the query begins with `memoryam_estimate_rel_size`, which
returns an approximation, then follows with `memoryam_slot_callbacks`. From
there, a quick call to the `object_access_hook` with an `ObjectAccessType`
of `OAT_FUNCTION_EXECUTE` gives us an additional entry point.

From here, we begin a sequential scan, just like a `SELECT`, but this scan
determines which rows should be deleted. Upon a match from a
`memoryam_getnextslot` call, a call into `memoryam_tuple_delete` is made,
which attempts to find the row and delete it by making a call into the storage
engine with a `delete_row` call for the `ItemPointerData` entry that we
returned during the previous step of the scan.

As with the completion of the sequential scan for a `SELECT`, we finish with
calls to `ExecutorFinish`, `ExecutorEnd`, and then `memoryam_end_scan`. As
with every other call, we wait until the transaction disposition to finalize
any deletion changes.

## Updates

Updates in PostgreSQL are typically done via a deletion and insertion, not
as an in-place change.

```SQL
UPDATE t1 SET t = 'world' WHERE i = 1;
```

Like a `DELETE`, we begin with `memoryam_estimate_rel_size` and quickly
move to the `memoryam_slot_callbacks` and `memoryam_object_access_hook` before
moving into a sequential scan.

Instead of calling for deletion directly, `memoryam_fetch_row_version` is
called to check the visibility of the row to be updated, as well as to retrieve
a copy of the row. We use this to check for visibility by calling into the
storage engine's `row_visible_in_snapshot` method, and then the storage engine's
`read_row` method.

If all is available, `memoryam_tuple_update` is called, which internally simply
calls the storage engine's `row_delete`, and then `memoryam_tuple_insert` to
insert the replacement row.

As before, the scan is finished and any disposition as to the transaction is
executed.
