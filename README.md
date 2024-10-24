# MemoryAM - An in-memory table storage method for PostgreSQL

MemoryAM is an in-memory `TEMPORARY TABLE` implementation in `C++` of a PostgreSQL storage method. Its
mission is to be a simple implementation of a TableAM storage system. As such, we store all changes
in memory, and only allow access from a single connection.

More information about the extension and how it works can be found in [Lifecycle.md](docs/Lifecycle.md),
and more information about how the storage engine works can be found in [Store.md](docs/Store.md).

## What is it?

A very simple implementation of TableAM that allows for experimentation in a fairly safe sandbox.

## What is it not?

Something you should use in any production environment. This is a learning tool, not production code.

## FAQ

### Does MemoryAM Support Indexes?

No. Some design decisions were made to keep MemoryAM extremely simple, and thus indexes
are not supported.

### Are transaction supported?

Yes, at the basic level transactions are supported. This does not include sub-transactions or
CommandId's.

### Is vacuum supported?

No, the decision was made to forego implementing a vacuum strategy for now, but it would be fairly
easy to add.

### Is (my favorite feature) supported?

Likely no, again this is a very simple TableAM implementation that implements a simple MVCC and skips over
some of the more advanced implementations such as indexes, custom scans, bulk insertion, or altering the
structure of the table itself.

## Building

Generally, running `make` should get things built.

After that, a `make install` should get MemoryAM installed, and `make installcheck` can run the
tests for you.

If you have `doxygen` involved, you can build the code documentation by running `doxygen src/Doxyfile`.
