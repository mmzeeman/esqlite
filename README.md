Esqlite ![Test](https://github.com/mmzeeman/esqlite/workflows/Test/badge.svg)
=======

An Erlang nif library for sqlite3.

Introduction
------------

This library allows you to use the excellent sqlite engine from
erlang. The library is implemented as a nif library, which allows for
the fastest access to a sqlite database. This can be risky, as a bug
in the nif library or the sqlite database can crash the entire Erlang
VM. If you do not want to take this risk, it is always possible to
access the sqlite nif from a separate erlang node.

Special care has been taken not to block the normal erlang scheduler
of the calling process. This is done by handling neccesary commands
from erlang by using a dirty scheduler.

SQLite Compile Options
----------------------

Esqlite contains an embedded version of sqlite3. Currently version
`3.45.2` is embedded in the repositoy. It is also possible to use 
sqlite provided by the system by using the `ESQLITE_USE_SYSTEM` 
environment flag. 

When sqlite is compiled, the following compile flags are used. These
flags are recommended by sqlite.

```
SQLITE_DQS=0 SQLITE_THREADSAFE=1 SQLITE_DEFAULT_MEMSTATUS=0
SQLITE_DEFAULT_WAL_SYNCHRONOUS=1 SQLITE_LIKE_DOESNT_MATCH_BLOBS
SQLITE_MAX_EXPR_DEPTH=0 SQLITE_OMIT_DEPRECATED
SQLITE_OMIT_PROGRESS_CALLBACK SQLITE_USE_ALLOCA
SQLITE_OMIT_AUTOINIT SQLITE_USE_URI
SQLITE_ENABLE_FTS3 SQLITE_ENABLE_FTS3_PARENTHESIS
SQLITE_ENABLE_FTS4
SQLITE_ENABLE_FTS5
SQLITE_ENABLE_MATH_FUNCTIONS
SQLITE_ENABLE_JSON1
SQLITE_ENABLE_RTREE
SQLITE_ENABLE_GEOPOLY
```

The use of flag `SQLITE_DQS=0` is new in version `0.7`. It can lead
to incompatibilities with respect to the use of single and double 
qouted values. Historically sqlite didn't differentiate between double
and single quoted values, but SQL does. In retrospect, the authors of 
sqlite, think this was a mistake, and introduced this compile flag 
to correct this mistake. It means that string literals **must** use 
single quotes:

```
INSERT INTO table VALUES('abcd', 1234);
```

When double quotes are used the value is seen as object values in 
SQL. So a query like:

```
INSERT INTO table VALUES("abcd", 1234);
```

Will not work, because it sees the value 'abcd' as a SQL object
value, and not a string literal.

Version 0.8.0
-------------

This version is a major derivation from previous versions. When 
I started with this library it was implemented by using a separate
os level thread per connection. At the time this was the only way
to use functions in C which take longer to process than 1ms. 
A lot has changed since then. The VM now has dirty schedulers 
which make it possible to remove the thread per connection.
This makes it possible to open a lot more connections. On the
SQLite side some things have also changed. Extended error codes,
introspection into the internals. This release modernizes the
integration. In some places the API is no longer compatible and
will require small changes. In order to ease this process the 
library now has typespecs, and the documentation was extended.


