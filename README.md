Esqlite [![Build Status](https://api.travis-ci.org/eiri/esqlite.svg?branch=master)](http://travis-ci.org/eiri/esqlite)
=======

An Erlang nif library for sqlite3.

_A note_
------

This fork adds a call to enable extension loading
with fts3, dbstat and json1 compiled right from the get-go.

Introduction
------------

This library allows you to use the accelent sqlite engine from
erlang. The library is implemented as a nif library, which allows for
the fastest access to a sqlite database. This can be risky, as a bug
in the nif library or the sqlite database can crash the entire Erlang
VM. If you do not want to take this risk, it is always possible to
access the sqlite nif from a separate erlang node.

Special care has been taken not to block the scheduler of the calling
process. This is done by handling all commands from erlang within a
lightweight thread. The erlang scheduler will get control back when
the command has been added to the command-queue of the thread.

