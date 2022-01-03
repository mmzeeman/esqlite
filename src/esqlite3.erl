%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 - 2017 Maas-Maarten Zeeman

%% @doc Erlang API for sqlite3 databases

%% Copyright 2011 - 2017 Maas-Maarten Zeeman
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(esqlite3).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([open/1, open/2,
         set_update_hook/2, set_update_hook/3,
         exec/2, exec/3, exec/4,
         changes/1, changes/2,
         insert/2,
         last_insert_rowid/1,
         get_autocommit/1,
         get_autocommit/2,
         prepare/2, prepare/3,
         step/1, step/2,
         reset/1,
         bind/2, bind/3,
         fetchone/1,
         fetchall/1,
         fetchall/2,
         fetchall/3,
         column_names/1, column_names/2,
         column_types/1, column_types/2,
         close/1, close/2,
         flush/0
        ]).

-export([q/2, q/3, q/4, map/3, map/4, foreach/3, foreach/4]).

-define(DEFAULT_TIMEOUT, infinity).
-define(DEFAULT_CHUNK_SIZE, 5000).

%%

-type connection() :: {connection, reference(), term()}.
-type statement() :: {statement, term(), connection()}.
-type sql() :: iodata().

%% erlang -> sqlite type conversions
%%
%% 'undefined' -> null
%% 'null' -> null
%% atom() -> text
%% int() -> int or int64
%% float() -> double
%% string() -> text
%% binary() -> text

-type rowid() :: integer().
-type row() :: tuple(). % tuple of cell_type
-type cell_type() :: undefined | integer() | binary() | float(). 

-export_types([connection/0, statement/0, sql/0, row/0, row_id/0, cell_type/0]).

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
%% The standard supplied sqlite3 library supports uri filenames, which makes
%% it possible to open the connection to the database in read-only mode. More
%% information about this can be found here: [https://sqlite.org/uri.html] 
%%
%% Example:
%%
%% ```open("file:data.db")'''
%%     Opens "data.db" in the current working directory
%% ```open("file:data.db?mode=ro&cache=private")''' 
%%     Opens "data.db" in read only mode with a private cache
%% ```open("file:memdb1?mode=memory&cache=shared")'''
%%     Opens a shared memory database named memdb1 with a shared cache.
%%
-spec open(string()) -> {ok, connection()} | {error, _}.
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Like open/1, but with an additional timeout.
%%
-spec open(string(), timeout()) -> {ok, connection()} | {error, _}.
open(Filename, Timeout) ->
    {ok, Connection} = esqlite3_nif:start(),

    Ref = make_ref(),
    ok = esqlite3_nif:open(Connection, Ref, self(), Filename),
    case receive_answer(Connection, Ref, Timeout) of
        ok ->
            {ok, {connection, make_ref(), Connection}};
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Subscribe to database notifications. When rows are inserted deleted
%% or updates, the process will receive messages:
%% ```{insert, string(), rowid()}'''
%% When a new row has been inserted.
%% ```{delete, string(), rowid()}''' 
%% When a new row has been deleted.
%% ```{update, string(), rowid()}''' 
%% When a row has been updated.
%%
-spec set_update_hook(pid(), connection()) -> ok | {error, term()}.
set_update_hook(Pid, Connection) ->
    set_update_hook(Pid, Connection, ?DEFAULT_TIMEOUT).

%% @doc Same as set_update_hook, but with an additional timeout parameter.
-spec set_update_hook(pid(), connection(), timeout()) -> ok | {error, term()}.
set_update_hook(Pid, {connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:set_update_hook(Connection, Ref, self(), Pid),
    receive_answer(Connection, Ref, Timeout).

%% @doc Execute a sql statement, returns a list with tuples.
-spec q(sql(), connection()) -> list(tuple()) | {error, term()}.
q(Sql, Connection) ->
    q(Sql, [], Connection, ?DEFAULT_TIMEOUT).

%% @doc Execute statement, bind args and return a list with tuples as result.
-spec q(sql(), list(), connection()) -> list(tuple()) | {error, term()}.
q(Sql, Args, Connection) ->
    q(Sql, Args, Connection, ?DEFAULT_TIMEOUT).

%% @doc Execute statement, bind args and return a list with tuples as result restricted by timeout.
-spec q(sql(), list(), connection(), timeout()) -> list(row()) | {error, term()}.
q(Sql, [], Connection, Timeout) ->
    case prepare(Sql, Connection, Timeout) of
        {ok, Statement} ->
            fetchall(Statement, ?DEFAULT_CHUNK_SIZE, Timeout);
        {error, _Msg}=Error ->
            Error
    end;
q(Sql, Args, Connection, Timeout) ->
    case prepare(Sql, Connection, Timeout) of
        {ok, Statement} ->
            ok = bind(Statement, Args, Timeout),
            fetchall(Statement, ?DEFAULT_CHUNK_SIZE, Timeout);
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Execute statement and return a list with the result of F for each row.
-spec map(F, sql(), connection()) -> list(Type) when
      F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: tuple(),
      ColumnNames :: tuple(),
      Type :: any().
map(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            map_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%% @doc Execute statement, bind args and return a list with the result of F for each row.
-spec map(F, sql(), list(), connection()) -> list(Type) when
      F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: tuple(),
      ColumnNames :: tuple(),
      Type :: any().
map(F, Sql, [], Connection) ->
    map(F, Sql, Connection);
map(F, Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            ok = bind(Statement, Args),
            map_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%% @doc Execute statement and call F with each row.
-spec foreach(F, sql(), connection()) -> ok when
      F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: tuple(),
      ColumnNames :: tuple().
foreach(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            foreach_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%% @doc Execute statement, bind args and call F with each row.
-spec foreach(F, sql(), list(), connection()) -> ok when
      F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: tuple(),
      ColumnNames :: tuple().
foreach(F, Sql, [], Connection) ->
    foreach(F, Sql, Connection);
foreach(F, Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            ok = bind(Statement, Args),
            foreach_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%%
-spec foreach_s(F, statement()) -> ok when
      F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: tuple(),
      ColumnNames :: tuple().
foreach_s(F, Statement) when is_function(F, 1) ->
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} -> ok;
        {error, _} = E -> F(E);
        {rows, [Row | []]} ->
            F(Row),
            foreach_s(F, Statement)
    end;
foreach_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} -> ok;
        {error, _} = E -> F([], E);
        {rows, [Row | []]} ->
            F(ColumnNames, Row),
            foreach_s(F, Statement)
    end.

%%
-spec map_s(F, statement()) -> list(Type) when
      F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: tuple(),
      ColumnNames :: tuple(),
      Type :: term().
map_s(F, Statement) when is_function(F, 1) ->
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} -> [];
        {error, _} = E -> F(E);
        {rows, [Row | []]} ->
            [F(Row) | map_s(F, Statement)]
    end;
map_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} -> [];
        {error, _} = E -> F([], E);
        {rows, [Row | []]} ->
            [F(ColumnNames, Row) | map_s(F, Statement)]
    end.

%%
-spec fetchone(statement()) -> tuple().
fetchone(Statement) ->
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} -> ok;
        {error, _} = E -> E;
        {rows, [Row | []]} -> Row
    end.

%% @doc Fetch all records
%% @param Statement is prepared sql statement
-spec fetchall(statement()) ->
                      list(tuple()) |
                      {error, term()}.
fetchall(Statement) ->
    fetchall(Statement, ?DEFAULT_CHUNK_SIZE, ?DEFAULT_TIMEOUT).

%% @doc Fetch all records
%% @param Statement is prepared sql statement
%% @param ChunkSize is a count of rows to read from sqlite and send to erlang process in one bulk.
%%        Decrease this value if rows are heavy. Default value is 5000 (DEFAULT_CHUNK_SIZE).
-spec fetchall(statement(), pos_integer()) ->
                      list(tuple()) |
                      {error, term()}.
fetchall(Statement, ChunkSize) ->
    fetchall(Statement, ChunkSize, ?DEFAULT_TIMEOUT).

%% @doc Fetch all records
%% @param Statement is prepared sql statement
%% @param ChunkSize is a count of rows to read from sqlite and send to erlang process in one bulk.
%%        Decrease this value if rows are heavy. Default value is 5000 (DEFAULT_CHUNK_SIZE).
%% @param Timeout is timeout per each request of the one bulk
-spec fetchall(statement(), pos_integer(), timeout()) ->
                      list(tuple()) |
                      {error, term()}.
fetchall(Statement, ChunkSize, Timeout) ->
    case fetchall_internal(Statement, ChunkSize, [], Timeout) of
        {'$done', Rows} -> lists:reverse(Rows);
        {error, _} = E -> E
    end.

%% return rows in reverse order
-spec fetchall_internal(statement(), pos_integer(), list(tuple()), timeout()) ->
                {'$done', list(tuple())} |
                {error, term()}.
fetchall_internal(Statement, ChunkSize, Rest, Timeout) ->
    case try_multi_step(Statement, ChunkSize, Rest, 0, Timeout) of
        {rows, Rows} -> fetchall_internal(Statement, ChunkSize, Rows, Timeout);
        Else -> Else
    end.

%% Try a number of steps, when the database is busy,
%% return rows in revers order
try_multi_step(Statement, ChunkSize, Rest, Tries) ->
    try_multi_step(Statement, ChunkSize, Rest, Tries, ?DEFAULT_TIMEOUT).

%% Try a number of steps, when the database is busy,
%% return rows in revers order
-spec try_multi_step(statement(), pos_integer(), list(tuple()), non_neg_integer(), timeout()) ->
                {rows, list(tuple())} |
                {'$done', list(tuple())} |
                {error, term()}.
try_multi_step(_Statement, _ChunkSize, _Rest, Tries, _Timeout) when Tries > 5 ->
    throw(too_many_tries);
try_multi_step(Statement, ChunkSize, Rest, Tries, Timeout) ->
    case multi_step(Statement, ChunkSize, Timeout) of
        {'$busy', Rows} -> %% core can fetch a number of rows (rows < ChunkSize) per 'multi_step' call and then get busy...
            erlang:display({"busy", Tries}),
            timer:sleep(100 * Tries),
            try_multi_step(Statement, ChunkSize, Rows ++ Rest, Tries + 1, Timeout);
        {rows, Rows} ->
            {rows, Rows ++ Rest};
        {'$done', Rows} ->
            {'$done', Rows ++ Rest};
        Else -> Else
    end.

%% @doc Execute Sql statement.
%%
-spec exec(sql(), connection()) -> ok |  {error, _}.
exec(Sql, Connection) ->
    exec(Sql, [], Connection, ?DEFAULT_TIMEOUT).

-spec exec(sql(), list(cell_type()) | connection(), connection() | timeout()) -> ok | {error, _}.
exec(Sql, {connection, _,_}=Connection, Timeout) ->
    exec(Sql, [], Connection, Timeout);
exec(Sql, Params, Connection) ->
    exec(Sql, Params, Connection, ?DEFAULT_TIMEOUT).

-spec exec(sql(), list(cell_type()), connection(), timeout()) -> ok | {error, _}.
exec(Sql, [], {connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(Connection, Ref, self(), Sql),
    receive_answer(Connection, Ref, Timeout);
exec(Sql, Params, Connection, Timeout) ->
    {ok, Statement} = prepare(Sql, Connection, Timeout),
    bind(Statement, Params),
    step(Statement, Timeout).


%% @doc Return the number of affected rows of last statement.
-spec changes(connection()) -> non_neg_integer().
changes(Connection) ->
    changes(Connection, ?DEFAULT_TIMEOUT).

-spec changes(connection(), timeout()) -> non_neg_integer().
changes({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:changes(Connection, Ref, self()),
    receive_answer(Connection, Ref, Timeout).

%% @doc Insert records, returns the last rowid.
%%
-spec insert(sql(), connection()) -> {ok, rowid()} |  {error, _}.
insert(Sql, Connection) ->
    insert(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Like insert/2, but with extra timeout parameter.
-spec insert(sql(), connection(), timeout()) -> {ok, rowid()} |  {error, _}.
insert(Sql, {connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:insert(Connection, Ref, self(), Sql),
    receive_answer(Connection, Ref, Timeout).

%% @doc Get the last insert rowid, using the default timeout.
%%
-spec last_insert_rowid(connection()) -> {ok, rowid()} | {error, _}.
last_insert_rowid(Connection) ->
    last_insert_rowid(Connection, ?DEFAULT_TIMEOUT).

%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(connection(), timeout()) -> {ok, integer()} | {error, _}.
last_insert_rowid({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:last_insert_rowid(Connection, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Get autocommit
%% @doc Check if the connection is in auto-commit mode.
%% See: [https://sqlite.org/c3ref/get_autocommit.html] for more details.
%%
-spec get_autocommit(connection()) -> true | false.
get_autocommit(Connection) ->
    get_autocommit(Connection, ?DEFAULT_TIMEOUT).

%% @doc Like autocommit/1, but with an extra timeout attribute.
-spec get_autocommit(connection(), timeout()) -> true | false.
get_autocommit({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:get_autocommit(Connection, Ref, self()),
    receive_answer(Connection, Ref, Timeout).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec prepare(sql(), connection()) -> {ok, statement()} | {error, _}.
prepare(Sql, Connection) ->
    prepare(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Like prepare/2, but with an extra timeout value.
-spec prepare(sql(), connection(), timeout()) -> {ok, statement()} | {error, _}.
prepare(Sql, {connection, _Ref, Connection}=C, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(Connection, Ref, self(), Sql),
    case receive_answer(Connection, Ref, Timeout) of
        {ok, Stmt} -> {ok, {statement, Stmt, C}};
        Else -> Else
    end.

%% @doc Step
%%
-spec step(statement()) -> tuple() | '$busy' | '$done'.
step(Stmt) ->
    step(Stmt, ?DEFAULT_TIMEOUT).

%% @doc
%%
-spec step(statement(), timeout()) -> tuple() | '$busy' | '$done'.
step({statement, Stmt, {connection, _, Conn}}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:multi_step(Conn, Stmt, 1, Ref, self()),
    case receive_answer(Conn, Ref, Timeout) of
        {rows, [Row | []]} -> {row, Row};
        {'$done', []} -> '$done';
        {'$busy', []} -> '$busy';
        Else -> Else
    end.

%% make multiple sqlite steps per call
%% return rows in reverse order
-spec multi_step(term(), pos_integer(), timeout()) ->
                {rows, list(tuple())} |
                {'$busy', list(tuple())} |
                {'$done', list(tuple())} |
                {error, term()}.
multi_step({statement, Stmt, {connection, _, Conn}}, ChunkSize, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:multi_step(Conn, Stmt, ChunkSize, Ref, self()),
    receive_answer(Conn, Ref, Timeout).

%% @doc Reset the prepared statement back to its initial state.
%%
-spec reset(statement()) -> ok | {error, _}.
reset({statement, Stmt, {connection, _, Conn}}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:reset(Conn, Stmt, Ref, self()),
    receive_answer(Conn, Ref, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
%%
-spec bind(statement(), list(cell_type())) -> ok | {error, _}.
bind(Stmt, Args) ->
    bind(Stmt, Args, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
-spec bind(statement(), list(cell_type()), timeout()) -> ok | {error, _}.
bind({statement, Stmt, {connection, _, Conn}}, Args, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(Conn, Stmt, Ref, self(), Args),
    receive_answer(Conn, Ref, Timeout).

%% @doc Return the column names of the prepared statement.
%%
-spec column_names(statement()) -> {atom()}.
column_names(Stmt) ->
    column_names(Stmt, ?DEFAULT_TIMEOUT).

-spec column_names(statement(), timeout()) -> {atom()}.
column_names({statement, Stmt, {connection, _, Conn}}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(Conn, Stmt, Ref, self()),
    receive_answer(Conn, Ref, Timeout).

%% @doc Return the column types of the prepared statement.
%%
-spec column_types(statement()) -> {atom()}.
column_types(Stmt) ->
    column_types(Stmt, ?DEFAULT_TIMEOUT).

-spec column_types(statement(), timeout()) -> {atom()}.
column_types({statement, Stmt, {connection, _, Conn}}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_types(Conn, Stmt, Ref, self()),
    receive_answer(Conn, Ref, Timeout).

%% @doc Close the database
-spec close(connection()) -> ok | {error, _}.
close(Connection) ->
    close(Connection, ?DEFAULT_TIMEOUT).

%% @doc Close the database
-spec close(connection(), timeout()) -> ok | {error, _}.
close({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(Connection, Ref, self()),
    receive_answer(Connection, Ref, Timeout).


%% @doc Flush any stale answers left in the mailbox of the current process.
%%      This can happen if there has been a timeout. Normally the nif functions
%%      are called with the default 'infinite' timeout, so calling this is not
%%      needed.
-spec flush() -> ok.
flush() ->
    flush_answers().


%% Internal functions

receive_answer(Connection, Ref, Timeout) ->
    receive
        {esqlite3, Ref, Resp} -> Resp
    after
        Timeout ->
            ok = esqlite3_nif:interrupt(Connection),
            throw({error, timeout, Ref})
    end.

flush_answers() ->
    receive
        {esqlite3, _, _} -> flush_answers()
    after
        0 -> ok
    end.
