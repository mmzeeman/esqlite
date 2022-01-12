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
%%
%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 - 2022 Maas-Maarten Zeeman
%% @doc Erlang API for sqlite3 databases

-module(esqlite3).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([
    open/1, open/2,
    close/1, close/2,
    set_update_hook/2, set_update_hook/3,
    exec/2, exec/3, exec/4,
    changes/1, changes/2,
    insert/2, insert/3,
    last_insert_rowid/1,
    get_autocommit/1, get_autocommit/2,
    prepare/2, prepare/3,
    step/1, step/2,
    reset/1,
    bind/2, bind/3,
    fetchone/1,
    fetchall/1, fetchall/2, fetchall/3,
    column_names/1, column_names/2,
    column_types/1, column_types/2,
    backup_init/4, backup_init/5,
    backup_finish/1, backup_finish/2,
    backup_remaining/1, backup_remaining/2,
    backup_pagecount/1, backup_pagecount/2, 
    backup_step/2, backup_step/3, 
    flush/0
]).

-export([q/2, q/3, q/4, map/3, map/4, foreach/3, foreach/4]).

-define(DEFAULT_TIMEOUT, infinity).
-define(DEFAULT_CHUNK_SIZE, 5000).

-record(connection, {
    raw_connection :: esqlite3_nif:raw_connection()
}).

-record(statement, {
    raw_connection :: esqlite3_nif:raw_connection(),
    raw_statement :: esqlite3_nif:raw_statement()
}).

-record(backup, {
    raw_connection :: esqlite3_nif:raw_connection(),
    raw_backup :: esqlite3_nif:raw_backup()
}).

-type connection() :: #connection{}. 
-type statement() :: #statement{}.
-type backup() :: #backup{}.
-type sql() :: esqlite3_nif:sql().

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

-export_type([connection/0, statement/0, sql/0, row/0, rowid/0, cell_type/0]).

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
    {ok, RawConnection} = esqlite3_nif:start(),

    Ref = make_ref(),
    ok = esqlite3_nif:open(RawConnection, Ref, self(), Filename),
    case receive_answer(RawConnection, Ref, Timeout) of
        ok ->
            {ok, #connection{raw_connection=RawConnection}};
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Close the database
-spec close(connection()) -> ok | {error, _}.
close(Connection) ->
    close(Connection, ?DEFAULT_TIMEOUT).

%% @doc Close the database
-spec close(connection(), timeout()) -> ok | {error, _}.
close(#connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(RawConnection, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Flush any stale answers left in the mailbox of the current process.
%%      This can happen if there has been a timeout. Normally the nif functions
%%      are called with the default 'infinite' timeout, so calling this is not
%%      needed.
-spec flush() -> ok.
flush() ->
    flush_answers().


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

%% @doc Same as set_update_hook/2, but with an additional timeout parameter.
%%
-spec set_update_hook(pid(), connection(), timeout()) -> ok | {error, term()}.
set_update_hook(Pid, #connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:set_update_hook(RawConnection, Ref, self(), Pid),
    receive_answer(RawConnection, Ref, Timeout).

%%
%% q
%%

%% @doc Execute a sql statement, returns a list with tuples.
-spec q(sql(), connection()) -> list(row()) | {error, _}.
q(Sql, Connection) ->
    q(Sql, [], Connection, ?DEFAULT_TIMEOUT).

%% @doc Execute statement, bind args and return a list with tuples as result.
-spec q(sql(), list(), connection()) -> list(row()) | {error, _}.
q(Sql, Args, Connection) ->
    q(Sql, Args, Connection, ?DEFAULT_TIMEOUT).

%% @doc Execute statement, bind args and return a list with tuples as result restricted by timeout.
-spec q(sql(), list(), connection(), timeout()) -> list(row()) | {error, _}.
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
            case bind(Statement, Args, Timeout) of
                ok ->
                    fetchall(Statement, ?DEFAULT_CHUNK_SIZE, Timeout);
                {error, _}=Error ->
                    Error
            end;
        {error, _Msg}=Error ->
            Error
    end.

%%
%% map
%%

%% @doc Execute statement and return a list with the result of F for each row.
-spec map(Fun, sql(), connection()) -> list(Type) when
      Fun :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: row(),
      ColumnNames :: tuple(),
      Type :: any().
map(Fun, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            map_s(Fun, Statement);
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Execute statement, bind args and return a list with the result of F for each row.
-spec map(F, sql(), list(), connection()) -> list(Type) when
      F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: tuple(),
      ColumnNames :: tuple(),
      Type :: any().
map(Fun, Sql, [], Connection) ->
    map(Fun, Sql, Connection);
map(Fun, Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            case bind(Statement, Args) of
                ok ->
                    map_s(Fun, Statement);
                {error, _}=Error ->
                    Error
            end;
        {error, _Msg}=Error ->
            Error
    end.

%%
%% foreach
%%

%% @doc Execute statement and call F with each row.
-spec foreach(Fun, sql(), connection()) -> ok when
      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: tuple(),
      ColumnNames :: tuple().
foreach(Fun, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            foreach_s(Fun, Statement);
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Execute statement, bind args and call F with each row.
-spec foreach(Fun, sql(), list(), connection()) -> ok when
      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: row(),
      ColumnNames :: tuple().
foreach(F, Sql, [], Connection) ->
    foreach(F, Sql, Connection);
foreach(F, Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            case bind(Statement, Args) of
                ok ->
                    foreach_s(F, Statement);
                {error, _Msg}=Error ->
                    Error
            end;
        {error, _Msg}=Error ->
            Error
    end.

%%
%% fetchall
%%

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
-spec fetchall(statement()) -> list(row()) | {error, _}.
fetchall(Statement) ->
    fetchall(Statement, ?DEFAULT_CHUNK_SIZE, ?DEFAULT_TIMEOUT).

%% @doc Fetch all records
%% @param Statement is prepared sql statement
%% @param ChunkSize is a count of rows to read from sqlite and send to erlang process in one bulk.
%%        Decrease this value if rows are heavy. Default value is 5000 (DEFAULT_CHUNK_SIZE).
-spec fetchall(statement(), pos_integer()) -> list(row()) | {error, _}.
fetchall(Statement, ChunkSize) ->
    fetchall(Statement, ChunkSize, ?DEFAULT_TIMEOUT).

%% @doc Fetch all records
%% @param Statement is prepared sql statement
%% @param ChunkSize is a count of rows to read from sqlite and send to erlang process in one bulk.
%%        Decrease this value if rows are heavy. Default value is 5000 (DEFAULT_CHUNK_SIZE).
%% @param Timeout is timeout per each request of the one bulk
-spec fetchall(statement(), pos_integer(), timeout()) -> list(row()) | {error, _}.
fetchall(Statement, ChunkSize, Timeout) ->
    case fetchall_internal(Statement, ChunkSize, [], Timeout) of
        {'$done', Rows} -> lists:reverse(Rows);
        {error, _} = E -> E
    end.

%% @doc Execute Sql statement.
%%
-spec exec(sql(), connection()) -> ok |  {error, _}.
exec(Sql, Connection) ->
    exec(Sql, [], Connection, ?DEFAULT_TIMEOUT).

-spec exec(sql(), list(cell_type()) | connection(), connection() | timeout()) -> ok | {error, _}.
exec(Sql, #connection{}=Connection, Timeout) ->
    exec(Sql, [], Connection, Timeout);
exec(Sql, Params, #connection{}=Connection) ->
    exec(Sql, Params, Connection, ?DEFAULT_TIMEOUT).

-spec exec(sql(), list(cell_type()), connection(), timeout()) -> ok | {error, _}.
exec(Sql, [], #connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(RawConnection, Ref, self(), Sql),
    receive_answer(RawConnection, Ref, Timeout);
exec(Sql, Params, Connection, Timeout) ->
    {ok, Statement} = prepare(Sql, Connection, Timeout),
    case bind(Statement, Params) of
        ok ->
            step(Statement, Timeout);
        {error, _}=Error ->
            Error
    end.


%% @doc Return the number of affected rows of last statement.
-spec changes(connection()) -> non_neg_integer().
changes(Connection) ->
    changes(Connection, ?DEFAULT_TIMEOUT).

-spec changes(connection(), timeout()) -> non_neg_integer().
changes(#connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:changes(RawConnection, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Insert records, returns the last rowid.
%%
-spec insert(sql(), connection()) -> {ok, rowid()} |  {error, _}.
insert(Sql, Connection) ->
    insert(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Like insert/2, but with extra timeout parameter.
-spec insert(sql(), connection(), timeout()) -> {ok, rowid()} |  {error, _}.
insert(Sql, #connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:insert(RawConnection, Ref, self(), Sql),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Get the last insert rowid, using the default timeout.
%%
-spec last_insert_rowid(connection()) -> {ok, rowid()} | {error, _}.
last_insert_rowid(Connection) ->
    last_insert_rowid(Connection, ?DEFAULT_TIMEOUT).

%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(connection(), timeout()) -> {ok, rowid()} | {error, _}.
last_insert_rowid(#connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:last_insert_rowid(RawConnection, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Check if the connection is in auto-commit mode.
%% See: [https://sqlite.org/c3ref/get_autocommit.html] for more details.
%%
-spec get_autocommit(connection()) -> true | false.
get_autocommit(Connection) ->
    get_autocommit(Connection, ?DEFAULT_TIMEOUT).

%% @doc Like autocommit/1, but with an extra timeout attribute.
-spec get_autocommit(connection(), timeout()) -> true | false.
get_autocommit(#connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:get_autocommit(RawConnection, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec prepare(sql(), connection()) -> {ok, statement()} | {error, _}.
prepare(Sql, Connection) ->
    prepare(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Like prepare/2, but with an extra timeout value.
-spec prepare(sql(), connection(), timeout()) -> {ok, statement()} | {error, _}.
prepare(Sql, #connection{raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(RawConnection, Ref, self(), Sql),
    case receive_answer(RawConnection, Ref, Timeout) of
        {ok, Stmt} when is_reference(Stmt) ->
            {ok, #statement{raw_statement=Stmt, raw_connection=RawConnection}};
        {error, _}=Error ->
            Error 
    end.

%% @doc Step
%%
-spec step(statement()) -> tuple() | '$busy' | '$done'.
step(Stmt) ->
    step(Stmt, ?DEFAULT_TIMEOUT).

%% @doc
%%
-spec step(statement(), timeout()) -> tuple() | '$busy' | '$done'.
step(#statement{raw_statement=RawStatement, raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:multi_step(RawConnection, RawStatement, 1, Ref, self()),
    case receive_answer(RawConnection, Ref, Timeout) of
        {rows, [Row | []]} -> {row, Row};
        {'$done', []} -> '$done';
        {'$busy', []} -> '$busy';
        Else -> Else
    end.

%% @doc Reset the prepared statement back to its initial state.
%%
-spec reset(statement()) -> ok | {error, _}.
reset(#statement{raw_statement=RawStatement, raw_connection=RawConnection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:reset(RawConnection, RawStatement, Ref, self()),
    receive_answer(RawConnection, Ref, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
%%
-spec bind(statement(), list(cell_type())) -> ok | {error, _}.
bind(Stmt, Args) ->
    bind(Stmt, Args, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
-spec bind(statement(), list(cell_type()), timeout()) -> ok | {error, _}.
bind(#statement{raw_statement=RawStatement, raw_connection=RawConnection}, Args, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(RawConnection, RawStatement, Ref, self(), Args),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Return the column names of the prepared statement.
%%
-spec column_names(statement()) -> {atom()}.
column_names(Stmt) ->
    column_names(Stmt, ?DEFAULT_TIMEOUT).

-spec column_names(statement(), timeout()) -> {atom()}.
column_names(#statement{raw_statement=RawStatement, raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(RawConnection, RawStatement, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc Return the column types of the prepared statement.
%%
-spec column_types(statement()) -> {atom()}.
column_types(Statement) ->
    column_types(Statement, ?DEFAULT_TIMEOUT).

-spec column_types(statement(), timeout()) -> {atom()}.
column_types(#statement{raw_statement=RawStatement, raw_connection=RawConnection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_types(RawConnection, RawStatement, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%% @doc make multiple sqlite steps per call return rows in reverse order
%%
-spec multi_step(term(), pos_integer(), timeout()) ->
                {rows, list(tuple())} |
                {'$busy', list(tuple())} |
                {'$done', list(tuple())} |
                {error, _}.
multi_step(#statement{raw_statement=RawStatement, raw_connection=RawConnection}, ChunkSize, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:multi_step(RawConnection, RawStatement, ChunkSize, Ref, self()),
    receive_answer(RawConnection, Ref, Timeout).

%%
%% Backup API
%%

% @doc Initialize a backup procedure. 
%%
-spec backup_init(connection(), string(), connection(), string()) -> {ok, backup()} | {error, _}.
backup_init(Dest, DestName, Src, SrcName) ->
    backup_init(Dest, DestName, Src, SrcName, ?DEFAULT_TIMEOUT).

%% @doc Like backup_init/4, but with an extra timeout value.
%%
-spec backup_init(connection(), string(), connection(), string(), timeout()) -> {ok, backup()} | {error, _}.
backup_init(#connection{raw_connection=Dest}, DestName, #connection{raw_connection=Src}, SrcName, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_init(Dest, DestName, Src, SrcName, Ref, self()),
    case receive_answer(Dest, Ref, Timeout) of
        {ok, RawBackup} when is_reference(RawBackup) ->
            {ok, #backup{raw_connection=Dest, raw_backup=RawBackup}};
        {error, _} = Error ->
            Error
    end.


%% @doc Release the resources held by the backup.
-spec backup_finish(backup()) -> ok | {error, _}.
backup_finish(Backup) ->
    backup_finish(Backup, ?DEFAULT_TIMEOUT).
%% @doc Like backup_finish/1, but with an extra timeout.    
-spec backup_finish(backup(), timeout()) -> ok | {error, _}.
backup_finish(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_finish(Conn, Back, Ref, self()),
    receive_answer(Conn, Ref, Timeout).

%% @doc Do a backup step. 
-spec backup_step(backup(), integer()) -> ok | {error, _}.
backup_step(Backup, NPage) ->
    backup_step(Backup, NPage, ?DEFAULT_TIMEOUT).

%% @doc Do a backup step. 
-spec backup_step(backup(), integer(), timeout()) -> ok | {error, _}.
backup_step(#backup{raw_connection=Conn, raw_backup=Back}, NPage, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_step(Conn, Back, NPage, Ref, self()),
    receive_answer(Conn, Ref, Timeout).


%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_remaining(backup()) -> {ok, pos_integer()} | {error, _}.
backup_remaining(Backup) ->
    backup_remaining(Backup, ?DEFAULT_TIMEOUT).

%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_remaining(backup(), timeout()) -> {ok, pos_integer()} | {error, _}.
backup_remaining(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_remaining(Conn, Back, Ref, self()),
    case receive_answer(Conn, Ref, Timeout) of
        {ok, R} when is_integer(R) ->
            {ok, R};
        {error, _}=E ->
            E
    end.

%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_pagecount(backup()) -> {ok, pos_integer()} | {error, _}.
backup_pagecount(Backup) ->
    backup_pagecount(Backup, ?DEFAULT_TIMEOUT).

%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_pagecount(backup(), timeout()) -> {ok, pos_integer()} | {error, _}.
backup_pagecount(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_pagecount(Conn, Back, Ref, self()),
    case receive_answer(Conn, Ref, Timeout) of
        {ok, R} when is_integer(R) ->
            {ok, R};
        {error, _}=E ->
            E
    end.

%%
%% Helpers
%%

-spec foreach_s(Fun, statement()) -> ok when
      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
      Row :: row(),
      ColumnNames :: tuple().
foreach_s(Fun, Statement) when is_function(Fun, 1) ->
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} ->
            ok;
        {error, _} = Error ->
            Error;
        {rows, [Row | []]} ->
            Fun(Row),
            foreach_s(Fun, Statement)
    end;
foreach_s(Fun, Statement) when is_function(Fun, 2) ->
    ColumnNames = column_names(Statement),
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} ->
            ok;
        {error, _} = Error ->
            Error;
        {rows, [Row | []]} ->
            Fun(ColumnNames, Row),
            foreach_s(Fun, Statement)
    end.

-spec map_s(Fun, statement()) -> list(Type) when
      Fun :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
      Row :: row(),
      ColumnNames :: tuple(),
      Type :: term().
map_s(Fun, Statement) when is_function(Fun, 1) ->
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} ->
            [];
        {error, _} = Error ->
            Error;
        {rows, [Row | []]} ->
            [Fun(Row) | map_s(Fun, Statement)]
    end;
map_s(Fun, Statement) when is_function(Fun, 2) ->
    ColumnNames = column_names(Statement),
    case try_multi_step(Statement, 1, [], 0) of
        {'$done', []} ->
            [];
        {error, _} = Error ->
            Error;
        {rows, [Row | []]} ->
            [Fun(ColumnNames, Row) | map_s(Fun, Statement)]
    end.

%% return rows in reverse order
-spec fetchall_internal(statement(), pos_integer(), list(row()), timeout()) ->
                {'$done', list(row())} |
                {error, _}.
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

receive_answer(RawConnection, Ref, Timeout) ->
    receive
        {esqlite3, Ref, Resp} -> Resp
    after
        Timeout ->
            ok = esqlite3_nif:interrupt(RawConnection),
            throw({error, timeout, Ref})
    end.

flush_answers() ->
    receive
        {esqlite3, _, _} -> flush_answers()
    after
        0 -> ok
    end.
