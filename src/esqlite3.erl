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
    open/1,
    close/1,

    error_info/1,

    %% db connection functions
    set_update_hook/2,

    get_autocommit/1,
    last_insert_rowid/1,
    changes/1,

    %% queries
    exec/2,
    prepare/2,
    prepare/3,

    %% prepared statement functions
    column_names/1,
    column_decltypes/1,

    bind_int/3,
    bind_int64/3,
    bind_double/3,
    bind_text/3,
    bind_blob/3,
    bind_null/2,

    bind/2,

    step/1,
    reset/1,

    q/2, q/3,

    fetchone/1,
    fetchall/1
%
%    backup_init/4, backup_init/5,
%    backup_finish/1, backup_finish/2,
%    backup_remaining/1, backup_remaining/2,
%    backup_pagecount/1, backup_pagecount/2, 
%    backup_step/2, backup_step/3, 
]).

% -export([q/2, q/3, q/4, map/3, map/4, foreach/3, foreach/4]).

-define(DEFAULT_TIMEOUT, infinity).
-define(DEFAULT_CHUNK_SIZE, 5000).

-define(SQLITE_PREPARE_PERSISTENT, 16#01).
-define(SQLITE_PREPARE_NO_VTAB, 16#04).

-record(esqlite3, {
    db :: esqlite3_nif:esqlite3()
}).

-record(esqlite3_stmt, {
    db :: esqlite3_nif:esqlite3(),
    stmt :: esqlite3_nif:esqlite3_stmt()
}).

%-%record(esqlite3_backup, {
%    db :: esqlite3_nif:esqlite3(),
%    backup :: esqlite3_nif:esqlite3_backup()
%}).

-type esqlite3() :: #esqlite3{}. 
-type esqlite3_stmt() :: #esqlite3_stmt{}.
%-type esqlite3_backup() :: #esqlite3_backup{}.
-type sql() :: esqlite3_nif:sql().

-type prepare_flags() :: persistent | no_vtab.

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

-export_type([esqlite3/0, esqlite3_stmt/0, prepare_flags/0, sql/0, row/0, rowid/0, cell_type/0]).

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
-spec open(Filename) -> OpenResult
    when Filename :: string(),
         OpenResult ::  {ok, esqlite3()} | {error, _}.
open(Filename) ->
    case esqlite3_nif:open(Filename) of
        {ok, Connection} ->
            {ok, #esqlite3{db=Connection}};
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Close the database
-spec close(Connection) -> CloseResult
    when Connection :: esqlite3(),
         CloseResult :: ok | {error, _}.
close(#esqlite3{db=Connection}) ->
    esqlite3_nif:close(Connection).

%% @doc Return a description of the last occurred error. 
-spec error_info(Connection) -> ErrorMsg 
    when Connection :: esqlite3(),
         ErrorMsg :: undefined | binary().
error_info(#esqlite3{db=Connection}) ->
    esqlite3_nif:error_info(Connection).


%% @doc Subscribe to database notifications. When rows are inserted deleted
%% or updates, the process will receive messages:
%% ```{insert, binary(), binary(), rowid()}'''
%% When a new row has been inserted.
%% ```{delete, binary(), binary(), rowid()}''' 
%% When a new row has been deleted.
%% ```{update, binary(), binary(), rowid()}''' 
%% When a row has been updated.
%%
-spec set_update_hook(esqlite3(), pid() | undefined) -> ok | {error, term()}.
set_update_hook(#esqlite3{db=Connection}, MaybePid) when is_pid(MaybePid) orelse MaybePid =:= undefined ->
    esqlite3_nif:set_update_hook(Connection, MaybePid).

%%%
%%% q
%%%

%%% @doc Execute a sql statement, returns a list with tuples.
%-spec q(sql(), connection()) -> list(row()) | {error, _}.
q(Connection, Sql) ->
    q(Connection, Sql, []).

%% @doc Execute statement, bind args and return a list with tuples as result restricted by timeout.
%-spec q(sql(), list(), connection(), timeout()) -> list(row()) | {error, _}.
q(Connection, Sql, []) ->
    case prepare(Connection, Sql) of
        {ok, Statement} ->
            fetchall(Statement);
        {error, _Msg}=Error ->
            Error
    end;

q(Connection, Sql, Args) ->
    case prepare(Connection, Sql) of
        {ok, Statement} ->
            case bind(Statement, Args) of
                ok ->
                    fetchall(Statement);
                {error, _}=Error ->
                    Error
            end;
        {error, _Msg}=Error ->
            Error
    end.

%%%
%% map
%%%
%
%%% @doc Execute statement and return a list with the result of F for each row.
%-spec map(Fun, sql(), connection()) -> list(Type) when
%      Fun :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
%      Row :: row(),
%      ColumnNames :: tuple(),
%      Type :: any().
%map(Fun, Sql, Connection) ->
%    case prepare(Sql, Connection) of
%        {ok, Statement} ->
%            map_s(Fun, Statement);
%        {error, _Msg}=Error ->
%            Error
%    end.
%
%%% @doc Execute statement, bind args and return a list with the result of F for each row.
%-spec map(F, sql(), list(), connection()) -> list(Type) when
%      F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
%      Row :: tuple(),
%      ColumnNames :: tuple(),
%      Type :: any().
%map(Fun, Sql, [], Connection) ->
%    map(Fun, Sql, Connection);
%map(Fun, Sql, Args, Connection) ->
%    case prepare(Sql, Connection) of
%        {ok, Statement} ->
%            case bind(Statement, Args) of
%                ok ->
%                    map_s(Fun, Statement);
%%                {error, _}=Error ->
%                    Error
%            end;
%        {error, _Msg}=Error ->
%%            Error
%    end.

%%
%% foreach
%%

%% @doc Execute statement and call F with each row.
%-spec foreach(Fun, sql(), connection()) -> ok when
%      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
%      Row :: tuple(),
%      ColumnNames :: tuple().
%foreach(Fun, Sql, Connection) ->
%    case prepare(Sql, Connection) of
%        {ok, Statement} ->
%            foreach_s(Fun, Statement);
%        {error, _Msg}=Error ->
%            Error
%    end.

%% @doc Execute statement, bind args and call F with each row.
%-spec foreach(Fun, sql(), list(), connection()) -> ok when
%      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
%      Row :: row(),
%      ColumnNames :: tuple().
%foreach(F, Sql, [], Connection) ->
%    foreach(F, Sql, Connection);
%foreach(F, Sql, Args, Connection) ->
%    case prepare(Sql, Connection) of
%        {ok, Statement} ->
%            case bind(Statement, Args) of
%%                ok ->
%                    foreach_s(F, Statement);
%                {error, _Msg}=Error ->
%                    Error
%            end;
%        {error, _Msg}=Error ->
%            Error
%    end.

%%
%% fetchall
%%

%%
%-spec fetchone(statement()) -> tuple().
fetchone(Statement) ->
    case step(Statement) of
        Row when is_list(Row) ->
            Row;
        '$done' ->
            ok;
        {error, _} = E ->
            E
    end.
%
%%% @doc Fetch all records
%%% @param Statement is prepared sql statement
%-spec fetchall(statement()) -> list(row()) | {error, _}.
fetchall(Statement) ->
    fetchall1(Statement, []).

fetchall1(Statement, Acc) ->
    case step(Statement) of
        Row when is_list(Row) ->
            fetchall1(Statement, [Row|Acc]);
        '$done' -> 
            lists:reverse(Acc);
        {error, _} = E ->
            E
    end.

%% @doc Bind an array of values to a prepared statement
%%
bind(#esqlite3_stmt{}=Statement, Args) when is_list(Args) ->
    bind1(Statement, 1, Args).

bind1(_Statement, _Column, []) ->
    ok;
bind1(Statement, Column, [Arg | Args]) ->
    bind_arg(Statement, Column, Arg),
    bind1(Statement, Column + 1, Args).

% Bind with automatic tyoe conversion
bind_arg(Statement, Column, undefined) ->
    bind_null(Statement, Column);
bind_arg(Statement, Column, null) ->
    bind_null(Statement, Column);
bind_arg(Statement, Column, Atom) when is_atom(Atom) ->
    bind_text(Statement, Column, atom_to_binary(Atom, utf8)); 
bind_arg(Statement, Column, Int) when is_integer(Int) ->
    bind_int64(Statement, Column, Int);
bind_arg(Statement, Column, Float) when is_float(Float) ->
    bind_double(Statement, Column, Float);
bind_arg(Statement, Column, Bin) when is_binary(Bin) ->
    bind_text(Statement, Column, Bin);
bind_arg(Statement, Column, String) when is_list(String) ->
    bind_text(Statement, Column, String);
%% Explicit type binds.
bind_arg(Statement, Column, {int, Value}) ->
    bind_int(Statement, Column, Value);
bind_arg(Statement, Column, {int64, Value}) ->
    bind_int64(Statement, Column, Value);
bind_arg(Statement, Column, {float, Value}) ->
    bind_double(Statement, Column, Value);
bind_arg(Statement, Column, {text, Value}) ->
    bind_text(Statement, Column, Value);
bind_arg(Statement, Column, {blob, Value}) ->
    bind_blob(Statement, Column, Value).



%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(Connection) -> RowidResult when
      Connection :: esqlite3(),
      RowidResult :: integer() | {error, closed}.
last_insert_rowid(#esqlite3{db=Connection}) ->
    esqlite3_nif:last_insert_rowid(Connection).

%% @doc Get the number of changes in the most recent INSERT, UPDATE or DELETE.
%%
-spec changes(Connection) -> ChangesResult
    when Connection :: esqlite3(),
         ChangesResult :: integer() | {error, closed}.
changes(#esqlite3{db=Connection}) ->
    esqlite3_nif:changes(Connection).


%% @doc Check if the connection is in auto-commit mode.
%% See: [https://sqlite.org/c3ref/get_autocommit.html] for more details.
%%
-spec get_autocommit(Connection) -> AutocommitResult
    when Connection :: esqlite3(),
         AutocommitResult ::  true | false | {error, closed}.
get_autocommit(#esqlite3{db=Connection}) ->
    esqlite3_nif:get_autocommit(Connection).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec exec(Connection, Sql) -> ExecResult
    when Connection :: esqlite3(),
         Sql ::  sql(),
         ExecResult :: ok | {error, _}.
exec(#esqlite3{db=Connection}, Sql) ->
    esqlite3_nif:exec(Connection, Sql).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec prepare(Connection, Sql) -> PrepareResult
    when Connection :: esqlite3(),
         Sql ::  sql(),
         PrepareResult :: {ok, esqlite3_stmt()} | {error, _}.
prepare(Connection, Sql) ->
    prepare(Connection, Sql, []).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec prepare(Connection, Sql, PrepareFlags) -> PrepareResult
    when Connection :: esqlite3(),
         Sql ::  sql(),
         PrepareFlags :: list(prepare_flags()),
         PrepareResult :: {ok, esqlite3_stmt()} | {error, _}.
prepare(#esqlite3{db=Connection}, Sql, PrepareFlags) ->
    case esqlite3_nif:prepare(Connection, Sql, props_to_prepare_flag(PrepareFlags)) of
        {ok, Stmt} ->
            {ok, #esqlite3_stmt{db=Connection, stmt=Stmt}};
        {error, _}=Error ->
            Error
    end.

-spec bind_int(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: integer(),
         BindResult :: ok | {error, _}.
bind_int(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_int(Stmt, Index, Value).

-spec bind_int64(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: integer(),
         BindResult :: ok | {error, _}.
bind_int64(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_int64(Stmt, Index, Value).

-spec bind_double(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: float(),
         BindResult :: ok | {error, _}.
bind_double(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_double(Stmt, Index, Value).

-spec bind_text(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: iodata(),
         BindResult :: ok | {error, _}.
bind_text(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_text(Stmt, Index, Value).

-spec bind_blob(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: iodata(),
         BindResult :: ok | {error, _}.
bind_blob(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_blob(Stmt, Index, Value).

-spec bind_null(Statement, Index) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         BindResult :: ok | {error, _}.
bind_null(#esqlite3_stmt{stmt=Stmt}, Index) ->
    esqlite3_nif:bind_null(Stmt, Index).

-spec step(Statement) -> StepResult 
    when Statement :: esqlite3_stmt(),
         StepResult:: ok | {error, _}.
step(#esqlite3_stmt{stmt=Stmt}) ->
    esqlite3_nif:step(Stmt).

-spec reset(Statement) -> ResetResult 
    when Statement :: esqlite3_stmt(),
         ResetResult:: ok | {error, _}.
reset(#esqlite3_stmt{stmt=Stmt}) ->
    esqlite3_nif:reset(Stmt).

%% @doc Return the column names of the prepared statement.
%%
-spec column_names(Statement) -> Names
    when Statement :: esqlite3_stmt(),
         Names :: list(binary()).
column_names(#esqlite3_stmt{stmt=Stmt}) ->
     esqlite3_nif:column_names(Stmt).

%% @doc Return the column types of the prepared statement.
%%
-spec column_decltypes(Statement) -> Types
      when Statement :: esqlite3_stmt(),
           Types :: list(binary() | undefined).
column_decltypes(#esqlite3_stmt{stmt=Stmt}) ->
    esqlite3_nif:column_decltypes(Stmt).

%%
%% Backup API
%%

% @doc Initialize a backup procedure. 
%%
%-spec backup_init(connection(), string(), connection(), string()) -> {ok, backup()} | {error, _}.
%backup_init(Dest, DestName, Src, SrcName) ->
%    backup_init(Dest, DestName, Src, SrcName, ?DEFAULT_TIMEOUT).
%
%% @doc Like backup_init/4, but with an extra timeout value.
%%
%-spec backup_init(connection(), string(), connection(), string(), timeout()) -> {ok, backup()} | {error, _}.
%backup_init(#connection{raw_connection=Dest}, DestName, #connection{raw_connection=Src}, SrcName, Timeout) ->
%    Ref = make_ref(),
%    ok = esqlite3_nif:backup_init(Dest, DestName, Src, SrcName, Ref, self()),
%    case receive_answer(Dest, Ref, Timeout) of
%        {ok, RawBackup} when is_reference(RawBackup) ->
%            {ok, #backup{raw_connection=Dest, raw_backup=RawBackup}};
%        {error, _} = Error ->
%            Error
%    end.


%% @doc Release the resources held by the backup.
%-spec backup_finish(backup()) -> ok | {error, _}.
%backup_finish(Backup) ->
%    backup_finish(Backup, ?DEFAULT_TIMEOUT).
%%% @doc Like backup_finish/1, but with an extra timeout.    
%-spec backup_finish(backup(), timeout()) -> ok | {error, _}.
%backup_finish(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
%    Ref = make_ref(),
%    ok = esqlite3_nif:backup_finish(Conn, Back, Ref, self()),
%    receive_answer(Conn, Ref, Timeout).

%% @doc Do a backup step. 
%-spec backup_step(backup(), integer()) -> ok | {error, _}.
%backup_step(Backup, NPage) ->
%    backup_step(Backup, NPage, ?DEFAULT_TIMEOUT).

%% @doc Do a backup step. 
%-spec backup_step(backup(), integer(), timeout()) -> ok | {error, _}.
%backup_step(#backup{raw_connection=Conn, raw_backup=Back}, NPage, Timeout) ->
%    Ref = make_ref(),
%    ok = esqlite3_nif:backup_step(Conn, Back, NPage, Ref, self()),
%    receive_answer(Conn, Ref, Timeout).


%% @doc Get the remaining number of pages which need to be backed up.
%-spec backup_remaining(backup()) -> {ok, pos_integer()} | {error, _}.
%backup_remaining(Backup) ->
%    backup_remaining(Backup, ?DEFAULT_TIMEOUT).

%% @doc Get the remaining number of pages which need to be backed up.
%-spec backup_remaining(backup(), timeout()) -> {ok, pos_integer()} | {error, _}.
%backup_remaining(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
%    Ref = make_ref(),
%    ok = esqlite3_nif:backup_remaining(Conn, Back, Ref, self()),
%    case receive_answer(Conn, Ref, Timeout) of
%        {ok, R} when is_integer(R) ->
%            {ok, R};
%        {error, _}=E ->
%            E
%    end.
%%
%% @doc Get the remaining number of pages which need to be backed up.
%-spec backup_pagecount(backup()) -> {ok, pos_integer()} | {error, _}.
%backup_pagecount(Backup) ->
%    backup_pagecount(Backup, ?DEFAULT_TIMEOUT).
%
%% @doc Get the remaining number of pages which need to be backed up.
%-spec backup_pagecount(backup(), timeout()) -> {ok, pos_integer()} | {error, _}.
%backup_pagecount(#backup{raw_connection=Conn, raw_backup=Back}, Timeout) ->
%    Ref = make_ref(),
%    ok = esqlite3_nif:backup_pagecount(Conn, Back, Ref, self()),
%    case receive_answer(Conn, Ref, Timeout) of
%        {ok, R} when is_integer(R) ->
%            {ok, R};
%        {error, _}=E ->
%            E
%    end.

%%
%% Helpers
%%

%-spec foreach_s(Fun, statement()) -> ok when
%      Fun :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
%      Row :: row(),
%      ColumnNames :: tuple().
%foreach_s(Fun, Statement) when is_function(Fun, 1) ->
%    case try_multi_step(Statement, 1, [], 0) of
%        {'$done', []} ->
%%            ok;
%        {error, _} = Error ->
%            Error;
%        {rows, [Row | []]} ->
%            Fun(Row),
%            foreach_s(Fun, Statement)
%    end;
%foreach_s(Fun, Statement) when is_function(Fun, 2) ->
%    ColumnNames = column_names(Statement),
%    case try_multi_step(Statement, 1, [], 0) of
%        {'$done', []} ->
%            ok;
%        {error, _} = Error ->
%            Error;
%        {rows, [Row | []]} ->
%            Fun(ColumnNames, Row),
%            foreach_s(Fun, Statement)
%    end.

%-spec map_s(Fun, statement()) -> list(Type) when
%%      Fun :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
%      Row :: row(),
%      ColumnNames :: tuple(),
%      Type :: term().
%map_s(Fun, Statement) when is_function(Fun, 1) ->
%    case try_multi_step(Statement, 1, [], 0) of
%        {'$done', []} ->
%            [];
%        {error, _} = Error ->
%            Error;
%        {rows, [Row | []]} ->
%            [Fun(Row) | map_s(Fun, Statement)]
%    end;
%map_s(Fun, Statement) when is_function(Fun, 2) ->
%    ColumnNames = column_names(Statement),
%    case try_multi_step(Statement, 1, [], 0) of
%        {'$done', []} ->
%            [];
%        {error, _} = Error ->
%            Error;
%        {rows, [Row | []]} ->
%            [Fun(ColumnNames, Row) | map_s(Fun, Statement)]
%    end.

%% return rows in reverse order
%-spec fetchall_internal(statement(), pos_integer(), list(row()), timeout()) ->
%                {'$done', list(row())} |
%                {error, _}.
%fetchall_internal(Statement, ChunkSize, Rest, Timeout) ->
%    case try_multi_step(Statement, ChunkSize, Rest, 0, Timeout) of
%        {rows, Rows} -> fetchall_internal(Statement, ChunkSize, Rows, Timeout);
%        Else -> Else
%    end.

%% Try a number of steps, when the database is busy,
%%% return rows in revers order
%try_multi_step(Statement, ChunkSize, Rest, Tries) ->
%    try_multi_step(Statement, ChunkSize, Rest, Tries, ?DEFAULT_TIMEOUT).

%% Try a number of steps, when the database is busy,
%% return rows in revers order
%-spec try_multi_step(statement(), pos_integer(), list(tuple()), non_neg_integer(), timeout()) ->
%                {rows, list(tuple())} |
%                {'$done', list(tuple())} |
%                {error, term()}.
%try_multi_step(_Statement, _ChunkSize, _Rest, Tries, _Timeout) when Tries > 5 ->
%    throw(too_many_tries);
%try_multi_step(Statement, ChunkSize, Rest, Tries, Timeout) ->
%    case multi_step(Statement, ChunkSize, Timeout) of
%        {'$busy', Rows} -> %% core can fetch a number of rows (rows < ChunkSize) per 'multi_step' call and then get busy...
%            erlang:display({"busy", Tries}),
%            timer:sleep(100 * Tries),
%            try_multi_step(Statement, ChunkSize, Rows ++ Rest, Tries + 1, Timeout);
%        {rows, Rows} ->
%            {rows, Rows ++ Rest};
%        {'$done', Rows} ->
%            {'$done', Rows ++ Rest};
%        Else -> Else
%    end.

%%
%% Helpers
%%

props_to_prepare_flag(Props) ->
    Flag = case proplists:get_value(no_vtab, Props, false) of
         true -> ?SQLITE_PREPARE_NO_VTAB;
         false -> 0
    end,
    case proplists:get_value(persistent, Props, false) of
        true -> Flag bor ?SQLITE_PREPARE_PERSISTENT;
        false -> Flag
    end.


