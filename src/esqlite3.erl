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

    step/1,
    reset/1,
    interrupt/1,

    backup_init/4,
    backup_remaining/1,
    backup_pagecount/1,
    backup_step/2,
    backup_finish/1,

    bind/2,
    q/2, q/3,
    fetchall/1,

    status/0, status/1, status/2
]).

-define(DEFAULT_TIMEOUT, infinity).
-define(DEFAULT_CHUNK_SIZE, 5000).

-define(SQLITE_PREPARE_PERSISTENT, 16#01).
-define(SQLITE_PREPARE_NO_VTAB, 16#04).

-record(esqlite3, {
    db :: esqlite3_nif:esqlite3_ref()
}).

-record(esqlite3_stmt, {
    stmt :: esqlite3_nif:esqlite3_stmt_ref()
}).

-record(esqlite3_backup, {
    backup :: esqlite3_nif:esqlite3_backup_ref()
}).

-type esqlite3() :: #esqlite3{}. 
-type esqlite3_stmt() :: #esqlite3_stmt{}.
-type esqlite3_backup() :: #esqlite3_backup{}.
-type sql() :: esqlite3_nif:sql().

-type prepare_flags() :: persistent | no_vtab.

-type status_info() :: #{ memory_used => stats(),
                          pagecache_used => stats(),
                          pagecache_overflow => stats(),
                          malloc_size := stats(),
                          parser_stack := stats(),
                          pagecache_size := stats(),
                          malloc_count := stats() }.
-type stats() :: #{ used := non_neg_integer(), highwater := non_neg_integer() }.
-type rowid() :: esqlite3_nif:rowid().
-type cell() :: esqlite3_nif:cell().
-type row() :: esqlite3_nif:row().
-type error() :: esqlite3_nif:error().

-export_type([esqlite3/0, esqlite3_stmt/0, esqlite3_backup/0, prepare_flags/0, sql/0, row/0, rowid/0, cell/0]).

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
%% The standard supplied sqlite3 library supports uri filenames, which makes
%% it possible to open the connection to the database in read-only mode. More
%% information about this can be found here: [https://sqlite.org/uri.html] 
%%
%% Example:
%
%% ```open("file:data.db")'''
%%     Opens "data.db" in the current working directory
%% ```open("file:data.db?mode=ro&cache=private")''' 
%%     Opens "data.db" in read only mode with a private cache
%% ```open("file:memdb1?mode=memory&cache=shared")'''
%%     Opens a shared memory database named memdb1 with a shared cache.
%%
-spec open(Filename) -> OpenResult
    when Filename :: string(),
         OpenResult ::  {ok, esqlite3()} | error().
open(Filename) ->
    case esqlite3_nif:open(Filename) of
        {ok, Connection} ->
            {ok, #esqlite3{db=Connection}};
        {error, _}=Error ->
            Error
    end.

%% @doc Close the database
-spec close(Connection) -> CloseResult
    when Connection :: esqlite3(),
         CloseResult :: ok | {error, _}.
close(#esqlite3{db=Connection}) ->
    esqlite3_nif:close(Connection).

%% @doc Return a description of the last occurred error. 
-spec error_info(Connection) -> ErrorInfo
    when Connection :: esqlite3(),
         ErrorInfo :: esqlite3_nif:error_info().
error_info(#esqlite3{db=Connection}) ->
    esqlite3_nif:error_info(Connection).

%% @doc Interrupt a long running query. See [https://sqlite.org/c3ref/interrupt.html] for more details.
-spec interrupt(Connection) -> Result 
    when Connection :: esqlite3(),
         Result:: ok.
interrupt(#esqlite3{db=Db}) ->
    esqlite3_nif:interrupt(Db).

%% @doc Subscribe to database notifications. When rows are inserted deleted
%% or updates, the registered process will receive messages:
%% ```{insert, binary(), binary(), rowid()}'''
%% When a new row has been inserted.
%% ```{delete, binary(), binary(), rowid()}''' 
%% When a new row has been deleted.
%% ```{update, binary(), binary(), rowid()}''' 
%% When a row has been updated.
%%
-spec set_update_hook(Connection, Pid) -> Result when
      Connection :: esqlite3(),
      Pid :: pid(),
      Result :: ok. 
set_update_hook(#esqlite3{db=Connection}, Pid) ->
    esqlite3_nif:set_update_hook(Connection, Pid).

%%%
%%% q
%%%

%% @doc Execute a sql statement, returns the result as list rows.
-spec q(Connection, Sql) -> Result when
      Connection :: esqlite3(),
      Sql :: sql(),
      Result :: list(row()) | error().
q(Connection, Sql) ->
    q(Connection, Sql, []).

%% @doc Execute statement, bind args and return a list rows.
-spec q(Connection, Sql, Args) -> Result when
      Connection :: esqlite3(),
      Sql :: sql(),
      Args :: list(), 
      Result :: list(row()) | error().
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


%%
%% fetchall
%%

% @doc Fetch all rows from the prepared statement.
-spec fetchall(Statement) -> Result when
      Statement :: esqlite3_stmt(),
      Result :: list(row()) | error().
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

%% @doc Get the last inserted rowid.
%%      See [https://sqlite.org/c3ref/set_last_insert_rowid.html] for more details.
-spec last_insert_rowid(Connection) -> RowidResult when
      Connection :: esqlite3(),
      RowidResult :: integer().
last_insert_rowid(#esqlite3{db=Connection}) ->
    esqlite3_nif:last_insert_rowid(Connection).

%% @doc Get the number of changes in the most recent INSERT, UPDATE or DELETE.
%%      See [https://sqlite.org/c3ref/changes.html] for more details.
-spec changes(Connection) -> ChangesResult
    when Connection :: esqlite3(),
         ChangesResult :: integer().
changes(#esqlite3{db=Connection}) ->
    esqlite3_nif:changes(Connection).


%% @doc Check if the connection is in auto-commit mode.
%%      See: [https://sqlite.org/c3ref/get_autocommit.html] for more details.
%%
-spec get_autocommit(Connection) -> AutocommitResult
    when Connection :: esqlite3(),
         AutocommitResult ::  true | false.
get_autocommit(#esqlite3{db=Connection}) ->
    esqlite3_nif:get_autocommit(Connection).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec exec(Connection, Sql) -> ExecResult
    when Connection :: esqlite3(),
         Sql ::  sql(),
         ExecResult :: ok | error().
exec(#esqlite3{db=Connection}, Sql) ->
    esqlite3_nif:exec(Connection, Sql).

%%
%% Prepared Statements
%%

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
%%
-spec prepare(Connection, Sql) -> PrepareResult
    when Connection :: esqlite3(),
         Sql ::  sql(),
         PrepareResult :: {ok, esqlite3_stmt()} | error().
prepare(Connection, Sql) ->
    prepare(Connection, Sql, []).

%% @doc Compile a SQL statement. Returns a cached compiled statement which can be used in
%% queries.
-spec prepare(Connection, Sql, PrepareFlags) -> PrepareResult when
      Connection :: esqlite3(),
      Sql ::  sql(),
      PrepareFlags :: list(prepare_flags()),
      PrepareResult :: {ok, esqlite3_stmt()} | {error, _}.
prepare(#esqlite3{db=Connection}, Sql, PrepareFlags) ->
    case esqlite3_nif:prepare(Connection, Sql, props_to_prepare_flag(PrepareFlags)) of
        {ok, Stmt} ->
            {ok, #esqlite3_stmt{stmt=Stmt}};
        {error, _}=Error ->
            Error
    end.

%% @doc Bind an array of values as parameters of a prepared statement
-spec bind(Statement, Args) -> Result when
      Statement :: esqlite3_stmt(),
      Args :: list() | map(),
      Result :: ok | {error, _}.
%% Named parameters
bind(#esqlite3_stmt{}=Statement, Args) when is_map(Args) ->
    bind(Statement, maps:to_list(Args));
bind(#esqlite3_stmt{}=Statement, [{_Type, _ParameterName, _Value} | _] = Args) ->
    bind2(Statement, Args);
bind(#esqlite3_stmt{}=Statement, [{ParameterName, _Value} | _] = Args)
        when is_binary(ParameterName); is_list(ParameterName) ->
    bind2(Statement, Args);
%% Anonymous parameters
bind(#esqlite3_stmt{}=Statement, Args) when is_list(Args) ->
    bind1(Statement, 1, Args).

bind1(_Statement, _Column, []) ->
    ok;
bind1(Statement, Column, [Arg | Args]) ->
    case bind_arg(Statement, Column, Arg) of
        ok ->
            bind1(Statement, Column + 1, Args);
        {error, _}=Error ->
            Error
    end.

bind2(_Statement, []) ->
    ok;
bind2(#esqlite3_stmt{stmt=Stmt}=Statement, [{Type, ParameterName, Value} | Args]) ->
    case esqlite3_nif:bind_parameter_index(Stmt, ParameterName) of
        {ok, Column} ->
            case bind_arg(Statement, Column, {Type, Value}) of
                ok ->
                    bind2(Statement, Args);
                {error, _}=Error ->
                    Error
            end;
        error ->
            {error, named_parameter_not_found}
    end;
bind2(#esqlite3_stmt{stmt=Stmt}=Statement, [{ParameterName, Value} | Args]) ->
    case esqlite3_nif:bind_parameter_index(Stmt, ParameterName) of
        {ok, Column} ->
            case bind_arg(Statement, Column, Value) of
                ok ->
                    bind2(Statement, Args);
                {error, _}=Error ->
                    Error
            end;
        error ->
            {error, named_parameter_not_found}
    end.

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

-spec bind_int(Statement, Index, Value) -> BindResult when
      Statement :: esqlite3_stmt(),
      Index :: integer(),
      Value :: integer(),
      BindResult :: ok | error().
bind_int(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_int(Stmt, Index, Value).

-spec bind_int64(Statement, Index, Value) -> BindResult when
      Statement :: esqlite3_stmt(),
      Index :: non_neg_integer(),
      Value :: integer(),
      BindResult :: ok | error().
bind_int64(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_int64(Stmt, Index, Value).

-spec bind_double(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: float(),
         BindResult :: ok | error().
bind_double(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_double(Stmt, Index, Value).

-spec bind_text(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: iodata(),
         BindResult :: ok | error().
bind_text(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_text(Stmt, Index, Value).

-spec bind_blob(Statement, Index, Value) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         Value :: iodata(),
         BindResult :: ok | error().
bind_blob(#esqlite3_stmt{stmt=Stmt}, Index, Value) ->
    esqlite3_nif:bind_blob(Stmt, Index, Value).

-spec bind_null(Statement, Index) -> BindResult
    when Statement :: esqlite3_stmt(),
         Index :: integer(),
         BindResult :: ok | error().
bind_null(#esqlite3_stmt{stmt=Stmt}, Index) ->
    esqlite3_nif:bind_null(Stmt, Index).

-spec step(Statement) -> StepResult 
    when Statement :: esqlite3_stmt(),
         StepResult:: row() | '$done' | error().
step(#esqlite3_stmt{stmt=Stmt}) ->
    esqlite3_nif:step(Stmt).

% @doc Reset the prepared statement.
-spec reset(Statement) -> ResetResult 
    when Statement :: esqlite3_stmt(),
         ResetResult:: ok | error().
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
%      See [https://sqlite.org/backup.html] for more details on the backup api.
-spec backup_init(esqlite3(), iodata(), esqlite3(), iodata()) -> {ok, esqlite3_backup()} | {error, _}.
backup_init(#esqlite3{db=Dest}, DestName, #esqlite3{db=Src}, SrcName) ->
    case esqlite3_nif:backup_init(Dest, DestName, Src, SrcName) of
        {ok, BackupRef} ->
            {ok, #esqlite3_backup{backup=BackupRef}};
        {error, _}=Error ->
            Error
    end.

%% @doc Release the resources held by the backup.
-spec backup_finish(Backup) -> Result when
      Backup :: esqlite3_backup(),
      Result :: ok | error().
backup_finish(#esqlite3_backup{backup=Backup}) ->
    esqlite3_nif:backup_finish(Backup).

%% @doc Do a backup step. 
-spec backup_step(esqlite3_backup(), integer()) -> ok | '$done' | error().
backup_step(#esqlite3_backup{backup=Backup}, NPage) ->
    esqlite3_nif:backup_step(Backup, NPage).

%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_remaining(Backup) -> Remaining when
      Backup :: esqlite3_backup(),
      Remaining :: integer().
backup_remaining(#esqlite3_backup{backup=Backup}) ->
    esqlite3_nif:backup_remaining(Backup).

%% @doc Get the remaining number of pages which need to be backed up.
-spec backup_pagecount(Backup) -> Pagecount when
      Backup :: esqlite3_backup(),
      Pagecount :: integer().
backup_pagecount(#esqlite3_backup{backup=Backup}) ->
    esqlite3_nif:backup_pagecount(Backup).

%%
%% Status
%%

%% @doc Get all internal status information from sqlite.
%%
-spec status() -> StatusInfo when
      StatusInfo :: status_info().
status() ->
    status(false).

%% @doc Specify which internal status information you need, when <code>true</code>
%%      is passed, the <code>highwater</code> information from the status information
%%      will be reset.
-spec status(ArgsOrResetHighWater) -> Status when
      ArgsOrResetHighWater :: list(atom()) | boolean(),
      Status :: status_info().
status(Args) when is_list(Args) ->
    status(Args, false);
status(ResetHighWater) when ResetHighWater =:= true orelse ResetHighWater =:= false ->
    status([memory_used,pagecache_used, pagecache_overflow, malloc_size,
            parser_stack, pagecache_size, malloc_count], ResetHighWater);
status(Op) when is_atom(Op) -> 
    status(Op, false).

status(Args, ResetHighWater) when is_list(Args) ->
    status1(Args, #{}, ResetHighWater);
status(Op, ResetHighWater) ->
    esqlite3_nif:status(op_arg(Op), reset_arg(ResetHighWater)).

%%
%% Helpers
%%

status1([], Acc, _ResetArg) -> Acc;
status1([S|Rest], Acc, ResetArg) ->
    status1(Rest, Acc#{ S => status(S, ResetArg)}, ResetArg).

reset_arg(true) -> 1;
reset_arg(false) -> 0.

op_arg(memory_used) -> 0;
op_arg(pagecache_used) -> 1;
op_arg(pagecache_overflow) -> 2;
op_arg(malloc_size) -> 5;
op_arg(parser_stack) -> 6;
op_arg(pagecache_size) -> 7;
op_arg(malloc_count) -> 8.

props_to_prepare_flag(Props) ->
    Flag = case proplists:get_value(no_vtab, Props, false) of
         true -> ?SQLITE_PREPARE_NO_VTAB;
         false -> 0
    end,
    case proplists:get_value(persistent, Props, false) of
        true -> Flag bor ?SQLITE_PREPARE_PERSISTENT;
        false -> Flag
    end.


