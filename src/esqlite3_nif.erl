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
%%
%% @doc Low level erlang API for sqlite3 databases.

-module(esqlite3_nif).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% low-level exports
-export([
    open/1,
    close/1,

    get_autocommit/1,
    last_insert_rowid/1,
    changes/1,

    exec/2,
    prepare/3,

    column_names/1,
    column_decltypes/1,

    bind_int/3,
    bind_int64/3,
    bind_double/3,
    %bind_text/3,
    %bind_blob/3,
    bind_null/2,

    step/1,

    reset/1,

    interrupt/1
%    set_update_hook/4,
%    exec/4,
%    changes/3,
%
%    multi_step/5,
%    reset/4,
%    finalize/4,
%    bind/5,

%    column_types/4,

%    backup_init/6,
%    backup_step/5,
%    backup_remaining/4,
%    backup_pagecount/4,
%    backup_finish/4,
]).

-type esqlite3() :: reference().
-type esqlite3_stmt() :: reference().
%-type esqlite3_backup() :: reference().
-type sql() :: iodata(). 

-export_type([esqlite3/0, esqlite3_stmt/0, sql/0]).
%-export_type([esqlite3_backup/0]).

-on_load(init/0).

init() ->
    NifName = "esqlite3_nif",
    NifFileName = case code:priv_dir(esqlite) of
                      {error, bad_name} -> filename:join("priv", NifName);
                      Dir -> filename:join(Dir, NifName)
                  end,
    ok = erlang:load_nif(NifFileName, 0).


%% @doc Open the specified sqlite3 database.
%%
-spec open(Filename) -> OpenResult
    when Filename :: string(),
         OpenResult :: {ok, esqlite3()} | {error, _}.
open(_Filename) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Close the connection.
%%
-spec close(Connection) -> CloseResult
    when Connection :: esqlite3(),
          CloseResult :: ok | {error, _}.
close(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Execute a sql statement
%%
-spec exec(Connection, Sql) -> ExecResult 
    when Connection :: esqlite3(),
         Sql :: sql(),
         ExecResult :: ok | {error, _}.
exec(_Connection, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Compile a sql statement. 
%%
-spec prepare(Connection, Sql, PrepareFlags) -> PrepareResult
    when Connection :: esqlite3(),
         Sql :: sql(),
         PrepareFlags :: non_neg_integer(),
         PrepareResult :: {ok, esqlite3_stmt()} | {error, _}.
prepare(_Connection, _Sql, _PrepareFlags) ->
    erlang:nif_error(nif_library_not_loaded).

bind_int(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_int64(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_double(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_null(_Statement, _Index) ->
    erlang:nif_error(nif_library_not_loaded).

step(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

reset(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

% -spec set_update_hook((), pid(), pid()) -> ok | {error, _}.
%set_update_hook(_Db, _Ref, _Dest, _Pid) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Exec the query.
%%
%% Sends an asynchronous exec command over the connection and returns
%% ok immediately.
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%%
%-spec exec(esqlite3(), reference(), pid(), sql()) -> ok | {error, _}.
%%exec(_Db, _Ref, _Dest, _Sql) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the number of affected rows of last statement
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%-spec changes(esqlite3(), reference(), pid()) -> ok | {error, _}.
%changes(_Db, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%-spec multi_step(esqlite3(), esqlite3_stmt(), pos_integer(), reference(), pid()) -> ok | {error, _}.
%multi_step(_Db, _Stmt, _Chunk_Size, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%-spec reset(esqlite3(), esqlite3_stmt(), reference(), pid()) -> ok | {error, _}.
%reset(_Db, _Stmt, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%-spec finalize(esqlite3(), esqlite3_stmt(), reference(), pid()) -> ok | {error, _}.
%finalize(_Db, _Stmt, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind parameters to a prepared statement.
%%
%-spec bind(esqlite3(), esqlite3_stmt(), reference(), pid(), list(any())) -> ok | {error, _}.
%bind(_Db, _Stmt, _Ref, _Dest, _Args) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
-spec column_names(esqlite3_stmt()) -> list(binary()) | {error, _}.
column_names(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the declared datatypes of all columns.
%%
-spec column_decltypes(esqlite3_stmt()) -> list(undefined | binary()) | {error, _}.
column_decltypes(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Initialize a backup procedure of a database.
%-spec backup_init(esqlite3(), string(), esqlite3_stmt(), string(), reference(), pid()) -> ok | {error, _}.
%backup_init(_DestDb, _DestName, _SourceDb, _SourceName, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Do a backup step.
%-spec backup_step(esqlite3(), esqlite3_backup(), integer(), reference(), pid()) -> ok | {error, _}.
%backup_step(_Db, _Backup, _NPages, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the amount of remaining pages which need to be backed up.
%-spec backup_remaining(esqlite3(), esqlite3_backup(), reference(), pid()) -> ok | {error, _}.
%backup_remaining(_Db, _Backup, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the total number of pages which need to be backed up.
%-spec backup_pagecount(esqlite3(), esqlite3_backup(), reference(), pid()) -> ok | {error, _}.
%backup_pagecount(_Db, _Backup, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Finish the backup.
%-spec backup_finish(esqlite3(), esqlite3_backup(), reference(), pid()) -> ok | {error, _}.
%backup_finish(_Db, _Backup, _Ref, _Dest) ->
%    erlang:nif_error(nif_library_not_loaded).

%% @doc Interrupt all active queries.
-spec interrupt(esqlite3()) -> ok.
interrupt(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(esqlite3()) -> integer() | {error, _}.
last_insert_rowid(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get number of changes insert, delete of the most recent completed
%%      INSERT, DELETE or UPDATE statement.
%%
-spec changes(esqlite3()) -> integer() | {error, _}.
changes(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Get autocommit
%%
-spec get_autocommit(esqlite3()) -> true | false | {error, _}.
get_autocommit(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).

