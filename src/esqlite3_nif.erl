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
    start/0,
    open/4,
    set_update_hook/4,
    exec/4,
    changes/3,
    insert/4,
    last_insert_rowid/3,
    get_autocommit/3,
    prepare/4,
    multi_step/5,
    reset/4,
    finalize/4,
    bind/5,
    column_names/4,
    column_types/4,
    backup_init/6,
    backup_step/5,
    backup_remaining/4,
    backup_pagecount/4,
    backup_finish/4,
    interrupt/1,
    close/3
]).

-type raw_connection() :: reference().
-type raw_statement() :: reference().
-type raw_backup() :: reference().
-type sql() :: iodata(). 

-export_type([raw_connection/0, raw_statement/0, raw_backup/0, sql/0]).

-on_load(init/0).

init() ->
    NifName = "esqlite3_nif",
    NifFileName = case code:priv_dir(esqlite) of
                      {error, bad_name} -> filename:join("priv", NifName);
                      Dir -> filename:join(Dir, NifName)
                  end,
    ok = erlang:load_nif(NifFileName, 0).

%% @doc Start a low level thread which will can handle sqlite3 calls.
%%
-spec start() -> {ok, raw_connection()} | {error, _}.
start() ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Open the specified sqlite3 database.
%%
%% Sends an asynchronous open command over the connection and returns
%% ok immediately. When the database is opened
%%
-spec open(raw_connection(), reference(), pid(), string()) -> ok | {error, _}.
open(_Db, _Ref, _Dest, _Filename) ->
    erlang:nif_error(nif_library_not_loaded).

-spec set_update_hook(raw_connection(), reference(), pid(), pid()) -> ok | {error, _}.
set_update_hook(_Db, _Ref, _Dest, _Pid) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Exec the query.
%%
%% Sends an asynchronous exec command over the connection and returns
%% ok immediately.
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%%
-spec exec(raw_connection(), reference(), pid(), sql()) -> ok | {error, _}.
exec(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the number of affected rows of last statement
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
-spec changes(raw_connection(), reference(), pid()) -> ok | {error, _}.
changes(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec prepare(raw_connection(), reference(), pid(), sql()) -> ok | {error, _}.
prepare(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec multi_step(raw_connection(), raw_statement(), pos_integer(), reference(), pid()) -> ok | {error, _}.
multi_step(_Db, _Stmt, _Chunk_Size, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec reset(raw_connection(), raw_statement(), reference(), pid()) -> ok | {error, _}.
reset(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec finalize(raw_connection(), raw_statement(), reference(), pid()) -> ok | {error, _}.
finalize(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind parameters to a prepared statement.
%%
-spec bind(raw_connection(), raw_statement(), reference(), pid(), list(any())) -> ok | {error, _}.
bind(_Db, _Stmt, _Ref, _Dest, _Args) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
-spec column_names(raw_connection(), raw_statement(), reference(), pid()) -> ok | {error, _}.
column_names(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column types of the prepared statement
%%
-spec column_types(raw_connection(), raw_statement(), reference(), pid()) -> ok | {error, _}.
column_types(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Initialize a backup procedure of a database.
-spec backup_init(raw_connection(), string(), raw_connection(), string(), reference(), pid()) -> ok | {error, _}.
backup_init(_DestDb, _DestName, _SourceDb, _SourceName, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Do a backup step.
-spec backup_step(raw_connection(), raw_backup(), integer(), reference(), pid()) -> ok | {error, _}.
backup_step(_Db, _Backup, _NPages, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the amount of remaining pages which need to be backed up.
-spec backup_remaining(raw_connection(), raw_backup(), reference(), pid()) -> ok | {error, _}.
backup_remaining(_Db, _Backup, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the total number of pages which need to be backed up.
-spec backup_pagecount(raw_connection(), raw_backup(), reference(), pid()) -> ok | {error, _}.
backup_pagecount(_Db, _Backup, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Finish the backup.
-spec backup_finish(raw_connection(), raw_backup(), reference(), pid()) -> ok | {error, _}.
backup_finish(_Db, _Backup, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Interrupt all active queries.
-spec interrupt(raw_connection()) -> ok.
interrupt(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Close the connection.
%%
-spec close(raw_connection(), reference(), pid()) -> ok | {error, _}.
close(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Insert record
%%
-spec insert(raw_connection(), reference(), pid(), sql()) -> ok | {error, _}.
insert(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(raw_connection(), reference(), pid()) -> ok | {error, _}.
last_insert_rowid(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get automcommit
%%
-spec get_autocommit(raw_connection(), reference(), pid()) -> ok | {error, _}.
get_autocommit(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

