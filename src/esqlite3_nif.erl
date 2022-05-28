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
    error_info/1,

    set_update_hook/2,

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

    memory_stats/1,
    status/2
]).

-type esqlite3() :: reference().
-type esqlite3_stmt() :: reference().
-type esqlite3_backup() :: reference().
-type sql() :: iodata(). 

-export_type([esqlite3/0, esqlite3_stmt/0, esqlite3_backup/0, sql/0]).

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

%% @doc Get an error messages for the last occurred error.
%%
-spec error_info(Connection) -> ErrorMsg 
    when Connection :: esqlite3(),
         ErrorMsg :: map().
error_info(_Db) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Set an update hook
%%
-spec set_update_hook(Connection, Pid) -> Result
    when Connection :: esqlite3(),
         Pid :: pid(),
         Result :: ok | {error, closed}.
set_update_hook(_Db, _Pid) ->
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

bind_text(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_blob(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_null(_Statement, _Index) ->
    erlang:nif_error(nif_library_not_loaded).

step(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

reset(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

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
%    erlang:nif_error(nif_library_not_loaded).
-spec backup_init(Destination, DestinationName, Source, SourceName) -> InitResult when
      Destination :: esqlite3(),
      DestinationName :: iodata(),
      Source :: esqlite3(),
      SourceName :: iodata(),
      InitResult :: {ok, esqlite3_backup()} | {error, _}.
backup_init(_Dest, _DestName, _Src, _SrcName) ->
    erlang:nif_error(nif_library_not_loaded).


backup_remaining(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

backup_pagecount(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

backup_step(_Backup, _PageCount) ->
    erlang:nif_error(nif_library_not_loaded).

backup_finish(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

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

%% @doc Get memory statistics 
%%
-spec memory_stats(HighwaterResetFlag) -> Stats when 
      HighwaterResetFlag :: integer(),
      Stats :: #{ used := integer(), highwater := integer() }.
memory_stats(_Flag) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get sqlite status information.
%%
%% MEMORY_USED        0
%% PAGECACHE_USED     1
%% PAGECACHE_OVERFLOW 2
%% MALLOC_SIZE        5
%% PARSER_STACK       6
%% PAGECACHE_SIZE     7
%% MALLOC_COUNT       8
%%
-spec status(Op, HighwaterResetFlag) -> Stats when 
      Op :: integer(),
      HighwaterResetFlag :: integer(),
      Stats :: #{ used := integer(), highwater := integer() }.
status(_Op, _Flag) ->
    erlang:nif_error(nif_library_not_loaded).

