%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 - 2022 Maas-Maarten Zeeman
%%
%% @doc Low level Erlang API for sqlite3 databases.
%% @end

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
    bind_parameter_index/2,

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

-type esqlite3_ref() :: reference().        % Reference to a database connection handle. See [https://sqlite.org/c3ref/sqlite3.html] for more details.
-type esqlite3_stmt_ref() :: reference().   % Reference to a prepared statement object. See [https://sqlite.org/c3ref/stmt.html] for more details.
-type esqlite3_backup_ref() :: reference(). % Reference to a online backup object. See [https://sqlite.org/c3ref/backup.html] for more details.
-type sql() :: iodata().                % Make sure the iodata contains utf-8 encoded data.
-type rowid() :: integer().
-type cell() :: undefined | integer() | float() | binary().
-type row() :: list(cell()).
-type extended_errcode() :: integer().  % Extended sqlite3 error code. See [https://sqlite.org/rescode.html] for more details.
-type error() :: {error, extended_errcode()}.
-type error_info() :: #{ errcode := integer(),  
                         extended_errcode := extended_errcode(),
                         errstr := unicode:unicode_binary(),     % English-language text that describes the result code, as UTF-8
                         errmsg := unicode:unicode_binary(),     % English-language text that describes the error, as UTF-8
                         error_offset := integer()               % The byte offset to the token in the input sql.
                       }.  % See: [https://sqlite.org/c3ref/errcode.html] for more information.

-export_type([esqlite3_ref/0, esqlite3_stmt_ref/0, esqlite3_backup_ref/0, sql/0, rowid/0, cell/0, row/0, error/0, error_info/0]).

-on_load(init/0).

init() ->
    NifName = "esqlite3_nif",
    NifFileName = case code:priv_dir(esqlite) of
                      {error, bad_name} -> filename:join("priv", NifName);
                      Dir -> filename:join(Dir, NifName)
                  end,
    ok = erlang:load_nif(NifFileName, 0).


%% @doc Open the specified sqlite3 database. 
%%      It is possible to use sqlite's uri filenames to open files. 
%%      See: [https://sqlite.org/uri.html] for more information.
-spec open(Filename) -> OpenResult when
      Filename :: string(),
      OpenResult :: {ok, esqlite3_ref()} | error().
open(_Filename) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Close the connection.
%%
-spec close(Connection) -> CloseResult
    when Connection :: esqlite3_ref(),
         CloseResult :: ok | {error, _}.
close(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get an error messages for the last occurred error.
%%
-spec error_info(Connection) -> ErrorInfo 
    when Connection :: esqlite3_ref(),
         ErrorInfo :: error_info().
error_info(_Db) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Set an update hook
-spec set_update_hook(Connection, Pid) -> Result
    when Connection :: esqlite3_ref(),
         Pid :: pid(),
         Result :: ok.
set_update_hook(_Db, _Pid) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Execute a sql statement
-spec exec(Connection, Sql) -> ExecResult 
    when Connection :: esqlite3_ref(),
         Sql :: sql(),
         ExecResult :: ok | error().
exec(_Connection, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Compile a sql statement. 
-spec prepare(Connection, Sql, PrepareFlags) -> PrepareResult
    when Connection :: esqlite3_ref(),
         Sql :: sql(),
         PrepareFlags :: non_neg_integer(),
         PrepareResult :: {ok, esqlite3_stmt_ref()} | error().
prepare(_Connection, _Sql, _PrepareFlags) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind an integer to a position in a prepared statement.
-spec bind_int(Statement, Index, Value) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Value :: integer(), %% [todo] Should be a 32 bit integer range.
      Result :: ok | error().
bind_int(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a 64 bit integer to a position in a prepared statement.
-spec bind_int64(Statement, Index, Value) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Value :: integer(), %% [todo] Should be a 64 bit integer range.
      Result :: ok | error().
bind_int64(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind an double/float to a position in a prepared statement.
-spec bind_double(Statement, Index, Value) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Value :: float(), %% [todo] Should be a 64 bit integer range.
      Result :: ok | error().
bind_double(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a utf-8 string to a position in a prepared statement.
-spec bind_text(Statement, Index, Value) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Value :: iodata(), %% [todo] Should be a utf-8 iodata.
      Result :: ok | error().
bind_text(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a blob to a position in a prepared statement.
-spec bind_blob(Statement, Index, Value) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Value :: iodata(),
      Result :: ok | error().
bind_blob(_Statement, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a null to a position in a prepared statement.
-spec bind_null(Statement, Index) -> Result when
      Statement :: esqlite3_stmt_ref(),
      Index :: integer(),
      Result :: ok | error().
bind_null(_Statement, _Index) ->
    erlang:nif_error(nif_library_not_loaded).

-spec bind_parameter_index(Statement, ParameterName) -> Result when
      Statement :: esqlite3_stmt_ref(),
      ParameterName :: iodata(),
      Result :: {ok, integer()} | error.
bind_parameter_index(_Statement, _ParameterName) ->
    erlang:nif_error(nif_library_not_loaded).

-spec step(Statement) -> StepResult when
      Statement :: esqlite3_stmt_ref(),
      StepResult :: row() | '$done' | error().
step(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

-spec reset(Statement) -> ResetResult when
      Statement :: esqlite3_stmt_ref(),
      ResetResult :: ok | error().
reset(_Statement) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
-spec column_names(Statement) -> Names when
      Statement :: esqlite3_stmt_ref(),
      Names :: list(unicode:unicode_binary()).
column_names(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the declared datatypes of all columns.
%%
-spec column_decltypes(Statement) -> Types when
      Statement :: esqlite3_stmt_ref(),
      Types :: list(undefined | unicode:unicode_binary()).
column_decltypes(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Initialize a backup procedure of a database.
-spec backup_init(Destination, DestinationName, Source, SourceName) -> InitResult when
      Destination :: esqlite3_ref(),
      DestinationName :: iodata(),
      Source :: esqlite3_ref(),
      SourceName :: iodata(),
      InitResult :: {ok, esqlite3_backup_ref()} | error().
backup_init(_Dest, _DestName, _Src, _SrcName) ->
    erlang:nif_error(nif_library_not_loaded).

-spec backup_remaining(Backup) -> Remaining when
      Backup :: esqlite3_backup_ref(),
      Remaining :: integer().
backup_remaining(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

-spec backup_pagecount(Backup) -> Pagecount when
      Backup :: esqlite3_backup_ref(),
      Pagecount :: integer().
backup_pagecount(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

-spec backup_step(Backup, NPage) -> Result when
      Backup :: esqlite3_backup_ref(),
      NPage :: integer(),
      Result :: ok | '$done' | error().
backup_step(_Backup, _PageCount) ->
    erlang:nif_error(nif_library_not_loaded).

-spec backup_finish(Backup) -> Result when
      Backup :: esqlite3_backup_ref(),
      Result :: ok | error().
backup_finish(_Backup) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Interrupt all active queries.
-spec interrupt(esqlite3_ref()) -> ok.
interrupt(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the last insert rowid.
%%
-spec last_insert_rowid(esqlite3_ref()) -> rowid().
last_insert_rowid(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get number of changes insert, delete of the most recent completed
%%      INSERT, DELETE or UPDATE statement.
%%
-spec changes(esqlite3_ref()) -> integer().
changes(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get autocommit
%%
-spec get_autocommit(esqlite3_ref()) -> true | false.
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
%% <code>
%% MEMORY_USED        0
%% PAGECACHE_USED     1
%% PAGECACHE_OVERFLOW 2
%% MALLOC_SIZE        5
%% PARSER_STACK       6
%% PAGECACHE_SIZE     7
%% MALLOC_COUNT       8
%% </code>
%%
-spec status(Op, HighwaterResetFlag) -> Stats when 
      Op :: integer(),
      HighwaterResetFlag :: integer(),
      Stats :: #{ used := non_neg_integer(), highwater := non_neg_integer() }.
status(_Op, _Flag) ->
    erlang:nif_error(nif_library_not_loaded).

