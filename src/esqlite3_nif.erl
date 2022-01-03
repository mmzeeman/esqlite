%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 - 2017 Maas-Maarten Zeeman

%% @doc Low level erlang API for sqlite3 databases

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

-module(esqlite3_nif).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% low-level exports
-export([start/0,
         open/4,
         set_update_hook/4,
         exec/4,
         changes/3,
         insert/4,
         get_autocommit/3,
         prepare/4,
         multi_step/5,
         reset/4,
         finalize/4,
         bind/5,
         column_names/4,
         column_types/4,
         interrupt/1,
         close/3
        ]).

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
-spec start() -> {ok, esqlite:connection()} | {error, any()}.
start() ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Open the specified sqlite3 database.
%%
%% Sends an asynchronous open command over the connection and returns
%% ok immediately. When the database is opened
%%
-spec open(esqlite:connection(), reference(), pid(), string()) -> ok | {error, any()}.
open(_Db, _Ref, _Dest, _Filename) ->
    erlang:nif_error(nif_library_not_loaded).

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
-spec exec(esqlite:connection(), reference(), pid(), string()) -> ok | {error, any()}.
exec(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the number of affected rows of last statement
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%%
changes(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec prepare(esqlite:connection(), reference(), pid(), string()) -> ok | {error, any()}.
prepare(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec multi_step(esqlite:connection(), esqlite:statement(), pos_integer(), reference(), pid()) -> ok | {error, any()}.
multi_step(_Db, _Stmt, _Chunk_Size, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec reset(esqlite:connection(), esqlite:statement(), reference(), pid()) -> ok | {error, any()}.
reset(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
-spec finalize(esqlite:connection(), esqlite:statement(), reference(), pid()) -> ok | {error, any()}.
finalize(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind parameters to a prepared statement.
%%
-spec bind(esqlite:connection(), esqlite:statement(), reference(), pid(), list(any())) -> ok | {error, any()}.
bind(_Db, _Stmt, _Ref, _Dest, _Args) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
-spec column_names(esqlite:connection(), esqlite:statement(), reference(), pid()) -> ok | {error, any()}.
column_names(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column types of the prepared statement
%%
-spec column_types(esqlite:connection(), esqlite:statement(), reference(), pid()) -> ok | {error, any()}.
column_types(_Db, _Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Interrupt all active queries.
interrupt(_Db) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Close the connection.
%%
-spec close(esqlite:connection(), reference(), pid()) -> ok | {error, any()}.
close(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Insert record
%%
-spec insert(esqlite:connection(), reference(), pid(), esqlite:sql()) -> ok | {error, any()}.
insert(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get automcommit
%%
-spec get_autocommit(esqlite:connection(), reference(), pid()) -> ok | {error, any()}.
get_autocommit(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).
