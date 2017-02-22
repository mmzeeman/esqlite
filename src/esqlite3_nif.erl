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
         exec/4,
         changes/3,
         insert/4,
         prepare/4,
         step/3,
         reset/3,
         finalize/3,
         bind/4,
         column_names/3,
         column_types/3,
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
%% @spec start() -> {ok, connection()} | {error, msg()}
start() ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Open the specified sqlite3 database.
%%
%% Sends an asynchronous open command over the connection and returns
%% ok immediately. When the database is opened
%%
%%  @spec open(connection(), reference(), pid(), string()) -> ok | {error, message()}

open(_Db, _Ref, _Dest, _Filename) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Exec the query.
%%
%% Sends an asynchronous exec command over the connection and returns
%% ok immediately.
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%%
%%  @spec exec(connection(), Ref::reference(), Dest::pid(), string()) -> ok | {error, message()}
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
%% @spec prepare(connection(), reference(), pid(), string()) -> ok | {error, message()}
prepare(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%% @spec step(statement(), reference(), pid()) -> ok | {error, message()}
step(_Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%% @spec reset(statement(), reference(), pid()) -> ok | {error, message()}
reset(_Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc
%%
%%
finalize(_Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind parameters to a prepared statement.
%%
%% @spec bind(statement(), reference(), pid(), []) -> ok | {error, message()}
bind(_Stmt, _Ref, _Dest, _Args) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
%% @spec column_names(statement(), reference(), pid()) -> {ok, tuple()} | {error, message()}
column_names(_Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Retrieve the column types of the prepared statement
%%
%% @spec column_types(statement(), reference(), pid()) -> {ok, tuple()} | {error, message()}
column_types(_Stmt, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Close the connection.
%%
%% @spec close(connection(), reference(), pid()) -> ok | {error, message()}
close(_Db, _Ref, _Dest) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Insert record
%%
%% @spec insert(connection(), Ref::reference(), Dest::pid(), string()) -> {ok, integer()} | {error, message()}
insert(_Db, _Ref, _Dest, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).
