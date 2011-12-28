%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 Maas-Maarten Zeeman

%% @doc Erlang API for sqlite3 databases

%% Copyright 2011 Maas-Maarten Zeeman
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
	 exec/2, exec/3,
	 prepare/2, prepare/3, 
	 step/1, step/2, 
	 bind/2, bind/3, 
	 fetchone/1,
	 fetchall/1,
	 column_names/1, column_names/2,
	 close/1, close/2]).

-export([q/2, q/3, map/3, foreach/3]).

-define(DEFAULT_TIMEOUT, infinity).

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
%% @spec open(string()) -> {ok, connection()} | {error, error_message()}
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Open a database connection
%%
%% @spec open(string(), timeout()) -> {ok, connection()} | {error, error_message()}
open(Filename, Timeout) ->
    {ok, Connection} = esqlite3_nif:start(),

    Ref = make_ref(),
    ok = esqlite3_nif:open(Connection, Ref, self(), Filename),
    case receive_answer(Ref, Timeout) of
	ok ->
	    {ok, Connection};
	Other ->
	    {error, Other}
    end.

%% @doc Execute a sql statement, returns a list with tuples. 
q(Sql, Connection) ->
    q(Sql, [], Connection).

%% @doc Execute statement, bind args and return a list with tuples as result.
q(Sql, [], Connection) ->
    {ok, Statement} = prepare(Sql, Connection),
    fetchall(Statement);
q(Sql, Args, Connection) ->
    {ok, Statement} = prepare(Sql, Connection),
    ok = bind(Statement, Args),
    fetchall(Statement).

%% 
map(F, Sql, Connection) ->
    {ok, Statement} = prepare(Sql, Connection),
    map_s(F, Statement).

%% 
foreach(F, Sql, Connection) ->
    {ok, Statement} = prepare(Sql, Connection),
    foreach_s(F, Statement).

%%
foreach_s(F, Statement) when is_function(F, 1) -> 
    case try_step(Statement, 0) of
	'$done' -> ok;
	Row when is_tuple(Row) ->
	    F(Row),
	    foreach_s(F, Statement)
    end;
foreach_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
	'$done' -> ok;
	Row when is_tuple(Row) -> 
	    F(ColumnNames, Row),
	    foreach_s(F, Statement)
    end.

%%
map_s(F, Statement) when is_function(F, 1) ->
    case try_step(Statement, 0) of
	'$done' -> [];
	Row when is_tuple(Row) -> 
	    [F(Row) | map_s(F, Statement)]
    end;
map_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
	'$done' -> [];
	Row when is_tuple(Row) -> 
	    [F(ColumnNames, Row) | map_s(F, Statement)]
    end.

%%
fetchone(Statement) ->
    case try_step(Statement, 0) of
	'$done' -> ok;
	Row when is_tuple(Row) -> 
	    Row
    end.

%%    
fetchall(Statement) ->
    case try_step(Statement, 0) of
	'$done' -> 
	    [];
	Row when is_tuple(Row) ->
	    [Row | fetchall(Statement)]
    end.  

%% Try the step, when the database is busy, 
try_step(_Statement, Tries) when Tries > 5 ->
    throw(too_many_tries);
try_step(Statement, Tries) ->
    case esqlite3:step(Statement) of
	'$busy' -> 
	    timer:sleep(100 * Tries),
	    try_step(Statement, Tries + 1);
	Something -> 
	    Something
    end.
	    
%% @doc Execute Sql statement, returns the number of affected rows.
%%
%% @spec exec(iolist(), connection()) -> integer() |  {error, error_message()}
exec(Sql, Connection) ->
    exec(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Execute 
%%
%% @spec exec(iolist(), connection(), timeout()) -> integer() | {error, error_message()}
exec(Sql, Connection, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(Connection, Ref, self(), add_eos(Sql)),
    receive_answer(Ref, Timeout).

%% @doc Prepare a statement
%%
%% @spec prepare(iolost(), connection()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, Connection) ->
    prepare(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc
%%
%% @spec(iolist(), connection(), timeout()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, Connection, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(Connection, Ref, self(), add_eos(Sql)),
    receive_answer(Ref, Timeout).

%% @doc Step
%%
%% @spec step(prepared_statement()) -> tuple()
step(Stmt) ->
    step(Stmt, ?DEFAULT_TIMEOUT).

%% @doc 
%%
%% @spec step(prepared_statement(), timeout()) -> tuple()
step(Stmt, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:step(Stmt, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Bind values to prepared statements
%%
%% @spec bind(prepared_statement(), value_list()) -> ok | {error, error_message()}
bind(Stmt, Args) ->
    bind(Stmt, Args, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
%%
%% @spec bind(prepared_statement(), [], timeout()) -> ok | {error, error_message()}
bind(Stmt, Args, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(Stmt, Ref, self(), Args),
    receive_answer(Ref, Timeout).

%% @doc Return the column names of the prepared statement.
%%
column_names(Stmt) ->
    column_names(Stmt, ?DEFAULT_TIMEOUT).

column_names(Stmt, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(Stmt, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Close the database
%%
%% @spec close(connection()) -> ok | {error, error_message()}
close(Connection) ->
    close(Connection, ?DEFAULT_TIMEOUT).

%% @doc Close the database
%%
%% @spec close(connection(), integer()) -> ok | {error, error_message()}
close(Connection, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(Connection, Ref, self()),
    receive_answer(Ref, Timeout).

%% Internal functions
add_eos(IoList) ->
    [IoList, 0].

receive_answer(Ref, Timeout) ->
    receive 
	{Ref, Resp} ->
	    Resp;
	Other ->
	    throw(Other)
    after Timeout ->
	    throw({error, timeout, Ref})
    end.

    



