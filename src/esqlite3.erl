%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011, 2012 Maas-Maarten Zeeman

%% @doc Erlang API for sqlite3 databases

%% Copyright 2011, 2012 Maas-Maarten Zeeman
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

-define(DEFAULT_TIMEOUT, 5000).

%% 
-type connection() :: tuple().
-type statement() :: term().
-type sql() :: iolist().

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
-spec open(FileName) -> {ok, connection()} | {error, _} when
    FileName :: string().
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Open a database connection
%%
-spec open(Filename, timeout()) -> {ok, connection()} | {error, _} when
    Filename :: string().
open(Filename, Timeout) ->
    {ok, Connection} = esqlite3_nif:start(),

    Ref = make_ref(),
    ok = esqlite3_nif:open(Connection, Ref, self(), Filename),
    case receive_answer(Ref, Timeout) of
        ok ->
            {ok, {connection, make_ref(), Connection}};
        {error, _Msg}=Error ->
            Error
    end.

%% @doc Execute a sql statement, returns a list with tuples.
-spec q(sql(), connection()) -> list(tuple()).
q(Sql, Connection) ->
    q(Sql, [], Connection).

%% @doc Execute statement, bind args and return a list with tuples as result.
-spec q(sql(), list(), connection()) -> list(tuple()).
q(Sql, [], Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} -> 
            fetchall(Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end;
q(Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            ok = bind(Statement, Args),
            fetchall(Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end.

%% @doc
-spec map(F, sql(), connection()) -> list(Type) when
    F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
    Row :: tuple(),
    ColumnNames :: tuple(),
    Type :: any(). 
map(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            map_s(F, Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end.

%% @doc
-spec foreach(F, sql(), connection()) -> ok when
    F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
    Row :: tuple(),
    ColumnNames :: tuple().
foreach(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            foreach_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%%
-spec foreach_s(F, statement()) -> ok when
    F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
    Row :: tuple(),
    ColumnNames :: tuple().
foreach_s(F, Statement) when is_function(F, 1) -> 
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} ->
            F(Row),
            foreach_s(F, Statement)
    end;
foreach_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} -> 
            F(ColumnNames, Row),
            foreach_s(F, Statement)
    end.

%%
-spec map_s(F, statement()) -> list(Type) when
    F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
    Row :: tuple(),
    ColumnNames :: tuple(),
    Type :: term().
map_s(F, Statement) when is_function(F, 1) ->
    case try_step(Statement, 0) of
        '$done' -> [];
        {row, Row} -> 
            [F(Row) | map_s(F, Statement)]
    end;
map_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> [];
        {row, Row} -> 
            [F(ColumnNames, Row) | map_s(F, Statement)]
    end.

%%
-spec fetchone(statement()) -> tuple().
fetchone(Statement) ->
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} -> Row
    end.

%% 
-spec fetchall(statement()) -> list(tuple()).
fetchall(Statement) ->
    case try_step(Statement, 0) of
        '$done' -> 
            [];
        {row, Row} ->
            [Row | fetchall(Statement)]
    end.  

%% Try the step, when the database is busy, 
-spec try_step(statement(), non_neg_integer()) -> term().
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
exec(Sql, {connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(Connection, Ref, self(), add_eos(Sql)),
    receive_answer(Ref, Timeout).

%% @doc Prepare a statement
%%
%% @spec prepare(iolist(), connection()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, Connection) ->
    prepare(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc
%%
%% @spec(iolist(), connection(), timeout()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, {connection, _Ref, Connection}, Timeout) ->
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
-spec column_names(statement()) -> tuple(atom()).
column_names(Stmt) ->
    column_names(Stmt, ?DEFAULT_TIMEOUT).

-spec column_names(statement(), timeout()) -> tuple(atom()).
column_names(Stmt, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(Stmt, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Close the database
%%
%% @spec close(connection()) -> ok | {error, error_message()}
-spec close(connection()) -> ok | {error, _}.
close(Connection) ->
    close(Connection, ?DEFAULT_TIMEOUT).

%% @doc Close the database
%%
%% @spec close(connection(), integer()) -> ok | {error, error_message()}
-spec close(connection(), timeout()) -> ok | {error, _}.
close({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(Connection, Ref, self()),
    receive_answer(Ref, Timeout).

%% Internal functions
add_eos(IoList) ->
    [IoList, 0].

receive_answer(Ref, Timeout) ->
    receive 
        {Ref, Resp} -> Resp;
        Other -> throw(Other)
    after Timeout ->
        throw({error, timeout, Ref})
    end.
