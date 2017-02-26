%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 - 2017 Maas-Maarten Zeeman

%% @doc Erlang API for sqlite3 databases

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

-module(esqlite3).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([open/1, open/2,
         exec/2, exec/3,
         changes/1, changes/2,
         insert/2,
         prepare/2, prepare/3,
         step/1, step/2,
         reset/1,
         bind/2, bind/3,
         fetchone/1,
         fetchall/1,
         column_names/1, column_names/2,
         column_types/1, column_types/2,
         close/1, close/2]).

-export([q/2, q/3, map/3, foreach/3]).

-define(DEFAULT_TIMEOUT, 5000).

%%
-type connection() :: {connection, reference(), term()}.
-type statement() :: {statement, term(), connection()}.
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
        {error, _} = E -> F(E);
        {row, Row} ->
            F(Row),
            foreach_s(F, Statement)
    end;
foreach_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> ok;
        {error, _} = E -> F([], E);
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
        {error, _} = E -> F(E);
        {row, Row} ->
            [F(Row) | map_s(F, Statement)]
    end;
map_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> [];
        {error, _} = E -> F([], E);
        {row, Row} ->
            [F(ColumnNames, Row) | map_s(F, Statement)]
    end.

%%
-spec fetchone(statement()) -> tuple().
fetchone(Statement) ->
    case try_step(Statement, 0) of
        '$done' -> ok;
        {error, _} = E -> E;
        {row, Row} -> Row
    end.

%%
-spec fetchall(statement()) ->
                      list(tuple()) |
                      {error, term()}.
fetchall(Statement) ->
    case try_step(Statement, 0) of
        '$done' ->
            [];
        {error, _} = E -> E;
        {row, Row} ->
            case fetchall(Statement) of
                {error, _} = E -> E;
                Rest -> [Row | Rest]
            end
    end.

%% Try the step, when the database is busy,
-spec try_step(statement(), non_neg_integer()) -> 
                      '$done' |
                      term().
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
    ok = esqlite3_nif:exec(Connection, Ref, self(), Sql),
    receive_answer(Ref, Timeout);

%% @spec exec(iolist(), list(term()), connection()) -> integer() | {error, error_message()}
exec(Sql, Params, {connection, _, _}=Connection) when is_list(Params) ->
    exec(Sql, Params, Connection, ?DEFAULT_TIMEOUT).

%% @spec exec(iolist(), list(term()), connection(), timeout()) -> integer() | {error, error_message()}
exec(Sql, Params, {connection, _, _}=Connection, Timeout) when is_list(Params) ->
    {ok, Statement} = prepare(Sql, Connection, Timeout),
    bind(Statement, Params),
    step(Statement, Timeout).


%% @doc Return the number of affected rows of last statement.
changes(Connection) ->
    changes(Connection, ?DEFAULT_TIMEOUT).

%% @doc Return the number of affected rows of last statement.
changes({connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:changes(Connection, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Insert records, returns the last rowid.
%%
%% @spec insert(iolist(), connection()) -> {ok, integer()} |  {error, error_message()}
insert(Sql, Connection) ->
    insert(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc Insert
%%
%% @spec insert(iolist(), connection(), timeout()) -> {ok, integer()} | {error, error_message()}
insert(Sql, {connection, _Ref, Connection}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:insert(Connection, Ref, self(), Sql),
    receive_answer(Ref, Timeout).

%% @doc Prepare a statement
%%
%% @spec prepare(iolist(), connection()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, Connection) ->
    prepare(Sql, Connection, ?DEFAULT_TIMEOUT).

%% @doc
%%
%% @spec(iolist(), connection(), timeout()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql, {connection, _Ref, Connection}=C, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(Connection, Ref, self(), Sql),
    case receive_answer(Ref, Timeout) of
        {ok, Stmt} -> {ok, {statement, Stmt, C}};
        Else -> Else
    end.

%% @doc Step
%%
%% @spec step(prepared_statement()) -> tuple()
step(Stmt) ->
    step(Stmt, ?DEFAULT_TIMEOUT).

%% @doc
%%
%% @spec step(prepared_statement(), timeout()) -> tuple()
-spec step(term(), timeout()) -> tuple() | '$busy' | '$done'.
step({statement, Stmt, _}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:step(Stmt, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Reset the prepared statement back to its initial state.
%%
%% @spec reset(prepared_statement()) -> ok | {error, error_message()}
reset({statement, Stmt, _}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:reset(Stmt, Ref, self()),
    receive_answer(Ref, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
%%
%% @spec bind(prepared_statement(), value_list()) -> ok | {error, error_message()}
bind(Stmt, Args) ->
    bind(Stmt, Args, ?DEFAULT_TIMEOUT).

%% @doc Bind values to prepared statements
%%
%% @spec bind(prepared_statement(), [], timeout()) -> ok | {error, error_message()}
bind({statement, Stmt, _}, Args, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(Stmt, Ref, self(), Args),
    receive_answer(Ref, Timeout).

%% @doc Return the column names of the prepared statement.
%%
-spec column_names(statement()) -> {atom()}.
column_names(Stmt) ->
    column_names(Stmt, ?DEFAULT_TIMEOUT).

-spec column_names(statement(), timeout()) -> {atom()}.
column_names({statement, Stmt, _}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(Stmt, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc Return the column types of the prepared statement.
%%
-spec column_types(statement()) -> {atom()}.
column_types(Stmt) ->
    column_types(Stmt, ?DEFAULT_TIMEOUT).

-spec column_types(statement(), timeout()) -> {atom()}.
column_types({statement, Stmt, _}, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_types(Stmt, Ref, self()),
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

receive_answer(Ref, Timeout) ->
    Start = os:timestamp(),
    receive
        {esqlite3, Ref, Resp} ->
            Resp;
        {esqlite3, _, _}=StaleAnswer ->
            error_logger:warning_msg("Esqlite3: Ignoring stale answer ~p~n", [StaleAnswer]),
            PassedMics = timer:now_diff(os:timestamp(), Start) div 1000,
            NewTimeout = case Timeout - PassedMics of
                             Passed when Passed < 0 -> 0;
                             TO -> TO
                         end,
            receive_answer(Ref, NewTimeout)
    after Timeout ->
            throw({error, timeout, Ref})
    end.
