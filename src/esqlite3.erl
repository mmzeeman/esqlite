%%
%%
%%

-module(esqlite3).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([open/1, open/2, 
	 exec/2, exec/3,
	 prepare/2, prepare/3, 
	 step/1, step/2, 
	 bind/2, bind/3, 
	 close/1, close/2]).

-define(DEFAULT_TIMEOUT, infinity).

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
%% @spec open(string()) -> {ok, connection()} | {error, error_message()}
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Open a database connection
%%
%% @spec open(string(), integer()) -> {ok, connection()} | {error, error_message()}
open(Filename, Timeout) ->
    {ok, Db} = esqlite3_nif:start(),

    Ref = make_ref(),
    ok = esqlite3_nif:open(Db, Ref, self(), Filename),
    case receive_answer(Ref, Timeout) of
	ok ->
	    {ok, Db};
	Other ->
	    {error, Other}
    end.

%% @doc Execute Sql statement
%%
%% @spec exec(connection(), iolist()) -> integer() |  {error, error_message()}
exec(Db, Sql) ->
    exec(Db, Sql, ?DEFAULT_TIMEOUT).

%% @doc Execute 
%%
%% @spec exec(connection(), iolist(), integer()) -> integer() | {error, error_message()}
exec(Db, Sql, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(Db, Ref, self(), add_eos(Sql)),
    receive_answer(Ref, Timeout).

%% @doc Prepare a statement
%%
%% @spec prepare(connection(), iolist()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Db, Sql) ->
    prepare(Db, Sql, ?DEFAULT_TIMEOUT).

%% @doc
%%
%% @spec(connection(), iolist()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Db, Sql, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(Db, Ref, self(), add_eos(Sql)),
    receive_answer(Ref, Timeout).

%% @doc Step
%%
%% @spec step(prepared_statement()) -> tuple()
step(Stmt) ->
    step(Stmt, ?DEFAULT_TIMEOUT).

%% @doc 
%%
%% @spec step(prepared_statement(), integer()) -> tuple()
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
%% @spec bind(prepared_statement()) -> ok | {error, error_message()}
bind(Stmt, Args, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(Stmt, Ref, self(), Args),
    receive_answer(Ref, Timeout).

%% @doc Close the database
%%
%% @spec close(connection()) -> ok | {error, error_message()}
close(Db) ->
    close(Db, ?DEFAULT_TIMEOUT).

%% @doc Close the database
%%
%% @spec close(connection(), integer()) -> ok | {error, error_message()}
close(Db, Timeout) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(Db, Ref, self()),
    receive_answer(Ref, Timeout).

%% Internal functions

add_eos(String) when is_list(String) ->
    [String, 0].

receive_answer(Ref, Timeout) ->
    receive 
	{Ref, Resp} ->
	    Resp;
	Other ->
	    throw(Other)
    after Timeout ->
	    throw({error, timeout, Ref})
    end.

    



