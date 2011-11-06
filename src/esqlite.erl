%%
%%
%%

-module(esqlite).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([open/1, open/2, 
	 exec/2, exec/3,
	 prepare/2, prepare/3, 
	 step/1, step/2, 
	 bind/2, bind/3, 
	 close/1, close/2]).

%% low-level exports
-export([esqlite_start/0, 
	 esqlite_open/4, 
	 esqlite_exec/4, 
	 esqlite_prepare/4,
	 esqlite_step/3,
	 esqlite_bind/4,
	 esqlite_close/3
]).

-on_load(init/0).

-define(DEFAULT_TIMEOUT, infinity).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite_nif", 0).

%% @doc Opens a sqlite3 database mentioned in Filename.
%%
%% @spec open(string()) -> {ok, connection()} | {error, error_message()}
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Open a database connection
%%
%% @spec open(string(), integer()) -> {ok, connection()} | {error, error_message()}
open(Filename, Timeout) ->
    {ok, Db} = esqlite_start(),

    Ref = make_ref(),
    ok = esqlite_open(Db, Ref, self(), Filename),
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
    ok = esqlite_exec(Db, Ref, self(), add_eos(Sql)),
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
    ok = esqlite_prepare(Db, Ref, self(), add_eos(Sql)),
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
    ok = esqlite_step(Stmt, Ref, self()),
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
    ok = esqlite_bind(Stmt, Ref, self(), Args),
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
    ok = esqlite_close(Db, Ref, self()),
    receive_answer(Ref, Timeout).

%% @doc
%%
esqlite_start() ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_open(_Db, _Ref, _Dest, _Filename) ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_exec(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_prepare(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_step(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_bind(_Stmt, _Ref, _Dest, _Args) ->
    exit(nif_library_not_loaded).

%% @doc
esqlite_close(_Db, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

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

    



