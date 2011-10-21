%%
%%
%%

-module(esqlite).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

-export([open/1, open/2, exec/2, exec/3, close/1, close/2]).

-on_load(init/0).

-define(DEFAULT_TIMEOUT, infinity).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite_nif", 0).

%% @doc Open a new database connection
%%
open(Filename) ->
    open(Filename, ?DEFAULT_TIMEOUT).

%% @doc Open a database connection
%%
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
exec(Db, Sql) ->
    exec(Db, Sql, ?DEFAULT_TIMEOUT).

exec(Db, Sql, Timeout) ->
    Ref = make_ref(),
    ok = esqlite_exec(Db, Ref, self(), Sql),
    receive_answer(Ref, Timeout).

%% @doc Close the database
%%
close(Db) ->
    close(Db, ?DEFAULT_TIMEOUT).

close(Db, Timeout) ->
    Ref = make_ref(),
    ok = esqlite_close(Db, Ref, self()),
    receive_answer(Ref, Timeout).
    

%% ---- Internal ----
esqlite_start() ->
    exit(nif_library_not_loaded).

esqlite_open(_Db, _Ref, _Dest, _Filename) ->
    exit(nif_library_not_loaded).

esqlite_exec(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

esqlite_close(_Db, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

receive_answer(Ref, Timeout) ->
    receive 
	{Ref, Resp} ->
	    Resp;
	Other ->
	    throw(Other)
    after Timeout ->
	    throw({error, timeout, Ref})
    end.

    



