%%
%%
%%

-module(esqlite3_nif).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% low-level exports
-export([start/0, 
	 open/4, 
	 exec/4, 
	 prepare/4,
	 step/3,
	 bind/4,
	 close/3
]).

-on_load(init/0).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite3_nif", 0).

%% @doc Start a low level thread which will can handle sqlite3 calls. 
%%
%% @spec 
start() ->
    exit(nif_library_not_loaded).

%% @doc Open a connection to 
%%
open(_Db, _Ref, _Dest, _Filename) ->
    exit(nif_library_not_loaded).

%% @doc
exec(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
prepare(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
step(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc
bind(_Stmt, _Ref, _Dest, _Args) ->
    exit(nif_library_not_loaded).

%% @doc
close(_Db, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

    



