%%
%%
%%

-module(esqlite).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

-export([open/1]).

-on_load(init/0).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite_nif", 0).

%% @doc Open a new database connection
%%
open(_Filename) ->
    exit(nif_library_not_loaded).

