%%
%%
%%

-module(esqlite).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

-on_load(init/0).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite_nif", 0).

%% @doc Open a new database connection
%%
open(Name) ->
    ok.

