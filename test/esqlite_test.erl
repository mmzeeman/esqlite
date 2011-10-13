%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").

open_test() ->
    {ok, C} = esqlite:open("test.db").
