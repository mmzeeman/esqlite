%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").

open_single_database_test() ->
    {ok, _C1} = esqlite:open("test.db"),
    ok.

open_multiple_same_databases_test() ->
    {ok, _C1} = esqlite:open("test.db"),
    {ok, _C2} = esqlite:open("test.db"),
    ok.

open_multiple_different_databases_test() ->
    {ok, _C1} = esqlite:open("test1.db"),
    {ok, _C2} = esqlite:open("test2.db"),
    ok.

simple_query_test() ->
    {ok, Db} = esqlite:open("test.db"),
    esqlite:exec(Db, "create table test_table(one varchar(10), two, smallint);"),
    esqlite:exec(Db, ["insert into test_table values(", "hello", ",", "10" ");"]),
    ok.

    
    
    
