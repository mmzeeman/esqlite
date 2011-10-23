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
    {ok, Db} = esqlite:open(":memory:"),
    esqlite:exec(Db, "begin;"),
    esqlite:exec(Db, "create table test_table(one varchar(10), two int);"),
    esqlite:exec(Db, ["insert into test_table values(", "\"hello1\"", ",", "10" ");"]),
    esqlite:exec(Db, ["insert into test_table values(", "\"hello2\"", ",", "11" ");"]),
    esqlite:exec(Db, ["insert into test_table values(", "\"hello3\"", ",", "12" ");"]),
    esqlite:exec(Db, ["insert into test_table values(", "\"hello4\"", ",", "13" ");"]),
    esqlite:exec(Db, "commit;"),
    esqlite:exec(Db, "select * from test_table;"),
    ok.

prepare_test() ->
     {ok, Db} = esqlite:open(":memory:"),
    esqlite:exec(Db, "begin;"),
    esqlite:exec(Db, "create table test_table(one varchar(10), two int);"),

    {ok, Statement} = esqlite:prepare("insert into_test_table(:one, :two)"),

    esqlite:bind(Statement, ":one", "hello"),
    esqlite:bind(Statement, ":two", 11),

    %% of
    esqlite:bind(Statement, ["hello", 11]),

    {ok, []} = esqlite:execute(Statement),


    

    esqlite:exec(Db, "commit;"),
    

    
	 

    

    

    

    
    

    
    
    
