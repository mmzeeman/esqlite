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
    ok = esqlite:exec(Db, "begin;"),
    ok = esqlite:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello1\"", ",", "10" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello2\"", ",", "11" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello3\"", ",", "12" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello4\"", ",", "13" ");"]),
    ok = esqlite:exec(Db, "commit;"),
    ok = esqlite:exec(Db, "select * from test_table;"),
    ok.

prepare_test() ->
    {ok, Db} = esqlite:open(":memory:"),
    esqlite:exec(Db, "begin;"),
    esqlite:exec(Db, "create table test_table(one varchar(10), two int);"),
    {ok, Statement} = esqlite:prepare(Db, "insert into test_table values(\"one\", 2)"),
    
    '$done' = esqlite:step(Statement),

    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello4\"", ",", "13" ");"]), 
    {ok, St2} = esqlite:prepare(Db, "select * from test_table order by two"),
    [{"one", 2}, {"hello4", 13}] = exec(St2),
    esqlite:exec(Db, "commit;"),
    esqlite:close(Db),

    ok.

bind_test() ->
    {ok, Db} = esqlite:open(":memory:"),
    ok = esqlite:exec(Db, "begin;"),
    ok = esqlite:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello1\"", ",", "10" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello2\"", ",", "11" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello3\"", ",", "12" ");"]),
    ok = esqlite:exec(Db, ["insert into test_table values(", "\"hello4\"", ",", "13" ");"]),
    ok = esqlite:exec(Db, "commit;"),

    %% Create a prepared statement
    {ok, Statement} = esqlite:prepare(Db, "insert into test_table values(?1, ?2)"),
    esqlite:bind(Statement, [one, 2]),
    esqlite:step(Statement),
    esqlite:bind(Statement, ["three", 4]),
    esqlite:step(Statement),
    esqlite:bind(Statement, [<<"five">>, 6]),
    esqlite:step(Statement),

    [{"one", 2}] = q(Db, "select * from test_table where two = '2'"),
    [{"three", 4}] = q(Db, "select * from test_table where two = 4"),
    [{<<"five">>, 6}] = q(Db, "select * from test_table where two = 6"),

    ok.

gen_db_test() ->
    {ok, Conn} = gen_db:open(sqlite, ":memory:"),
    [] = gen_db:execute("create table some_shit(hole_one varchar(10), hole_two int);", [], Conn),
    [] = gen_db:execute("insert into some_shit values('dung', 100);", Conn),
    [] = gen_db:execute("insert into some_shit values(?, ?);", ["manure", 1000], Conn),
    ok.


%% Handy functions...
%%
q(Connection, Sql) ->
    {ok, Statement} = esqlite:prepare(Connection, Sql),
    exec(Statement).

q(Connection, Sql, []) ->
    q(Connection, Sql);
q(Connection, Sql, Args) ->
    {ok, Statement} = esqlite:prepare(Connection, Sql),
    esqlite:bind(Statement, Args),
    exec(Statement).
	 

exec(Statement) ->
    exec(Statement, [], 0).

exec(_Statement, _Acc, Tries) when Tries > 5 ->
    throw(too_many_tries);
exec(Statement, Acc, Tries) ->
    case esqlite:step(Statement) of
	'$done' ->
	    lists:reverse(Acc);
	'$busy' ->
	    timer:sleep(100),
	    exec(Statement, Acc, Tries + 1);
	V when is_tuple(V) ->
	    exec(Statement, [V | Acc], 0)
    end.

%%    esqlite:bind(Statement, ":one", "hello"),
%%     esqlite:bind(Statement, ":two", 11),

%%     %% of
%%     esqlite:bind(Statement, ["hello", 11]),

%%     {ok, []} = esqlite:execute(Statement),

%%     esqlite:exec(Db, "commit;"),
    
%%

% api...

%% q(Sql, Connection) ->
%%     ok.

%% q(Sql, Args, Connection) ->
%%     ok.


%% 
%% options --
%%   [{sync, bool()} 
%%          -- asynchronous or synchronous. Default true
%%    {receiver, (pid()|function/1|{M, F, A}}, 
%%          -- for asynchronous requests, to send the result to.
%%    {stream, pid()}, 
%%          -- instead of receiving all rows at once, send them one by one.
%%    {..},
%%   ]
%%
%% return
%%   {ok, Result} -- result kan dan headers, rows of request-id (voor async)
%%   {error, ...
%% q(Sql, Args, Options, Connection) ->
%%     ok.



    
	 

    

    

    

    
    

    
    
    
