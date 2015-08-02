%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").

open_single_database_test() ->
    {ok, _C1} = esqlite3:open("test.db"),
    ok.

open_multiple_same_databases_test() ->
    {ok, _C1} = esqlite3:open("test.db"),
    {ok, _C2} = esqlite3:open("test.db"),
    ok.

open_multiple_different_databases_test() ->
    {ok, _C1} = esqlite3:open("test1.db"),
    {ok, _C2} = esqlite3:open("test2.db"),
    ok.

simple_query_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    {ok, 1} = esqlite3:changes(Db),

    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    {ok, 1} = esqlite3:changes(Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    {ok, 1} = esqlite3:changes(Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    {ok, 1} = esqlite3:changes(Db),
    ok = esqlite3:exec("commit;", Db),
    ok = esqlite3:exec("select * from test_table;", Db),

    ok = esqlite3:exec("delete from test_table;", Db),
    {ok, 4} = esqlite3:changes(Db),
    
    ok.

prepare_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    esqlite3:exec("begin;", Db),
    esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    {ok, Statement} = esqlite3:prepare("insert into test_table values(\"one\", 2)", Db),
    
    '$done' = esqlite3:step(Statement),
    {ok, 1} = esqlite3:changes(Db),

    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db), 

    %% Check if the values are there.
    [{<<"one">>, 2}, {<<"hello4">>, 13}] = esqlite3:q("select * from test_table order by two", Db),
    esqlite3:exec("commit;", Db),
    esqlite3:close(Db),

    ok.

bind_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec("commit;", Db),

    %% Create a prepared statement
    {ok, Statement} = esqlite3:prepare("insert into test_table values(?1, ?2)", Db),
    esqlite3:bind(Statement, [one, 2]),
    esqlite3:step(Statement),
    esqlite3:bind(Statement, ["three", 4]), 
    esqlite3:step(Statement),
    esqlite3:bind(Statement, ["five", 6]), 
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [[<<"se">>, $v, "en"], 8]), % iolist bound as text
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [<<"nine">>, 10]), % iolist bound as text
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [{blob, [<<"eleven">>, 0]}, 12]), % iolist bound as blob with trailing eos.
    esqlite3:step(Statement),

    %% int64
    esqlite3:bind(Statement, [int64, 308553449069486081]),
    esqlite3:step(Statement),

    %% negative int64
    esqlite3:bind(Statement, [negative_int64, -308553449069486081]),
    esqlite3:step(Statement),


    %% utf-8
    esqlite3:bind(Statement, [[<<228,184,138,230,181,183>>], 100]), 
    esqlite3:step(Statement),

    ?assertEqual([{<<"one">>, 2}], 
        esqlite3:q("select one, two from test_table where two = '2'", Db)),
    ?assertEqual([{<<"three">>, 4}], 
        esqlite3:q("select one, two from test_table where two = 4", Db)),
    ?assertEqual([{<<"five">>, 6}], 
        esqlite3:q("select one, two from test_table where two = 6", Db)),
    ?assertEqual([{<<"seven">>, 8}], 
        esqlite3:q("select one, two from test_table where two = 8", Db)),
    ?assertEqual([{<<"nine">>, 10}], 
        esqlite3:q("select one, two from test_table where two = 10", Db)),
    ?assertEqual([{{blob, <<$e,$l,$e,$v,$e,$n,0>>}, 12}], 
        esqlite3:q("select one, two from test_table where two = 12", Db)),

    ?assertEqual([{<<"int64">>, 308553449069486081}], 
        esqlite3:q("select one, two from test_table where one = 'int64';", Db)),
    ?assertEqual([{<<"negative_int64">>, -308553449069486081}], 
        esqlite3:q("select one, two from test_table where one = 'negative_int64';", Db)),

    %% utf-8
    ?assertEqual([{<<228,184,138,230,181,183>>, 100}], 
        esqlite3:q("select one, two from test_table where two = 100", Db)),

    
    ok.

bind_for_queries_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec("commit;", Db),

    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [test_table], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                ["test_table"], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [<<"test_table">>], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [[<<"test_table">>]], Db)),

    ok.

column_names_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "20" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    %% All columns
    {ok, Stmt} = esqlite3:prepare("select * from test_table", Db),
    {one, two} =  esqlite3:column_names(Stmt),
    {row, {<<"hello1">>, 10}} = esqlite3:step(Stmt),
    {one, two} =  esqlite3:column_names(Stmt),
    {row, {<<"hello2">>, 20}} = esqlite3:step(Stmt),
    {one, two} =  esqlite3:column_names(Stmt),
    '$done' = esqlite3:step(Stmt),
    {one, two} =  esqlite3:column_names(Stmt),

    %% One column
    {ok, Stmt2} = esqlite3:prepare("select two from test_table", Db),
    {two} =  esqlite3:column_names(Stmt2),
    {row, {10}} = esqlite3:step(Stmt2),
    {two} =  esqlite3:column_names(Stmt2),
    {row, {20}} = esqlite3:step(Stmt2),
    {two} =  esqlite3:column_names(Stmt2),
    '$done' = esqlite3:step(Stmt2),
    {two} =  esqlite3:column_names(Stmt2),

    %% No columns
    {ok, Stmt3} = esqlite3:prepare("values(1);", Db),
    {column1} =  esqlite3:column_names(Stmt3),
    {row, {1}} = esqlite3:step(Stmt3),
    {column1} =  esqlite3:column_names(Stmt3),

    %% Things get a bit weird when you retrieve the column name
    %% when calling an aggragage function.
    {ok, Stmt4} = esqlite3:prepare("select date('now');", Db),
    {'date(\'now\')'} =  esqlite3:column_names(Stmt4),
    {row, {Date}} = esqlite3:step(Stmt4),
    true = is_binary(Date),

    %% Some statements have no column names
    {ok, Stmt5} = esqlite3:prepare("create table dummy(a, b, c);", Db),
    {} = esqlite3:column_names(Stmt5),

    ok.

column_types_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "20" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    %% All columns
    {ok, Stmt} = esqlite3:prepare("select * from test_table", Db),
    {'varchar(10)', int} =  esqlite3:column_types(Stmt),
    {row, {<<"hello1">>, 10}} = esqlite3:step(Stmt),
    {'varchar(10)', int} =  esqlite3:column_types(Stmt),
    {row, {<<"hello2">>, 20}} = esqlite3:step(Stmt),
    {'varchar(10)', int} =  esqlite3:column_types(Stmt),
    '$done' = esqlite3:step(Stmt),
    {'varchar(10)', int} =  esqlite3:column_types(Stmt),

    %% Some statements have no column types
    {ok, Stmt2} = esqlite3:prepare("create table dummy(a, b, c);", Db),
    {} = esqlite3:column_types(Stmt2),

    ok.

nil_column_types_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table t1(c1 variant);", Db),
    ok = esqlite3:exec("commit;", Db),

    {ok, Stmt} = esqlite3:prepare("select c1 + 1, c1 from t1", Db),
    {nil, variant} =  esqlite3:column_types(Stmt),
    ok.

reset_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    {ok, Stmt} = esqlite3:prepare("select * from (values (1), (2));", Db),
    {row, {1}} = esqlite3:step(Stmt),

    ok = esqlite3:reset(Stmt),
    {row, {1}} = esqlite3:step(Stmt),
    {row, {2}} = esqlite3:step(Stmt),
    '$done' = esqlite3:step(Stmt),

    % After a done the statement is automatically reset.
    {row, {1}} = esqlite3:step(Stmt),

    % Calling reset multiple times...
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),

    % The statement should still be reset.
    {row, {1}} = esqlite3:step(Stmt),

    ok.


foreach_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    F = fun(Row) ->
		case Row of 
		    {Key, Value} ->
			put(Key, Value);
		    _ ->
			ok
		end
	end,
    
    esqlite3:foreach(F, "select * from test_table;", Db),
    
    10 = get(<<"hello1">>),
    11 = get(<<"hello2">>),
    12 = get(<<"hello3">>), 
    13 = get(<<"hello4">>),
    
    ok.

map_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    F = fun(Row) -> Row end,
    
    [{<<"hello1">>,10},{<<"hello2">>,11},{<<"hello3">>,12},{<<"hello4">>,13}] 
        = esqlite3:map(F, "select * from test_table", Db),

    %% Test that when the row-names are added..
    Assoc = fun(Names, Row) -> 
		    lists:zip(tuple_to_list(Names), tuple_to_list(Row))
	    end,

    [[{one,<<"hello1">>},{two,10}],
     [{one,<<"hello2">>},{two,11}],
     [{one,<<"hello3">>},{two,12}],
     [{one,<<"hello4">>},{two,13}]]  = esqlite3:map(Assoc, "select * from test_table", Db),
    
    ok.

error1_msg_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    
    %% Not sql.
    {error, {sqlite_error, _Msg1}} = esqlite3:exec("dit is geen sql", Db),
    
    %% Database test does not exist.
    {error, {sqlite_error, _Msg2}} = esqlite3:exec("select * from test;", Db),

    %% Opening non-existant database.
    {error, {cantopen, _Msg3}} = esqlite3:open("/dit/bestaat/niet"),
    ok.
    
sqlite_version_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare("select sqlite_version() as sqlite_version;", Db),
    {sqlite_version} =  esqlite3:column_names(Stmt),
    ?assertEqual({row, {<<"3.8.11">>}}, esqlite3:step(Stmt)),
    ok.

sqlite_source_id_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare("select sqlite_source_id() as sqlite_source_id;", Db),
    {sqlite_source_id} =  esqlite3:column_names(Stmt),
    ?assertEqual({row, {<<"2015-07-23 16:39:33 793e206f9032d9205bdb3f447b136bed9a25fa22">>}}, esqlite3:step(Stmt)),
    ok.
    
