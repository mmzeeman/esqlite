%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").

-define(DB1, "./test/dbs/temp_db1.db").
-define(DB2, "./test/dbs/temp_db2.db").

open_single_database_test() ->
    cleanup(),
    {ok, _C1} = esqlite3:open(?DB1),
    ok.

close_test() ->
    %% Open and close the database immediately
    {ok, C} = esqlite3:open(":memory:"),
    ok = esqlite3:close(C),

    %% Double close should also work.
    ok = esqlite3:close(C),

    %% Check if functions still return sensible values.
    ?assertError(closed, esqlite3:set_update_hook(C, self())),
    ?assertError(closed, esqlite3:changes(C)),
    ?assertError(closed, esqlite3:get_autocommit(C)),
    ?assertError(closed, esqlite3:last_insert_rowid(C)),

    ?assertEqual({error, 21},
                 esqlite3:exec(C, "create table test(one, two, three)")),

    ok.

prepare_test() ->
    {ok, C} = esqlite3:open(":memory:"),
    ?assertMatch({ok, {esqlite3_stmt, _}}, esqlite3:prepare(C, "select 1")),
    ok = esqlite3:close(C),
    ok.

prepare_after_close_test() ->
    {ok, C} = esqlite3:open(":memory:"),
    ?assertEqual(ok, esqlite3:close(C)),
    ?assertMatch({error, 21}, esqlite3:prepare(C, "select 1")),
    ok.

column_names_test() ->
    {ok, C} = esqlite3:open(":memory:"),

    {ok, Stmt} = esqlite3:prepare(C, "select 1 as one"),
    ?assertEqual([<<"one">>], esqlite3:column_names(Stmt)),

    {ok, Stmt1} = esqlite3:prepare(C, <<"select 1 as 😀"/utf8>>),
    ?assertEqual([<<"😀"/utf8>>], esqlite3:column_names(Stmt1)),
    
    {ok, Stmt2} = esqlite3:prepare(C, <<"select 1">>),
    ?assertEqual([<<"1">>], esqlite3:column_names(Stmt2)),

    {ok, Stmt3} = esqlite3:prepare(C, <<"select 1, 2, 3">>),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>], esqlite3:column_names(Stmt3)),

    ok.

column_decltypes_test() ->
    {ok, C} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare(C, "select 1, 2, 3"),
    ?assertEqual([undefined, undefined, undefined], esqlite3:column_decltypes(Stmt)),

    ok.

step_test() ->
    {ok, C} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare(C, "select 1, 2, 3;" ),

    ?assertEqual([1,2,3], esqlite3:step(Stmt)),
    ?assertEqual('$done', esqlite3:step(Stmt)),

    %% After the done, the statement is reset and 
    ?assertEqual([1,2,3], esqlite3:step(Stmt)),
    ?assertEqual('$done', esqlite3:step(Stmt)),

    ok.

iodata_test() ->
    {ok, C} = esqlite3:open(":memory:"),

    ?assertError(badarg, esqlite3:exec(C, 1000)),
    ?assertError(badarg, esqlite3:exec(C, 1000)),

    ok.

open_multiple_same_databases_test() ->
    cleanup(),

    %% Sqlite allows opening the same file multiple
    %% times
    {ok, _C1} = esqlite3:open(?DB1),
    {ok, _C2} = esqlite3:open(?DB1),

    cleanup(),
    ok.

open_multiple_different_databases_test() ->
    cleanup(),
    {ok, _C1} = esqlite3:open(?DB1),
    {ok, _C2} = esqlite3:open(?DB2),
    cleanup(),
    ok.

get_autocommit_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    %% By default, the database is in autocommit mode
    true = esqlite3:get_autocommit(Db),
    ok = esqlite3:exec(Db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val STRING);"),
    true = esqlite3:get_autocommit(Db),

    %% After a begin statement, the connection will not be in autocommit mode anymore
    ok = esqlite3:exec(Db, "BEGIN;"),
    false = esqlite3:get_autocommit(Db),
    ok = esqlite3:exec(Db, "INSERT INTO test (val) VALUES ('this is a test');"),
    ok = esqlite3:exec(Db, "COMMIT;"),

    %% After a commit statement, the connection will be in autocommit mode
    true = esqlite3:get_autocommit(Db),

    ok.

last_insert_rowid_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec(Db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val STRING);"),
    ok = esqlite3:exec(Db, "INSERT INTO test (val) VALUES ('this is a test');"),
    1 = esqlite3:last_insert_rowid(Db),
    ok = esqlite3:exec(Db, "INSERT INTO test (val) VALUES ('this is another test');"),
    2 = esqlite3:last_insert_rowid(Db),
    ok.

update_hook_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:set_update_hook(Db, self()),

    ok = esqlite3:exec(Db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val STRING);"),
    ok = esqlite3:exec(Db, "INSERT INTO test (val) VALUES ('this is a test');"),

    ok = receive {insert, <<"main">>, <<"test">>, 1} -> ok after 150 -> no_message end,

    ok = esqlite3:exec(Db, "UPDATE test SET val = 'a new test' WHERE id = 1;"),

    ok = receive {update, <<"main">>, <<"test">>, 1} -> ok after 150 -> no_message end,

    ok = esqlite3:exec(Db, "DELETE FROM test WHERE id = 1;"),

    ok = receive {delete, <<"main">>, <<"test">>, 1} -> ok after 150 -> no_message end,

    ok = esqlite3:set_update_hook(Db, undefined),

    ok = esqlite3:exec(Db, "INSERT INTO test (val) VALUES ('this is a test');"),
    no_message = receive {insert, <<"main">>, <<"test">>, 1} -> ok after 150 -> no_message end,

    ok.

simple_query_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "insert into test_table values('hello1', 10);"),
    ?assertEqual(1, esqlite3:changes(Db)),

    ok = esqlite3:exec(Db, "insert into test_table values('hello2', 11);"),
    ?assertEqual(1, esqlite3:changes(Db)),
    ok = esqlite3:exec(Db, "insert into test_table values('hello3', 12);"),
    ?assertEqual(1, esqlite3:changes(Db)),
    ok = esqlite3:exec(Db, "insert into test_table values('hello4', 13);"),
    ?assertEqual(1, esqlite3:changes(Db)),
    ok = esqlite3:exec(Db, "commit;"),
    ok = esqlite3:exec(Db, "select * from test_table;"),

    ok = esqlite3:exec(Db, "delete from test_table;"),
    ?assertEqual(4, esqlite3:changes(Db)),

    ok.

prepare2_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    esqlite3:exec(Db, "begin;"),
    esqlite3:exec(Db, "create table test_table(one varchar(10), two integer);"),
    {ok, Statement} = esqlite3:prepare(Db, "insert into test_table values('one', 1)"),

    '$done' = esqlite3:step(Statement),
    1 = esqlite3:changes(Db),

    ok = esqlite3:exec(Db, "insert into test_table values('two', 2);"),

    %% Check if the values are there.
    ?assertEqual([[<<"one">>, 1],
                  [<<"two">>, 2]],
                 esqlite3:q(Db, "select one, two from test_table order by two asc")),
    esqlite3:exec(Db, "commit;"),
    esqlite3:close(Db),

    ok.

bind_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "commit;"),

    %% Create a prepared statement
    {ok, Statement} = esqlite3:prepare(Db, "insert into test_table values(?1, ?2)"),
    ok = esqlite3:bind(Statement, [one, 2]),
    '$done' = esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, ["three", 4]),
    esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, ["five", 6]),
    esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, [[<<"se">>, $v, "en"], 8]), % iolist bound as text
    esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, [<<"nine">>, 10]), % iolist bound as text
    esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, [{blob, [<<"eleven">>, 0]}, 12]), % iolist bound as blob with trailing eos.
    esqlite3:step(Statement),
    ok = esqlite3:bind(Statement, ["empty", undefined]), % 'undefined' is converted to SQL null
    esqlite3:step(Statement),

    %% int64
    ok = esqlite3:bind(Statement, [int64, 308553449069486081]),
    esqlite3:step(Statement),
%
    %% negative int64
    ok = esqlite3:bind(Statement, [negative_int64, -308553449069486081]),
    esqlite3:step(Statement),

    %% utf-8
    ok = esqlite3:bind(Statement, [[<<228,184,138,230,181,183>>], 100]),
    esqlite3:step(Statement),

    ?assertEqual([[<<"one">>, 2]],
        esqlite3:q(Db, "select one, two from test_table where two = '2'")),
    ?assertEqual([[<<"three">>, 4]],
        esqlite3:q(Db, "select one, two from test_table where two = 4")),
    ?assertEqual([[<<"five">>, 6]],
        esqlite3:q(Db, "select one, two from test_table where two = 6")),
    ?assertEqual([[<<"seven">>, 8]],
        esqlite3:q(Db, "select one, two from test_table where two = 8")),
    ?assertEqual([[<<"nine">>, 10]],
        esqlite3:q(Db, "select one, two from test_table where two = 10")),
    ?assertEqual([[<<$e,$l,$e,$v,$e,$n,0>>, 12]],
        esqlite3:q(Db, "select one, two from test_table where two = 12")),
    ?assertEqual([[<<"empty">>, undefined]], 
        esqlite3:q(Db, "select one, two from test_table where two is null")),

    ?assertEqual([[<<"int64">>, 308553449069486081]],
        esqlite3:q(Db, "select one, two from test_table where one = 'int64';")),
    ?assertEqual([[<<"negative_int64">>, -308553449069486081]],
        esqlite3:q(Db, "select one, two from test_table where one = 'negative_int64';")),

    %% utf-8
    ?assertEqual([[<<228,184,138,230,181,183>>, 100]],
        esqlite3:q(Db, "select one, two from test_table where two = 100")),

    ok.

bind_for_queries_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "commit;"),

    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>,
                                   [test_table])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>,
                                   ["test_table"])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>,
                                   [<<"test_table">>])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>,
                                   [[<<"test_table">>]])),

    ok.

named_bind_for_queries_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "commit;"),

    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=:name;">>,
                                   [{<<":name">>, "test_table"}])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=$name;">>,
                                   [{"$name", test_table}])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=@name;">>,
                                   [{"@name", <<"test_table">>}])),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=:name;">>,
                                   #{":name" => test_table})),
    ?assertEqual([[1]], esqlite3:q(Db, <<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=:name;">>,
                                   [{text, ":name", "test_table"}])),

    ok.

column_names2_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "insert into test_table values('hello1', 10);"),
    ok = esqlite3:exec(Db, "insert into test_table values('hello2', 20);"),
    ok = esqlite3:exec(Db, "commit;"),

    %% All columns
    {ok, Stmt} = esqlite3:prepare(Db, "select * from test_table"),
    [<<"one">>, <<"two">>] =  esqlite3:column_names(Stmt),
    [<<"hello1">>, 10] = esqlite3:step(Stmt),
    [<<"one">>, <<"two">>] =  esqlite3:column_names(Stmt),
    [<<"hello2">>, 20] = esqlite3:step(Stmt),
    [<<"one">>, <<"two">>] =  esqlite3:column_names(Stmt),
    '$done' = esqlite3:step(Stmt),
    [<<"one">>, <<"two">>] =  esqlite3:column_names(Stmt),

    %% One column
    {ok, Stmt2} = esqlite3:prepare(Db, "select two from test_table"),
    [<<"two">>] =  esqlite3:column_names(Stmt2),
    [10] = esqlite3:step(Stmt2),
    [<<"two">>] =  esqlite3:column_names(Stmt2),
    [20] = esqlite3:step(Stmt2),
    [<<"two">>] =  esqlite3:column_names(Stmt2),
    '$done' = esqlite3:step(Stmt2),
    [<<"two">>] =  esqlite3:column_names(Stmt2),

    %% No columns
    {ok, Stmt3} = esqlite3:prepare(Db, "values(1);"),
    [<<"column1">>] =  esqlite3:column_names(Stmt3),
    [1] = esqlite3:step(Stmt3),
    [<<"column1">>] =  esqlite3:column_names(Stmt3),

    %% Things get a bit weird when you retrieve the column name
    %% when calling an aggragage function.
    {ok, Stmt4} = esqlite3:prepare(Db, "select date('now');"),
    [<<"date(\'now\')">>] =  esqlite3:column_names(Stmt4),
    [Date] = esqlite3:step(Stmt4),
    true = is_binary(Date),

    %% Some statements have no column names
    {ok, Stmt5} = esqlite3:prepare(Db, "create table dummy(a, b, c);"),
    [] = esqlite3:column_names(Stmt5),

    ok.

column_types_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table test_table(one varchar(10), two int);"),
    ok = esqlite3:exec(Db, "insert into test_table values('hello1', 10);"),
    ok = esqlite3:exec(Db, "insert into test_table values('hello2', 20);"),
    ok = esqlite3:exec(Db, "commit;"),

    %% All columns
    {ok, Stmt} = esqlite3:prepare(Db, "select * from test_table"),
    ?assertEqual([<<"varchar(10)">>, <<"INT">>], esqlite3:column_decltypes(Stmt)),

    %% Some statements have no column types
    {ok, Stmt2} = esqlite3:prepare(Db, "create table dummy(a, b, c);"),
    [] = esqlite3:column_decltypes(Stmt2),

    {ok, Stmt3} = esqlite3:prepare(Db, "select 1, 2, 3;"),
    [undefined, undefined, undefined] = esqlite3:column_decltypes(Stmt3),

    ok.

nil_column_decltypes_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec(Db, "begin;"),
    ok = esqlite3:exec(Db, "create table t1(c1 variant);"),
    ok = esqlite3:exec(Db, "commit;"),

    {ok, Stmt} = esqlite3:prepare(Db, "select c1 + 1, c1 from t1"),
    ?assertEqual([undefined, <<"variant">>], esqlite3:column_decltypes(Stmt)),

    ok.

reset_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    {ok, Stmt} = esqlite3:prepare(Db, "select * from (values (1), (2));"),
    [1] = esqlite3:step(Stmt),

    ok = esqlite3:reset(Stmt),

    [1] = esqlite3:step(Stmt),
    [2] = esqlite3:step(Stmt),
    '$done' = esqlite3:step(Stmt),

    % After a done the statement is automatically reset.
    [1] = esqlite3:step(Stmt),

    % Calling reset multiple times...
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),
    ok = esqlite3:reset(Stmt),

    % The statement should still be reset.
    [1] = esqlite3:step(Stmt),

    ok.

error1_msg_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    %% Not sql.
    {error, 1} = esqlite3:exec(Db, "dit is geen sql"),

    %% Database test does not exist.
    {error, 1} = esqlite3:exec(Db, "select * from test;"),

    %% Opening non-existant database.
    {error, 14} = esqlite3:open("/dit/bestaat/niet"),

    ok.

prepare_and_close_connection_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    [] = esqlite3:q(Db, "create table test(one, two, three)"),
    ok = esqlite3:exec(Db, ["insert into test values(1,2,3);"]),
    {ok, Stmt} = esqlite3:prepare(Db, "select * from test"),

    %% The prepated statment works.
    [1,2,3] = esqlite3:step(Stmt),
    '$done' = esqlite3:step(Stmt),

    ok = esqlite3:close(Db),
    ok = esqlite3:reset(Stmt),

    %% Internally sqlite3_close_v2 is used by the nif. This will destruct the
    %% connection when the last perpared statement is finalized
    [1,2,3] = esqlite3:step(Stmt),
    '$done' = esqlite3:step(Stmt),

    ok.

backup_test() ->
    cleanup(),

    {ok, Dest} = esqlite3:open(?DB1),
    {ok, Source} = esqlite3:open(?DB2),

    {ok, Backup} = esqlite3:backup_init(Dest, <<"main">>, Source, <<"main">>),

    0 = esqlite3:backup_remaining(Backup),
    0 = esqlite3:backup_pagecount(Backup),

    '$done' = esqlite3:backup_step(Backup, 1),

    cleanup(),

    ok.

backup1_test() ->
    cleanup(),

    {ok, Dest} = esqlite3:open(?DB1),
    {ok, Source} = esqlite3:open(?DB2),

    [] = esqlite3:q(Source, "create table test(one, two)"),
    [] = esqlite3:q(Source, "begin;"),
    [] = esqlite3:q(Source, "insert into test values(randomblob(10000), randomblob(10000));"),
    [] = esqlite3:q(Source, "insert into test values(randomblob(10000), randomblob(10000));"),
    [] = esqlite3:q(Source, "insert into test values(randomblob(10000), randomblob(10000));"),
    [] = esqlite3:q(Source, "insert into test values(randomblob(10000), randomblob(10000));"),
    [] = esqlite3:q(Source, "insert into test values(randomblob(10000), randomblob(10000));"),
    [] = esqlite3:q(Source, "commit;"),
    
    [[5]] = esqlite3:q(Source, "select count(*) from test"),
    {error, 1} = esqlite3:q(Dest, "select count(*) from test"),
    #{ errmsg := <<"no such table: test">> }  = esqlite3:error_info(Dest),

    {ok, Backup} = esqlite3:backup_init(Dest, "main", Source, "main"),

    0 = esqlite3:backup_remaining(Backup),
    0 = esqlite3:backup_pagecount(Backup),

    %% Backup 1 page.
    ok = esqlite3:backup_step(Backup, 1),

    26 = esqlite3:backup_remaining(Backup),
    27 = esqlite3:backup_pagecount(Backup),

    %% Do all the remaining pages.
    '$done' = esqlite3:backup_step(Backup, -1),

    0 = esqlite3:backup_remaining(Backup),
    27 = esqlite3:backup_pagecount(Backup),

    ok = esqlite3:backup_finish(Backup),

    [[5]] = esqlite3:q(Dest, "select count(*) from test"),

    cleanup(),

    ok.
    

sqlite_version_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare(Db, "select sqlite_version() as sqlite_version;"),
    [<<"sqlite_version">>] = esqlite3:column_names(Stmt),
    ?assertEqual([<<"3.41.0">>], esqlite3:step(Stmt)),
    ok.

sqlite_source_id_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    {ok, Stmt} = esqlite3:prepare(Db, "select sqlite_source_id() as sqlite_source_id;"),
    [<<"sqlite_source_id">>] = esqlite3:column_names(Stmt),
    ?assertEqual([<<"2023-02-21 18:09:37 05941c2a04037fc3ed2ffae11f5d2260706f89431f463518740f72ada350866d">>],
                 esqlite3:step(Stmt)),
    ok.

interrupt_on_timeout_test() ->
    {ok, Db1} = esqlite3:open("file:memdb1?mode=memory&cache=shared"),
    % {ok, Db2} = esqlite3:open("file:memdb1?mode=memory&cache=shared"),

    Self = self(),

    F = fun() ->
                CreateTableQuery = "CREATE TABLE all_numbers_in_the_world (number int not null);",
                ok = esqlite3:exec(Db1, CreateTableQuery),
                VeryLongQuery = "
                    WITH RECURSIVE
                    for(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM for WHERE i < 10000000)
                    INSERT INTO all_numbers_in_the_world SELECT i FROM for;
               ",

               %% The query was interrupted
               Self ! {msg, esqlite3:exec(Db1, VeryLongQuery)}
        end,

    spawn(F),
    timer:sleep(10),
    ok = esqlite3:interrupt(Db1),

    %% The query was interrupted, so no result
    ?assertEqual([[0]], esqlite3:q(Db1, "SELECT COUNT(*) FROM all_numbers_in_the_world")),

    %% We should have gotten an interrupt error.
    Msg =  receive {msg, M} -> M end,
    ?assertEqual({error, 9}, Msg),
    
    ok.

garbage_collect_test() ->
    F = fun() ->
                {ok, Db} = esqlite3:open(":memory:"),
                [] = esqlite3:q(Db, "create table test(one, two, three)"),
                [] = esqlite3:q(Db, "insert into test values(1, '2', 3.0)"), 
                {ok, Stmt} = esqlite3:prepare(Db, "select * from test"),
                [1, <<"2">>, 3.0] = esqlite3:step(Stmt),
                '$done' = esqlite3:step(Stmt),
                ok = esqlite3:close(Db)
        end,

    [spawn(F) || _X <- lists:seq(0,30)],
    receive after 500 -> ok end,
    erlang:garbage_collect(),

    [spawn(F) || _X <- lists:seq(0,30)],
    receive after 500 -> ok end,
    erlang:garbage_collect(),

    ok.

%%
%% Helpers
%%

cleanup() ->
    rm_rf(?DB1),
    rm_rf(?DB2).

rm_rf(Filename) ->
    case file:delete(Filename) of
        ok -> ok;
        {error, _} -> ok
    end.

