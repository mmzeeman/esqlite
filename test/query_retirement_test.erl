%% q owns its prepared statement; callers must not need garbage collection
%% before close releases the native WAL connection. A second connection's
%% transition out of WAL requires all original connections to have closed.
-module(query_retirement_test).
-include_lib("eunit/include/eunit.hrl").

query_success_retirement_test() ->
    retired(fun(C) ->
        ?assertEqual([[1]], esqlite3:q(C, "SELECT value FROM items"))
    end).

query_arguments_retirement_test() ->
    retired(fun(C) ->
        ?assertEqual([[1]], esqlite3:q(C,
            "SELECT value FROM items WHERE value = ?", [1]))
    end).

query_bind_error_retirement_test() ->
    retired(fun(C) ->
        ?assertEqual({error, 25}, esqlite3:q(C,
            "SELECT value FROM items WHERE value = ?", [1, 2])),
        ?assertMatch(#{errcode := 25, extended_errcode := 25,
            errmsg := <<"column index out of range">>},
            esqlite3:error_info(C))
    end).

query_step_error_retirement_test() ->
    retired(fun(C) ->
        ?assertEqual({error, 2067}, esqlite3:q(C,
            "INSERT INTO items(value) VALUES (?)", [1])),
        ?assertMatch(#{errcode := 2067, extended_errcode := 2067,
            errmsg := <<"UNIQUE constraint failed: items.value">>},
            esqlite3:error_info(C))
    end).

query_bind_exception_retirement_test() ->
    retired(fun(C) ->
        ?assertError(badarg, esqlite3:q(C,
            "SELECT value FROM items WHERE value = ?", [{int, <<"invalid">>}]))
    end).

retired(Query) ->
    Path = filename:join("test/dbs", "query-retirement-" ++
        integer_to_list(erlang:unique_integer([positive, monotonic])) ++ ".db"),
    ok = filelib:ensure_dir(Path),
    {ok, C} = esqlite3:open(Path),
    try
        ok = esqlite3:exec(C, "PRAGMA journal_mode=WAL;"
            "CREATE TABLE items(value INTEGER UNIQUE);"
            "INSERT INTO items VALUES(1);"),
        Query(C),
        ok = esqlite3:close(C),
        ok = esqlite3:close(C),

        %% No forced collection, process death, or sleep stands in for close.
        {ok, Probe} = esqlite3:open(Path),
        try
            ?assertEqual(ok, esqlite3:exec(Probe, "PRAGMA journal_mode=DELETE"))
        after
            ok = esqlite3:close(Probe)
        end
    after
        ok = esqlite3:close(C),
        _ = file:delete(Path),
        _ = file:delete(Path ++ "-wal"),
        _ = file:delete(Path ++ "-shm")
    end.
