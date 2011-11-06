%%
%% Exploring making a database driver for the generic gen_db interface
%%

-module(sqlite).

-behaviour(gen_db).

-export([handle_open/1, handle_execute/3, handle_close/1]).

%% @doc Open a database connection
%%
handle_open(DatabaseName) ->
    esqlite3:open(DatabaseName).

%% @doc Execute a query and return the results
%%
handle_execute(Operation, Args, Connection) ->
    {ok, Stmt} = esqlite3:prepare(Connection, Operation),
    ok = esqlite3:bind(Stmt, Args),
    Answer = execute(Stmt),
    %% TODO Finalize the statement.
    Answer.


%% @doc Close the connection
%%
handle_close(Connection) ->
    esqlite:close(Connection).

%% @doc
%%
execute(Statement) ->
    execute(Statement, [], 0).

%% @doc
%%
execute(_Statement, _Acc, Tries) when Tries > 5 ->
    throw(too_many_tries);
execute(Statement, Acc, Tries) ->
    case esqlite3:step(Statement) of
	'$done' ->
	    lists:reverse(Acc);
	'$busy' ->
	    timer:sleep(100), %% This is a bit lame... there is a trigger api for this.
	    execute(Statement, Acc, Tries + 1);
	V when is_tuple(V) ->
	    execute(Statement, [V | Acc], 0)
    end.
