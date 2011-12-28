%%
%% Exploring making a database driver for the generic gen_db interface
%%

-module(sqlite).

-include("edbc.hrl").

-behaviour(gen_db).

-export([open/1, run/3,  execute/3, close/1, commit/1, rollback/1, tables/1, describe_table/2]).

%% @doc Open a database connection
%%
open([DatabaseName]) ->
    {ok, C} = esqlite3:open(DatabaseName),
    ok = esqlite3:exec(<<"BEGIN TRANSACTION;">>, C),
    {ok, C}.


%% @doc Close the connection
%%
close(Connection) ->
    ok = esqlite3:close(Connection).

%% @doc 
%%
run(Sql, [], Connection) ->
    case esqlite3:exec(Sql, Connection) of
	{error, Error} -> {error, ?MODULE, Error};
	ok -> ok
    end;
run(Sql, Args, Connection) ->
    case esqlite3:prepare(Sql, Connection) of
	{error, Error} ->
	    {error, ?MODULE, Error};
	{ok, Stmt} ->
	    case esqlite3:bind(Stmt, Args) of
		{error, Error} -> {error, ?MODULE, Error};
		ok ->
		    case esqlite3:fetchone(Stmt) of
			{error, Error} -> {error, ?MODULE, Error};
			_ -> ok
		    end
	    end
    end.

%% @doc Execute a query and return the results
%%
execute(Sql, [], Connection) ->
    case esqlite3:prepare(Sql, Connection) of
	{error, Error} ->
	    {error, ?MODULE, Error};
	{ok, Stmt} ->
	    {ok, esqlite3:column_names(Stmt), esqlite3:fetchall(Stmt)}
    end;
execute(Sql, Args, Connection) ->
    case esqlite3:prepare(Sql, Connection) of
	{error, Error} ->
	    {error, ?MODULE, Error};
	{ok, Stmt} ->
	    case esqlite3:bind(Stmt, Args) of
		ok ->
		    Names = esqlite3:column_names(Stmt),
		    Result = esqlite3:fetchall(Stmt),
		    {ok, Names, Result};
		{error, Error} ->
		    {error, ?MODULE, Error}
	    end
    end.

%%
commit(Connection) ->
    ok = run(<<"COMMIT;">>, [], Connection).

%%
rollback(Connection) ->
    ok = run(<<"ROLLBACK;">>, [], Connection).

%% 
tables(Connection) ->
    esqlite3:map(fun({TableName}) -> list_to_atom(TableName) end, 
		 <<"SELECT name FROM sqlite_master WHERE type='table' ORDER by name;">>, Connection).

%%
describe_table(TableName, Connection) when is_atom(TableName) ->
    esqlite3:map(fun({_Cid, ColumnName, ColumnType, NotNull, Default, PrimaryKey}) -> 
			 #edbc_column_info{name=list_to_atom(ColumnName),
					   type=ColumnType,
					   default=Default,
					   notnull=NotNull =/= 0,
					   pk=PrimaryKey =/= 0}
		 end,
		 [<<"PRAGMA table_info('">>, atom_to_list(TableName), <<"');">>], Connection).

