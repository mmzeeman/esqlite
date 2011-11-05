%%
%% Generic database interface. Sort of...
%%
%% Inspired by python's db api
%%

-module(gen_db).

-export([behaviour_info/1]).

-export([open/2, execute/2, execute/3, close/1]).

behaviour_info(callbacks) ->
    [{handle_open, 1},
     {handle_execute, 3}, 
     {handle_close, 1}];
behaviour_info(_Other) ->
    undefined.

-record(gen_connection, {module, connection}).

%% @doc Open a connection to a new database
%%
open(Module, ModuleArgs) ->
    {ok, Conn} = Module:handle_open(ModuleArgs),
    {ok, #gen_connection{module=Module, connection=Conn}}.
    
%% @doc Prepare and execute a database operation
%%
execute(Operation, Connection) ->
    execute(Operation, [], Connection).

%% @doc Prepare and execute a database operation
%%
execute(Operation, Args, #gen_connection{module=Module, connection=Connection}) ->    
    Module:handle_execute(Operation, Args, Connection).

%% @doc Close a database connection.
%%
close(#gen_connection{module=Module, connection=Connection}) ->
    Module:handle_close(Connection).
