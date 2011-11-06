%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011 Maas-Maarten Zeeman

%% @doc Low level erlang API for sqlite3 databases

%% Copyright 2011 Maas-Maarten Zeeman
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(esqlite3_nif).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% low-level exports
-export([start/0, 
	 open/4, 
	 exec/4, 
	 prepare/4,
	 step/3,
	 bind/4,
	 close/3
]).

-on_load(init/0).

init() ->
    ok = erlang:load_nif(code:priv_dir(esqlite) ++ "/esqlite3_nif", 0).

%% @doc Start a low level thread which will can handle sqlite3 calls. 
%%
%% @spec 
start() ->
    exit(nif_library_not_loaded).

%% @doc Open a connection to 
%%
open(_Db, _Ref, _Dest, _Filename) ->
    exit(nif_library_not_loaded).

%% @doc
exec(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
prepare(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
step(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc
bind(_Stmt, _Ref, _Dest, _Args) ->
    exit(nif_library_not_loaded).

%% @doc
close(_Db, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

    



