-module(epgsql_wrapper).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% API
-export([simpleQuery/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    lager:start(),
    Host = application:get_env(epgsql_wrapper, host, "localhost"),
    Port = application:get_env(epgsql_wrapper, port, 5432),
    User = application:get_env(epgsql_wrapper, user, "user"),
    Pass = application:get_env(epgsql_wrapper, pass, "pass"),
    Db = application:get_env(epgsql_wrapper, db, "test"),
    Timeout = application:get_env(epgsql_wrapper, timeout, 5000),
    Args = [{host, Host}, {port, Port}, {user, User}, {pass, Pass}, {db, Db}, {timeout, Timeout}],
    epgsql_wrapper_sup:start_link([Args]).

stop(_State) ->
    lager:stop(),
    ok.

%% ===================================================================
%% API
%% ===================================================================

simpleQuery(SQL) ->
    epgsql_wrapper_worker:simpleQuery(SQL).