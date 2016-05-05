-module(epgsql_wrapper).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, stop/0]).

%% API
-export([simpleQuery/1, simpleQueryAsync/2, simpleQueryIterator/2, extendedQuery/2, extendedQueryAsync/3,
  extendedQueryIterator/3,prepareStatement/3, bindToStatement/3, executeStatement/3, closeStatement/1,
  closePortalOrStatement/2, batchExecuteStatements/1]).

-define(STATE, state).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    %%lager:start(),
    ets:new(?STATE, [named_table, set, public]),
    Host = application:get_env(epgsql_wrapper, host, "localhost"),
    Port = application:get_env(epgsql_wrapper, port, 5433),
    User = application:get_env(epgsql_wrapper, user, "postgres"),
    Pass = application:get_env(epgsql_wrapper, pass, "postgres"),
    Db = application:get_env(epgsql_wrapper, db, "test"),
    Timeout = application:get_env(epgsql_wrapper, timeout, 5000),
    Mode = application:get_env(epgsql_wrapper, mode, plain),
    PoolSize = application:get_env(epgsql_wrapper, pool_size, 1),
    Args = [{host, Host}, {port, Port}, {user, User}, {pass, Pass}, {db, Db}, {timeout, Timeout}, {mode, Mode},
      {pool_size, PoolSize}],
    SupStartRes = epgsql_wrapper_sup:start_link([Args]),
    epgsql_wrapper_manager:startConn(),
    SupStartRes.

stop(_State) ->
    epgsql_wrapper_manager:stop(),
    ok.

%% ===================================================================
%% API
%% ===================================================================

simpleQuery(SQL) ->
  epgsql_wrapper_manager:simpleQuery(SQL).

simpleQueryAsync(SQL, Caller) ->
  epgsql_wrapper_manager:simpleQueryAsync(SQL, Caller).

simpleQueryIterator(SQL, Caller) ->
  epgsql_wrapper_manager:simpleQueryIterator(SQL, Caller).

extendedQuery(SQL, Params) ->
  epgsql_wrapper_manager:extendedQuery(SQL, Params).

extendedQueryAsync(SQL, Params, Caller) ->
  epgsql_wrapper_manager:extendedQueryAsync(SQL, Params, Caller).

extendedQueryIterator(SQL, Params, Caller) ->
  epgsql_wrapper_manager:extendedQueryIterator(SQL, Params, Caller).

prepareStatement(Name, SQL, ParamsTypes) ->
  epgsql_wrapper_manager:prepareStatement(Name, SQL, ParamsTypes).

bindToStatement(Statement, PortalName, ParamsVals) ->
  epgsql_wrapper_manager:bindToStatement(Statement, PortalName, ParamsVals).

executeStatement(Statement, PortalName, MaxRows) ->
  epgsql_wrapper_manager:executeStatement(Statement, PortalName, MaxRows).

batchExecuteStatements(BatchData) ->
  epgsql_wrapper_manager:batchExecuteStatements(BatchData).

closeStatement(Statement) ->
  epgsql_wrapper_manager:closeStatement(Statement).

closePortalOrStatement(Type, Name) ->
  epgsql_wrapper_manager:closePortalOrStatement(Type, Name).

stop() ->
    stop([]).