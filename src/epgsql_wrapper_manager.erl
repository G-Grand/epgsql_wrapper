-module(epgsql_wrapper_manager).

-behaviour(gen_server).

%% API
-export([start_link/1, startConn/0, simpleQuery/1, extendedQuery/2, prepareStatement/3, bindToStatement/3, executeStatement/3,
  closeStatement/1, closePortalOrStatement/2, batchExecuteStatements/1, stop/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(STATE, state).
-define(CONN_POOL, pgsql_pool).

-record(state, { mode = plain, args = null }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

startConn() ->
  gen_server:call(?SERVER, start, infinity).

simpleQuery(SQL) ->
  gen_server:call(?SERVER, {simple_query, SQL}, infinity).

extendedQuery(SQL, Params) ->
  gen_server:call(?SERVER, {extended_query, SQL, Params}, infinity).

prepareStatement(Name, SQL, ParamsTypes) ->
  gen_server:call(?SERVER, {prepare_statement, Name, SQL, ParamsTypes}, infinity).

bindToStatement(Statement, PortalName, ParamsVals) ->
  gen_server:call(?SERVER, {bind_to_statement, Statement, PortalName, ParamsVals}, infinity).

executeStatement(Statement, PortalName, MaxRows) ->
  gen_server:call(?SERVER, {exec_statement, Statement, PortalName, MaxRows}, infinity).

batchExecuteStatements(BatchData) ->
  gen_server:call(?SERVER, {batch_exec_statements, BatchData}, infinity).

closeStatement(Statement) ->
  gen_server:call(?SERVER, {close_statement, Statement}, infinity).

closePortalOrStatement(Type, Name) when Type == statement; Type == portal ->
  gen_server:call(?SERVER, {close_statement_or_portal, Type, Name}, infinity);
closePortalOrStatement(_Type, _Name) ->
  {error, badarg}.

stop() ->
  gen_server:call(?SERVER, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
  Mode = proplists:get_value(mode, Args, plain),
  {ok, #state{ mode = Mode, args = Args }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------

handle_call(start, _From, State) ->
  Mode = State#state.mode,
  Args = State#state.args,
  case Mode of
    plain ->
      epgsql_wrapper_sup:add_child(epgsql_wrapper_worker, worker, [Args]);
    pool ->
      PoolSize = proplists:get_value(pool_size, Args, 1),
      PoolArgs = [{?CONN_POOL, [{size, PoolSize }, {max_overflow, 0 }], Args}],
      epgsql_wrapper_pool_sup:start_link(PoolArgs)
  end,
  {reply, ok, State};

handle_call({simple_query, SQL}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {simple_query, SQL}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {simple_query, SQL}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({extended_query, SQL, Params}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {extended_query, SQL, Params}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {extended_query, SQL, Params}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({prepare_statement, Name, SQL, ParamsTypes}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {prepare_statement, Name, SQL, ParamsTypes}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {prepare_statement, Name, SQL, ParamsTypes}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({bind_to_statement, Statement, PortalName, ParamsVals}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {bind_to_statement, Statement, PortalName, ParamsVals}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {bind_to_statement, Statement, PortalName, ParamsVals}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({exec_statement, Statement, PortalName, MaxRows}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {exec_statement, Statement, PortalName, MaxRows}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {exec_statement, Statement, PortalName, MaxRows}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({batch_exec_statements, BatchData}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {batch_exec_statements, BatchData}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {batch_exec_statements, BatchData}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({close_statement, Statement}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {close_statement, Statement}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {close_statement, Statement}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call({close_statement_or_portal, Type, Name}, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, {close_statement_or_portal, Type, Name}, infinity);
      pool ->
        poolboy:transaction(?CONN_POOL, fun(Worker) ->
          gen_server:call(Worker, {close_statement_or_portal, Type, Name}, infinity)
        end, infinity)
    end,
  {reply, Reply, State};

handle_call(stop, _From, State) ->
  Reply =
    case State#state.mode of
      plain ->
        [{worker_pid, WorkerPid}] = ets:lookup(?STATE, worker_pid),
        gen_server:call(WorkerPid, stop, infinity);
      pool ->
        poolboy:stop(?CONN_POOL)
    end,
  {stop, normal, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
