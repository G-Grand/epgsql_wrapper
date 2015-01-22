-module(epgsql_wrapper_worker).

-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

-include_lib("epgsql/include/pgsql.hrl").

%% API
-export([start_link/1, simpleQuery/1, extendedQuery/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { pg_conn = null }).

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

simpleQuery(SQL) ->
  gen_server:call(?SERVER, {simple_query, SQL}, infinity).

extendedQuery(SQL, Params) ->
  gen_server:call(?SERVER, {extended_query, SQL, Params}, infinity).

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
  Host = proplists:get_value(host, Args, "localhost"),
  Port = proplists:get_value(port, Args, 5432),
  User = proplists:get_value(user, Args, "user"),
  Pass = proplists:get_value(pass, Args, "pass"),
  Db = proplists:get_value(db, Args, "test"),
  Timeout = proplists:get_value(timeout, Args, 5000),
  {ok, Conn} = pgsql:connect(Host, User, Pass, [{database, Db}, {port, Port}, {timeout, Timeout}]),
  {ok, #state{ pg_conn = Conn}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call({simple_query, SQL}, _From, State) ->
  QueryRes = pgsql:squery(State#state.pg_conn, SQL),
  Reply = case QueryRes of
    SomeResult when is_list(SomeResult) == true ->
      BatchResultsParser = fun(BatchItem) ->
        case BatchItem of
          {ok, Columns, Rows} ->
            {ok, mapResult(Columns, Rows)};
          {ok, Count, Columns, Rows} ->
            {ok, Count, mapResult(Columns, Rows)};
          _ ->
            BatchItem
        end
      end,
      {ok, batch_res, lists:map(BatchResultsParser, SomeResult)};
    {ok, Columns, Rows} ->
      {ok, mapResult(Columns, Rows)};
    {ok, Count, Columns, Rows} ->
      {ok, Count, mapResult(Columns, Rows)};
    _ ->
      QueryRes
  end,
  {reply, Reply, State};

handle_call({extended_query, SQL, Params}, _From, State) ->
  QueryRes = case Params of
    [] ->
      pgsql:equery(State#state.pg_conn, SQL);
    _ ->
      pgsql:equery(State#state.pg_conn, SQL, Params)
  end,
  Reply = case QueryRes of
    {ok, Columns, Rows} ->
      {ok, mapResult(Columns, Rows)};
    {ok, Count, Columns, Rows} ->
      {ok, Count, mapResult(Columns, Rows)};
    _ ->
      QueryRes
  end,
  {reply, Reply, State};

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

mapResult([], _Rows) ->
  [];
mapResult(_Columns, []) ->
  [];
mapResult(Columns, Rows) ->
  ColumnsExtractor = fun(Elem) ->
    Elem#column.name
  end,
  ColsNames = lists:map(ColumnsExtractor, Columns),
  ResultsMapper = fun(Row) ->
    lists:zip(ColsNames, tuple_to_list(Row))
  end,
  lists:map(ResultsMapper, Rows).
