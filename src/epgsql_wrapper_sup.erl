-module(epgsql_wrapper_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_WITH_ARGS(I, Type, Args), {I, {I, start_link, Args}, transient, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    ConnWorker = ?CHILD_WITH_ARGS(epgsql_wrapper_worker, worker, Args),
    RestartStrategy = {one_for_one, 5, 60},
    Childs = [ConnWorker],
    {ok, { RestartStrategy, Childs} }.

