-module(epgsql_wrapper_sup).

-behaviour(supervisor).

%% API
-export([start_link/1, add_child/3]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_WITH_ARGS(I, Type, Args), {I, {I, start_link, Args}, transient, 5000, Type, [I]}).
-define(STATE, state).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

add_child(Module, Type, Args) ->
    Worker = ?CHILD_WITH_ARGS(Module, Type, Args),
    {ok, ChildPid} = supervisor:start_child(?MODULE, Worker),
    ets:insert(?STATE, [{worker_pid, ChildPid}]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    Manager = ?CHILD_WITH_ARGS(epgsql_wrapper_manager, worker, Args),
    RestartStrategy = {one_for_one, 5, 60},
    Childs = [Manager],
    {ok, { RestartStrategy, Childs} }.

