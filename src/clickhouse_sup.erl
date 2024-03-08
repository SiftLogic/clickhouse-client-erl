-module(clickhouse_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Pools = application:get_env(clickhouse, pools, []),
    [ok = pooler:new_pool(P) || P <- Pools],
    {ok, {{one_for_one, 10, 100}, []}}.
