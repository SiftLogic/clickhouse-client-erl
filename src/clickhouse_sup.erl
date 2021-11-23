-module(clickhouse_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolerSup = {pooler_sup,
                 {pooler_sup, start_link, []},
                 permanent,
                 infinity,
                 supervisor,
                 [pooler_sup]},
    {ok, {{one_for_one, 10, 100}, [PoolerSup]}}.
