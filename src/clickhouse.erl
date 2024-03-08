-module(clickhouse).

-behaviour(gen_server).

-export([make_pool/4,
         query/2,
         query/3,
         execute/2,
         start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("kernel/include/logger.hrl").

-define(CONNECTION_TIMEOUT, 3 * 1000). % 3 s

-define(TIMEOUT, 30 * 1000). % 30 s

-define(BODY_TIMEOUT, 60 * 1000). % 60 s

make_pool(PoolName, Params, Start, Max) ->
    pooler_sup:new_pool(#{name => PoolName,
                         max_count => Max,
                         init_count => Start,
                         start_mfa => {clickhouse, start_link, [Params]}}).

query(Pool, SQL) ->
    query(Pool, iolist_to_binary(SQL), <<>>).

query(Pool, SQL, ReturnFormat) when not is_binary(SQL) ->
    query(Pool, iolist_to_binary(SQL), ReturnFormat);
query(Pool, SQL, ReturnFormat) when is_binary(SQL) ->
    Pid = pooler:take_member(Pool),
    Result = gen_server:call(Pid, {query, SQL, ReturnFormat}),
    pooler:return_member(Pool, Pid, ok),
    Result.

execute(Pool, SQL) ->
    Pid = pooler:take_member(Pool),
    gen_server:cast(Pid, {query, SQL}),
    pooler:return_member(Pool, Pid, ok),
    ok.

%
% gen_server
%

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

init([Opts]) ->
    timer:send_after(0, connect),
    case maps:get(bulk_send_period, Opts, 0) of
        0 ->
            {ok, Opts#{bulk_timer => undefined}};
        N ->
            {ok, BulkTimer} = timer:send_interval(N * 1000,
                                                  bulk_send),
            {ok, Opts#{bulk_timer => BulkTimer, queries => []}}
    end.

handle_call({query, SQL, ReturnFormat}, _From, State) ->
    {reply, make_query(SQL, ReturnFormat, State), State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({query, SQL, _},
            #{bulk_timer := undefined} = State) ->
    Result = make_query(SQL, State),
    ?LOG_DEBUG("Clickhouse result for ~p - ~p",
               [SQL, Result]),
    {noreply, State};
handle_cast({query, SQL}, #{queries := Qs} = State) ->
    {noreply, State#{queries => Qs ++ [SQL]}};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(connect, State) ->
    {noreply, connect(State)};
handle_info({gun_error, Con, _StreamRef, Error},
            #{con := Con} = State) ->
    ?LOG_ERROR("Clickhouse client error - ~p", [Error]),
    gun:shutdown(Con),
    timer:send_after(?CONNECTION_TIMEOUT, connect),
    {noreply, State};
handle_info(bulk_send, #{queries := []} = State) ->
    {noreply, State};
handle_info(bulk_send, #{queries := Qs} = State) ->
    Result = lists:map(fun (Q) -> make_query(Q, State) end,
                       Qs),
    ?LOG_DEBUG("Clickhouse result for ~p - ~p",
               [Qs, Result]),
    {noreply, State#{queries => []}};
handle_info(Info, State) ->
    ?LOG_DEBUG("Unknown message - ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{con := Con}) when is_pid(Con) ->
    gun:shutdown(Con),
    ok;
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%
% local
%

connect(State) ->
    URI = uri_string:parse(to_list(maps:get(url,
                                            State,
                                            "http://127.0.0.1:8123"))),
    Host = to_list(maps:get(host, URI, "127.0.0.1")),
    Port = maps:get(port, URI, 8123),
    Path = to_list(maps:get(path, URI, "/")),
    QS = case maps:get(query, URI, undefined) of
             undefined ->
                 case maps:get(database, State, undefined) of
                     undefined -> "";
                     DB -> "database=" ++ to_list(DB)
                 end;
             ValidQS -> ValidQS
         end,
    Headers = [{<<"X-ClickHouse-User">>,
                to_binary(maps:get(user, State, <<"default">>))},
               {<<"X-ClickHouse-Key">>,
                to_binary(maps:get(password, State, <<"">>))}],
    case gun:open(Host, Port) of
        {ok, Con} ->
            case gun:await_up(Con) of
                {ok, _Protocol} ->
                    State#{con => Con, headers => Headers,
                           f_path => Path ++ "?output_format_json_quote_64bit_integers=0&" ++ QS};
                AwaitError ->
                    ?LOG_ERROR("Can't await connection ~p up - ~p",
                               [URI, AwaitError]),
                    timer:send_after(?CONNECTION_TIMEOUT, connect),
                    State
            end;
        OpenError ->
            ?LOG_ERROR("Can't open connection ~p up - ~p",
                       [URI, OpenError]),
            timer:send_after(?CONNECTION_TIMEOUT, connect),
            State
    end.

make_query(SQL, State) ->
    make_query(SQL, <<>>, State).

make_query(SQL0, ReturnFormat,
           #{con := Con, headers := Headers, f_path := FPath}) ->
    SQL = case ReturnFormat of
              <<>> -> SQL0;
              _ -> <<SQL0/binary, " FORMAT ", ReturnFormat/binary>>
          end,
    ?LOG_DEBUG("Execute ~p", [SQL]),
    StreamRef = gun:post(Con, FPath, Headers, SQL),
    case gun:await(Con, StreamRef, ?TIMEOUT) of
        {response, fin, Status, RespHeaders} ->
            process_response(Con,
                             StreamRef,
                             Status,
                             RespHeaders,
                             true,
                             ReturnFormat);
        {response, nofin, Status, RespHeaders} ->
            process_response(Con,
                             StreamRef,
                             Status,
                             RespHeaders,
                             false,
                             ReturnFormat);
        Other ->
            ?LOG_WARNING("Unknown clickhouse client response - ~p",
                         [Other]),
            {error, unknown_response}
    end.

process_response(Con, StreamRef, Status, RespHeaders,
                 IsFin, ReturnFormat) ->
    RespHeadersMap = maps:from_list([{string:lowercase(K),
                                      V}
                                     || {K, V} <- RespHeaders]),
    case Status =:= 200 orelse Status =:= 204 of
        true when IsFin -> ok;
        true ->
            case gun:await_body(Con, StreamRef, ?BODY_TIMEOUT) of
                {ok, Body} ->
                    {ok,
                     RespHeadersMap,
                     process_body(ReturnFormat, RespHeadersMap, Body)};
                Error ->
                    ?LOG_ERROR("Can't load body - ~p", [Error]),
                    {error, Error}
            end;
        false ->
            Body = case gun:await_body(Con,
                                       StreamRef,
                                       ?BODY_TIMEOUT)
                       of
                       {ok, B} -> B;
                       _Any -> <<>>
                   end,
            ?LOG_DEBUG("Clickhouse client return ~p (~p) ~p",
                       [Status, RespHeaders, Body]),
            {error, {Status, RespHeadersMap, Body}}
    end.

process_body(<<"JSONEachRow">>, #{<<"x-clickhouse-format">> := <<"JSONEachRow">>} = _RespHeadersMap, Body) ->
    to_json_array(Body, <<>>, []);
process_body(_RespFormat, _RespHeadersMap, Body) ->
    Body.

%
% utils
%

to_list(L) when is_binary(L) -> binary_to_list(L);
to_list(L) -> L.

to_binary(B) when is_binary(B) -> B;
to_binary(B) when is_list(B) -> list_to_binary(B).

-define(DECODE_OPTS, #{null_term => null, plugins => [datetime, inet], values => fun(<<Yr:4/binary,"-", Mon:2/binary, "-", Day:2/binary>>, _Opts) -> {binary_to_integer(Yr), binary_to_integer(Mon), binary_to_integer(Day)}; (Val, _Opts) -> Val end}).
to_json_array(<<>>, _, Terms) ->
    lists:reverse(Terms);
to_json_array(<<"\\n", Body/binary>>, Term, Acc) ->
    to_json_array(Body, <<Term/binary, "\\n">>, Acc);
to_json_array(<<"\n", Body/binary>>, Term, Acc) ->
    {ok, Json} = euneus:decode(Term, ?DECODE_OPTS),
    to_json_array(Body, <<>>, [Json | Acc]);
to_json_array(<<Char:1/binary, Body/binary>>, Term, Acc) ->
    to_json_array(Body, <<Term/binary, Char/binary>>, Acc).
