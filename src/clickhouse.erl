-module(clickhouse).

-behaviour(gen_server).

-export([make_pool/4,
         query/2,
         query/3,
         json_insert/3,
         json_insert_async/3,
         execute/2,
         start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_continue/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("kernel/include/logger.hrl").

-define(CONNECTION_TIMEOUT, 3 * 1000). % 3 s

-define(TIMEOUT, 30 * 1000). % 30 s

-define(BODY_TIMEOUT, 60 * 1000). % 60 s

-define(BULK_SEND_INTERVAL, 60).

-define(EUNEUS_DATE_ENCODE_PLUGIN,
        fun ({Yr, Mon, Day}, _Opts) ->
                io_lib:format("\"~4..0B-~2..0B-~2..0B\"", [Yr, Mon, Day]);
            ({{Yr, Mon, Day}, {Hr, Min, Sec}}, _Opts) ->
                io_lib:format("\"~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B\"",
                              [Yr, Mon, Day, Hr, Min, round(Sec)]);
            (Val, _Opts) -> Val
        end).

-define(EUNEUS_ENCODE_OPTS,
        #{nulls => [null, undefined, nil],
          plugins => [drop_nulls, inet],
          unhandled_encoder => ?EUNEUS_DATE_ENCODE_PLUGIN}).

-define(EUNEUS_DATE_DECODE_PLUGIN,
        fun (<<Yr:4/binary, "-", Mon:2/binary, "-",
               Day:2/binary>>,
             _Opts) ->
                {binary_to_integer(Yr),
                 binary_to_integer(Mon),
                 binary_to_integer(Day)};
            (Val, _Opts) -> Val
        end).

-define(EUNEUS_DECODE_OPTS,
        #{null_term => null, plugins => [datetime, inet],
          unhandled_encoder => ?EUNEUS_DATE_DECODE_PLUGIN}).

-record(state,
        {url = "http://127.0.0.1:8123/",
         database = "default",
         username = <<"default">>,
         password = <<>>,
         con :: gun:connection(),
         headers = [],
         f_path :: list(),
         bulk_timer = undefined,
         queries = [],
         json_inserts = #{}}).

make_pool(PoolName, Params, Start, Max) ->
    pooler_sup:new_pool(#{name => PoolName,
                          max_count => Max, init_count => Start,
                          start_mfa => {clickhouse, start_link, [Params]}}).

query(Pool, SQL) ->
    query(Pool, iolist_to_binary(SQL), <<>>).

query(Pool, SQL, ReturnFormat)
    when not is_binary(SQL) ->
    query(Pool, iolist_to_binary(SQL), ReturnFormat);
query(Pool, SQL, ReturnFormat) when is_binary(SQL) ->
    Pid = pooler:take_member(Pool),
    Result = gen_server:call(Pid,
                             {query, SQL, ReturnFormat}),
    pooler:return_member(Pool, Pid, ok),
    Result.

json_insert(Pool, Table, Body) when is_atom(Table) ->
    json_insert(Pool, atom_to_list(Table), Body);
json_insert(Pool, Table, Body) when is_binary(Table) ->
    json_insert(Pool, binary_to_list(Table), Body);
json_insert(Pool, Table, Body) when is_list(Table) ->
    Pid = pooler:take_member(Pool),
    Result = gen_server:call(Pid,
                             {json_insert, Table, Body}),
    pooler:return_member(Pool, Pid, ok),
    Result.

json_insert_async(Pool, Table, Body)
    when is_atom(Table) ->
    json_insert_async(Pool, atom_to_list(Table), Body);
json_insert_async(Pool, Table, Body)
    when is_binary(Table) ->
    json_insert_async(Pool, binary_to_list(Table), Body);
json_insert_async(Pool, Table, Body)
    when is_list(Table) ->
    Pid = pooler:take_member(Pool),
    gen_server:cast(Pid, {json_insert, Table, Body}),
    pooler:return_member(Pool, Pid, ok),
    ok.

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
    BulkInterval = maps:get(bulk_send_period,
                            Opts,
                            ?BULK_SEND_INTERVAL),
    Url = maps:get(url, Opts, <<"http://localhost:8123/">>),
    Database = maps:get(database, Opts, <<"default">>),
    Username = maps:get(username, Opts, <<"default">>),
    Password = maps:get(password, Opts, <<>>),
    {ok, BulkTimer} = timer:send_interval(BulkInterval *
                                              1000,
                                          bulk_send),
    {ok,
     #state{url = Url, database = Database,
            username = Username, password = Password,
            bulk_timer = BulkTimer},
     {continue, connect}}.

handle_call({query, SQL, ReturnFormat}, _From, State) ->
    {reply, make_query(SQL, ReturnFormat, State), State};
handle_call({json_insert, Table, Body}, _From, State) ->
    {reply, make_json_insert(Table, Body, State), State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({query, SQL, _},
            #state{bulk_timer = undefined} = State) ->
    Result = make_query(SQL, State),
    ?LOG_DEBUG("Clickhouse result for ~p - ~p",
               [SQL, Result]),
    {noreply, State};
handle_cast({query, SQL},
            #state{queries = Qs} = State) ->
    {noreply,
     State#state{queries = lists:append(Qs, [SQL])}};
handle_cast({json_insert, Table, Body},
            #state{json_inserts = JsonInserts} = State)
    when is_map_key(Table, JsonInserts) ->
    Rows = [Body | maps:get(Table, JsonInserts)],
    case map_size(Rows) > 100 of
        true ->
            NState = State#state{json_inserts =
                                     JsonInserts#{Table => []}},
            Result = make_json_insert(Table, Rows, NState),
            ?LOG_DEBUG("Clickhouse result for ~p - ~p",
                       [Rows, Result]),
            {noreply, NState};
        false ->
            NJsonInserts = JsonInserts#{Table => Rows},
            {noreply, State#state{json_inserts = NJsonInserts}}
    end;
handle_cast({json_insert, Table, Body},
            #state{json_inserts = JsonInserts} = State) ->
    NJsonInserts = JsonInserts#{Table := [Body]},
    {noreply, State#state{json_inserts = NJsonInserts}};
handle_cast(_Msg, State) -> {noreply, State}.

handle_continue(connect, State) ->
    {noreply, connect(State)}.

handle_info(connect, State) ->
    {noreply, connect(State)};
handle_info({gun_error, Con, _StreamRef, Error},
            #state{con = Con} = State) ->
    ?LOG_ERROR("Clickhouse client error - ~p", [Error]),
    gun:shutdown(Con),
    timer:send_after(?CONNECTION_TIMEOUT, connect),
    {noreply, State#state{con = undefined}};
handle_info(bulk_send, #state{queries = []} = State) ->
    {noreply, State};
handle_info(bulk_send,
            #state{queries = Qs, json_inserts = JsonInserts} =
                State) ->
    QResult = lists:map(fun (Q) -> make_query(Q, State) end,
                        Qs),
    ?LOG_DEBUG("Clickhouse result for ~p - ~p",
               [Qs, QResult]),
    JResult = lists:map(fun ({Table, Rows}) ->
                                make_json_insert(Table, Rows, State)
                        end,
                        maps:to_list(JsonInserts)),
    ?LOG_DEBUG("Clickhouse json result for ~p - ~p",
               [JsonInserts, JResult]),
    {noreply,
     State#state{queries = [], json_inserts = #{}}};
handle_info(Msg, State)
    when is_tuple(Msg) andalso element(1, Msg) =:= gun_up ->
    {noreply, State};
handle_info(Msg, State)
    when is_tuple(Msg) andalso
             element(1, Msg) =:= gun_down ->
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_DEBUG("Unknown message - ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{con = Con})
    when is_pid(Con) ->
    gun:shutdown(Con),
    ok;
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%
% local
%

connect(#state{url = Url, database = Db,
               username = Username, password = Password} =
            State) ->
    URI = uri_string:parse(Url),
    Host = to_list(maps:get(host, URI, "127.0.0.1")),
    Port = maps:get(port, URI, 8123),
    Path = to_list(maps:get(path, URI, "/")),
    QS = maps:get(query, URI, "") ++
             "database=" ++ to_list(Db),
    Opts = case maps:get(scheme, URI, "") =:= "https" of
               true ->
                   #{http_opts => #{keepalive => 5000}, transport => tls,
                     tls_opts =>
                         [{versions, ['tlsv1.2', 'tlsv1.3']},
                          {cacerts, public_key:cacerts_get()},
                          {customize_hostname_check,
                           [{match_fun,
                             public_key:pkix_verify_hostname_match_fun(https)}]},
                          %%,
                          {log_level, info}]};
               false -> #{http_opts => #{keepalive => 5000}}
           end,
    Headers = [{<<"X-ClickHouse-User">>,
                to_binary(Username)},
               {<<"X-ClickHouse-Key">>, to_binary(Password)}],
    case gun:open(Host, Port, Opts) of
        {ok, Con} ->
            case gun:await_up(Con) of
                {ok, _Protocol} ->
                    State#state{con = Con, headers = Headers,
                                f_path =
                                    Path ++
                                        "?output_format_json_quote_64bit_integers=0&" ++ QS};
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

make_query(SQL, State) -> make_query(SQL, <<>>, State).

make_query(SQL, ReturnFormat, State)
    when not is_binary(SQL) ->
    make_query(iolist_to_binary(SQL), ReturnFormat, State);
make_query(SQL0, ReturnFormat,
           #state{con = Con, headers = Headers, f_path = FPath}) ->
    SQL = case ReturnFormat of
              <<>> -> SQL0;
              _ -> <<SQL0/binary, " FORMAT ", ReturnFormat/binary>>
          end,
    ?LOG_DEBUG("Execute ~p", [SQL]),
    StreamRef = gun:post(Con, FPath, Headers, SQL),
    case gun:await(Con, StreamRef, ?TIMEOUT) of
        {response, fin, Status, RespHeaders} ->
            case process_response(Con,
                                  StreamRef,
                                  Status,
                                  RespHeaders,
                                  true,
                                  ReturnFormat)
                of
                {error, _} = Err ->
                    ?LOG_ERROR("Clickhouse client error - ~p ~p ~p ~p",
                               [Err, FPath, Headers, SQL]),
                    Err;
                Resp -> Resp
            end;
        {response, nofin, Status, RespHeaders} ->
            case process_response(Con,
                                  StreamRef,
                                  Status,
                                  RespHeaders,
                                  false,
                                  ReturnFormat)
                of
                {error, _} = Err ->
                    ?LOG_ERROR("Clickhouse client error - ~p ~p ~p ~p",
                               [Err, FPath, Headers, SQL]),
                    Err;
                Resp -> Resp
            end;
        Other ->
            ?LOG_WARNING("Unknown clickhouse client response - ~p",
                         [Other]),
            {error, unknown_response}
    end.

make_json_insert(Table, Body0,
                 #state{con = Con, headers = Headers, f_path = FPath}) ->
    %%?LOG_DEBUG("Execute ~p", [SQL]),
    Path = FPath ++
               "&query=INSERT%20INTO%20" ++
                   Table ++ "%20FORMAT%20JSONEachRow",
    Body = case Body0 of
               _ when is_binary(Body0) -> Body0;
               _ when is_list(Body0) -> mk_body(Body0, <<>>);
               _ when is_map(Body0) -> mk_body([Body0], <<>>)
           end,
    StreamRef = gun:post(Con, Path, Headers, Body),
    case gun:await(Con, StreamRef, ?TIMEOUT) of
        {response, fin, Status, RespHeaders} ->
            case process_response(Con,
                                  StreamRef,
                                  Status,
                                  RespHeaders,
                                  true,
                                  <<"JSONEachRow">>)
                of
                {error, _} = Err ->
                    ?LOG_ERROR("Clickhouse client error - ~p ~p ~p ~p",
                               [Err, Path, Headers, Body]),
                    Err;
                Resp -> Resp
            end;
        {response, nofin, Status, RespHeaders} ->
            case process_response(Con,
                                  StreamRef,
                                  Status,
                                  RespHeaders,
                                  false,
                                  <<"JSONEachRow">>)
                of
                {error, _} = Err ->
                    ?LOG_ERROR("Clickhouse client error - ~p ~p ~p ~p",
                               [Err, Path, Headers, Body]),
                    Err;
                Resp -> Resp
            end;
        Other ->
            ?LOG_ERROR("Unknown clickhouse client response - ~p",
                       [Other]),
            {error, unknown_response}
    end.

mk_body([], Acc) -> Acc;
mk_body([H | T], <<>>) ->
    {ok, Enc} = euneus:encode_to_binary(H,
                                        ?EUNEUS_ENCODE_OPTS),
    mk_body(T, Enc);
mk_body([H | T], Acc) ->
    {ok, Enc} = euneus:encode_to_binary(H,
                                        ?EUNEUS_ENCODE_OPTS),
    mk_body(T, <<Acc/binary, "\n", Enc/binary>>).

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
                    %% ?LOG_ERROR("Can't load body - ~p", [Error]),
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
            % ?LOG_ERROR("Clickhouse client return ~p (~p) ~p",
            %            [Status, RespHeaders, Body]),
            {error, {Status, RespHeadersMap, Body}}
    end.

process_body(<<"JSONEachRow">>,
             #{<<"x-clickhouse-format">> := <<"JSONEachRow">>} =
                 _RespHeadersMap,
             Body) ->
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

to_json_array(<<>>, _, Terms) -> lists:reverse(Terms);
to_json_array(<<"\\n", Body/binary>>, Term, Acc) ->
    to_json_array(Body, <<Term/binary, "\\n">>, Acc);
to_json_array(<<"\n", Body/binary>>, Term, Acc) ->
    {ok, Json} = euneus:decode(Term, ?EUNEUS_DECODE_OPTS),
    to_json_array(Body, <<>>, [Json | Acc]);
to_json_array(<<Char:1/binary, Body/binary>>, Term,
              Acc) ->
    to_json_array(Body, <<Term/binary, Char/binary>>, Acc).
