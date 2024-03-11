# Erlang client library for ClickHouse

## Configuration

```erlang

-type clickhouse_config() :: #{
    url := list(), %% "http://{ip}:{port}/"
    database := list() | binary(),
    username := list() | binary(),
    password := list() | binary()
  }.

-type pool() :: #{
    name := atom(),
    init_count := integer(),
    max_count := integer(),
    start_mfa := {clickhouse, start_link, clickhouse_config()}
}
```

In your sys.config add your pools

```
{clickhouse, {pools, [pool()|...]}}

eg:

{clickhouse, {pools, [
    #{name => default,
      init_count => 2,
      max_count => 8,
      start_mfa => {clickhouse, start_link, #{url => "http://127.0.0.1:8123/",
                                              database => "default"
                                              user => "default",
                                              password => ""}}
     }
]}}

```

Alternatively manually start a pool

Start with
```
1> clickhouse:make_pool(default, #{ url => "http://127.0.0.1:8123/", database => "default", username => "default", password => "" }, 2, 8).
{ok,<0.273.0>}
2> clickhouse:query(default, <<"SELECT 1;">>).
{ok,#{<<"connection">> => <<"Keep-Alive">>,
      <<"content-type">> =>
          <<"text/tab-separated-values; charset=UTF-8">>,
      <<"date">> => <<"Tue, 23 Nov 2021 15:29:00 GMT">>,
      <<"keep-alive">> => <<"timeout=3">>,
      <<"transfer-encoding">> => <<"chunked">>,
      <<"x-clickhouse-format">> => <<"TabSeparated">>,
      <<"x-clickhouse-query-id">> =>
          <<"5c5eb581-cf6d-4858-b287-c1596616a31c">>,
      <<"x-clickhouse-server-display-name">> =>
          <<"localhost">>,
      <<"x-clickhouse-summary">> =>
          <<"{\"read_rows\":\"0\",\"read_bytes\":\"0\",\"written_rows\":\"0\",\"written_bytes\":\"0\",\"total_rows_to_read\":\"0\"}">>,
      <<"x-clickhouse-timezone">> => <<"Etc/UTC">>},
    <<"1\n">>}

```

You can get an array of Maps as the response value if using
```
clickhouse:query(default, <<"SELECT * from table....">>, <<"JSONEachRow">>).
```
Only <<"JSONEachRow">> is currently supported and relies on the `euneus` JSON decoder
