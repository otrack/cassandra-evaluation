#!/usr/bin/env escript

%%! -pa _build/default/lib/eredis/ebin/

-define(IGNORE_CHAINS, false).

main(_) ->
    Redis = redis_connection(),

    %% get all the redis keys
    {ok, Keys} = eredis:q(Redis, ["KEYS", "*"]),

    io:format("Keys found: ~p~n", [Keys]),

    lists:foreach(
        fun(Key) ->
            case should_fetch(Key) of
                true ->
                    io:format("Fetching key ~p...~n", [Key]),
                    %% for all the keys (files), save them in the metrics dir
                    Content = case string:str(binary_to_list(Key), "path") of
                        0 ->
                            {ok, C} = eredis:q(Redis, ["SMEMBERS", Key], infinity),
                            C;
                        _ ->
                            {ok, HKeys} = eredis:q(Redis, ["HKEYS", Key], infinity),
                            lists:foldl(
                                fun(HKey, Acc) ->
                                    {ok, HVal} = eredis:q(Redis, ["HGET", Key, HKey], infinity),
                                    Acc ++ binary_to_list(HKey) ++ "-"
                                        ++ binary_to_list(HVal) ++ ","
                                end,
                                "",
                                HKeys
                            )
                    end,
                    save(Key, Content);
                false ->
                    ok
            end
        end,
        Keys
    ),

    ok.

%% @private
should_fetch(Key) ->
    case ?IGNORE_CHAINS of
        true -> string:str(binary_to_list(Key), "Chains") == 0;
        false -> true
    end.

%% @private
redis_connection() ->
    {ok, Redis} = eredis:start_link(),
    Redis.

%% @private
metrics_dir() ->
    os:getenv("METRICS_DIR").

%% @private
save(Filename, File) ->
    Path = get_path(Filename),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, File).

%% @private
get_path(Filename) ->
    metrics_dir() ++ "/" ++ binary_to_list(Filename).
