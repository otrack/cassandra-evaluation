#!/usr/bin/env escript

main(_) ->
    Iterations = 100,
    Keys = [8, 64, 512],
    Types = [orddict, dict, maps],

    Metrics = lists:foldl(
        fun(KeysNumber, In0) ->
            lists:foldl(
                fun(Type, In1) ->
                    %% get, set, update, delete, merge and keys functions
                    GetFun = get_key(Type),
                    SetFun = set_key(Type),
                    UpdateFun = update_key(Type),
                    DeleteFun = delete_key(Type),
                    MergeFun = merge(Type),
                    KeysFun = keys(Type),

                    %% run the benchmark
                    Result = benchmark(KeysNumber,
                                       Type,
                                       GetFun,
                                       SetFun,
                                       UpdateFun,
                                       DeleteFun,
                                       MergeFun,
                                       KeysFun,
                                       Iterations),

                    MetricsKey = {KeysNumber, Type},
                    orddict:store(MetricsKey, Result, In1)
                end,
                In0,
                Types
            )
        end,
        orddict:new(),
        Keys 
    ),
    analyze(Metrics).

%% @doc Benchmark `get', `set', `update', `delete' and `merge'
%%      operations on different map
%%      implementations.
benchmark(KeysNumber, Type, GetFun, SetFun, UpdateFun, DeleteFun, MergeFun, KeysFun, Iterations) ->
    Map = new(KeysNumber, Type),
    
    {_, Metrics} = lists:foldl(
        fun(It, {MapIn, MetricsIn}) ->

            %% log every 20 iterations
            case It rem 20 of
                0 -> io:format("[~p|~p] ~p of ~p ~n",
                               [Type, KeysNumber, It, Iterations]);
                _ -> ok
            end,

            Key1 = random_key(KeysFun(MapIn)),
            Key2 = random_key(KeysFun(MapIn)),

            {TimeGet, _} = timer:tc(
                fun() ->
                    GetFun(Key1,
                           MapIn)
                end
            ),

            {TimeSet, MapOut0} = timer:tc(
                fun() ->
                    SetFun(Key1,
                           random_value(),
                           MapIn)
                end
            ),

            {TimeUpdate, MapOut1} = timer:tc(
                fun() ->
                    UpdateFun(Key2,
                              fun(_) -> random_value() end,
                              MapOut0)
                end
            ),

            {TimeMerge, MapOut2} = timer:tc(
                fun() ->
                    MergeFun(fun(_, V1, V2) ->
                                lists:nth(rand:uniform(2), [V1, V2])
                             end,
                             MapIn,
                             MapOut1)
                end
            ),

            {TimeDelete, MapOut3} = timer:tc(
                fun() ->
                    DeleteFun(Key1,
                              MapOut2)
                end
            ),

            MapOut4 = SetFun(double_key(Key1),
                             random_value(),
                             MapOut3),

            MetricsOut0 = orddict:append(get, TimeGet, MetricsIn),
            MetricsOut1 = orddict:append(set, TimeSet, MetricsOut0),
            MetricsOut2 = orddict:append(update, TimeUpdate, MetricsOut1),
            MetricsOut3 = orddict:append(merge, TimeMerge, MetricsOut2),
            MetricsOut4 = orddict:append(delete, TimeDelete, MetricsOut3),

            {MapOut4, MetricsOut4}
        end,
        {Map, orddict:new()},
        lists:seq(1, Iterations)
    ),

    Metrics.

%% @doc Show metrics.
analyze(Metrics) ->
    lists:foreach(
        fun({{Size, Type}, Map}) ->
            io:format("- ~p ~p:~n", [Size, Type]),
            lists:foreach(
                fun({Operation, Values}) ->
                    Average = lists:sum(Values) / length(Values),
                    io:format("  > avg of ~p: ~p~n", [Operation, Average])
                end,
                Map
            )
        end,
        Metrics
    ).

%% @doc Create a new map
%%      given its `Size' and type.
%%      Type can be:
%%        - `orddict'
%%        - `dict'
%%        - `maps'
new(KeysNumber, Type) ->
    Type:from_list(generate_keys(KeysNumber)).

%% @doc Returns function that
%%      gets the value in the `Map'
%%      on key `Key'
get_key(orddict) ->
    fun(Key, Map) ->
        orddict:fetch(Key, Map)
    end;
get_key(dict) ->
    fun(Key, Map) ->
        dict:fetch(Key, Map)
    end;
get_key(maps) ->
    fun(Key, Map) ->
        maps:get(Key, Map)
    end.

%% @doc Returns function that
%%      sets the `Key' on `Map'
%%      to `Value'.
set_key(orddict) ->
    fun(Key, Value, Map) ->
        orddict:store(Key, Value, Map)
    end;
set_key(dict) ->
    fun(Key, Value, Map) ->
        dict:store(Key, Value, Map)
    end;
set_key(maps) ->
    fun(Key, Value, Map) ->
        maps:put(Key, Value, Map)
    end.

%% @doc Update a key, given a function.
update_key(orddict) ->
    fun(Key, Fun, Map) ->
        orddict:update(Key, Fun, Map)
    end;
update_key(dict) ->
    fun(Key, Fun, Map) ->
        dict:update(Key, Fun, Map)
    end;
update_key(maps) ->
    fun(Key, Fun, Map) ->
        maps:update_with(Key, Fun, Map)
    end.

%% @doc Returns a function
%%      that deletes the `Key'
%%      from the `Map'.
delete_key(orddict) ->
    fun(Key, Map) ->
        orddict:erase(Key, Map)
    end;
delete_key(dict) ->
    fun(Key, Map) ->
        dict:erase(Key, Map)
    end;
delete_key(maps) ->
    fun(Key, Map) ->
        maps:remove(Key, Map)
    end.

%% @doc Returns a list of keys
%%      given a `Map'.
keys(orddict) ->
    fun(Map) ->
        orddict:fetch_keys(Map)
    end;
keys(dict) ->
    fun(Map) ->
        dict:fetch_keys(Map)
    end;
keys(maps) ->
    fun(Map) ->
        maps:keys(Map)
    end.

%% @doc Merge two `Map'.
merge(orddict) ->
    fun(Fun, A, B) ->
        orddict:merge(Fun, A, B)
    end;
merge(dict) ->
    fun(Fun, A, B) ->
        dict:merge(Fun, A, B)
    end;
merge(maps) ->
    fun(Fun, A, B) ->
        maps:from_list(
            orddict:merge(Fun,
                          orddict:from_list(maps:to_list(A)),
                          orddict:from_list(maps:to_list(B))
            )
        )
        %maps:map(
        %    fun(Key, VA) ->
        %        Fun(Key, VA, maps:get(Key, B))
        %    end,
        %    A
        %)
    end.

%% @doc Generate a list of pairs:
%%        - first component is a key
%%        - second component is a random value
generate_keys(KeysNumber) ->
    lists:foldl(
        fun(Seq, Acc) -> [generate_pair(Seq) | Acc] end,
        [],
        lists:seq(1, KeysNumber)
    ).

%% @doc Generate a pair.
generate_pair(Key) ->
    {Key, random_value()}.

%% @doc Generate random value.
random_value() ->
    BytesNumber = 10000,
    base64:encode(crypto:strong_rand_bytes(BytesNumber)).

%% @doc Pick a random key from the list.
random_key(Keys) ->
    Size = length(Keys),
    lists:nth(rand:uniform(Size), Keys).

%% @doc Double the key.
double_key(Key) ->
    Key * 2.
