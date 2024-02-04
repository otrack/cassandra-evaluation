#!/usr/bin/env escript

main(_) ->
    Iterations = 5000,
    Sizes = [8, 16, 1024, 4096],
    Types = [array, orddict, dict],

    Metrics = lists:foldl(
        fun(Size, In0) ->
            lists:foldl(
                fun(Type, In1) ->
                    %% get, set and size functions
                    GetFun = get_index(Type),
                    SetFun = set_index(Type),
                    SizeFun = vector_size(Type),

                    %% run the benchmark
                    Result = benchmark(Size,
                                       Type,
                                       GetFun,
                                       SetFun,
                                       SizeFun,
                                       Iterations),

                    MetricsKey = {Size, Type},
                    orddict:store(MetricsKey, Result, In1)
                end,
                In0,
                Types
            )
        end,
        orddict:new(),
        Sizes
    ),
    analyze(Metrics).

%% @doc Benchmark `get', `set', `update', `append' and `size'
%%      operations on different vector
%%      implementations.
benchmark(Size, Type, GetFun, SetFun, SizeFun, Iterations) ->
    Vector = new(Size, Type),
    
    {_, _, Metrics} = lists:foldl(
        fun(_, {VectorIn, SizeIn, MetricsIn}) ->
            Index = random_index(SizeIn),

            {TimeGet, Value} = timer:tc(
                fun() ->
                    GetFun(Index,
                           VectorIn)
                end
            ),

            {TimeSet, VectorOut0} = timer:tc(
                fun() ->
                    SetFun(Index,
                           Value + 1,
                           VectorIn)
                end
            ),

            {TimeUpdate, VectorOut1} = timer:tc(
                fun() ->
                    update_index(Index,
                                 fun(V) -> V + 1 end,
                                 VectorOut0,
                                 GetFun,
                                 SetFun)
                end
            ),

            {TimeAppend, VectorOut2} = timer:tc(
                fun() ->
                    SetFun(SizeIn,
                           0,
                           VectorOut1)
                end
            ),

            SizeOut = SizeIn + 1,

            {TimeSize, SizeOut} = timer:tc(
                fun() ->
                    SizeFun(VectorOut2)
                end
            ),

            MetricsOut0 = orddict:append(get, TimeGet, MetricsIn),
            MetricsOut1 = orddict:append(set, TimeSet, MetricsOut0),
            MetricsOut2 = orddict:append(update, TimeUpdate, MetricsOut1),
            MetricsOut3 = orddict:append(append, TimeAppend, MetricsOut2),
            MetricsOut4 = orddict:append(size, TimeSize, MetricsOut3),

            {VectorOut2, SizeOut, MetricsOut4}
        end,
        {Vector, Size, orddict:new()},
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

%% @doc Create a new vector
%%      given its `Size' and type.
%%      Type can be:
%%        - `orddict'
%%        - `dict'
%%        - `array'
new(Size, array) ->
    array:new([{size, Size},
               {default, 0},
               {fixed, false}]);
new(Size, Type) ->
    Type:from_list([{Index, 0} || Index <- lists:seq(0, Size - 1)]).

%% @doc Returns function that
%%      gets the value of the `Vector'
%%      on index `Index'
get_index(array) ->
    fun(Index, Vector) ->
        array:get(Index, Vector)
    end;
get_index(Type) ->
    fun(Index, Vector) ->
        Type:fetch(Index, Vector)
    end.

%% @doc Returns function that
%%      sets the `Index' on `Vector'
%%      to `Value'.
set_index(array) ->
    fun(Index, Value, Vector) ->
        array:set(Index, Value, Vector)
    end;
set_index(Type) ->
    fun(Index, Value, Vector) ->
        Type:store(Index, Value, Vector)
    end.

%% @doc Returns size of `Vector'.
vector_size(array) ->
    fun(Vector) ->
        array:size(Vector)
    end;
vector_size(Type) ->
    fun(Vector) ->
        Type:size(Vector)
    end.

%% @doc Updates `Index' on `Vector'
%%      with the result of `UpdateFunction' on the current value.
update_index(Index, UpdateFunction, Vector, GetFunction, SetFunction) ->
    CurrentValue = GetFunction(Index, Vector),
    NewValue = UpdateFunction(CurrentValue),
    SetFunction(Index, NewValue, Vector).

%% @doc Get a random index given the
%%      `Size' of the vector.
random_index(Size) ->
    rand:uniform(Size) - 1.
