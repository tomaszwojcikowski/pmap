-module(pmap_tests).

-include_lib("eunit/include/eunit.hrl").

%% test handler
-define(LIMIT, 1000).
-define(RAND, erlang:phash2({self(), os:timestamp()}, ?LIMIT)).
-define(ARR, lists:seq(1, ?LIMIT)).

map_test() ->
    map(?ARR).

map(Arr) ->    
    F = fun(E) -> (E rem 2) end,
    Control = lists:map(F, Arr),
    Control = pmap:map(F, Arr).

filtermap_test() ->
    filtermap(?ARR).

filtermap(Arr) ->
    F = fun(E) -> 0 =:= (E rem 2) end,
    Control = lists:filtermap(F, Arr),
    Control = pmap:filtermap(F, Arr).
    
empty_test() ->
    map([]),
    filtermap([]).
    
random_test() ->
    Arr = [?RAND || _ <- lists:seq(1, ?LIMIT)],
    map(Arr),
    filtermap(Arr).

crash_test() ->
    Arr = ?ARR,
    F = fun(_) -> 0 = 1 end,
    Res = pmap:map(F, Arr),
    true = lists:all(fun({error, _}) -> true end, Res).

timeout_test() ->
    Arr = ?ARR,
    Timeout = 10,
    F = fun(El) -> timer:sleep(Timeout+1), El end,
    Res = pmap:map(F, Arr, Timeout),
    true = lists:all(fun({timeout, _}) -> true end, Res).