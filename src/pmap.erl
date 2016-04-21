% Parallel map functions
-module(pmap).

-export([map/2]).
-export([map/3]).
-export([map/4]).
-export([filtermap/2]).
-export([filtermap/3]).
-export([filtermap/4]).

-define(DEF_TIMEOUT, 5000).
-define(CHUNK_SIZE, 4).
-define(TIMEOUT_RATIO, 0.681).

-type mapfun() :: fun((term()) -> term()).
-type filtermapfun() :: fun((term()) -> boolean() | {true, term()}).

-type gatherfun() :: fun(([{pid(), term()}], list(), non_neg_integer()) -> list()).

% API
-spec map(mapfun(), list()) -> list().
map(Function, List) ->
    map(Function, List, ?DEF_TIMEOUT).
-spec map(mapfun(), list(), integer()) -> list().
map(Function, List, Timeout) ->
   map(Function, List, Timeout, ?CHUNK_SIZE).
-spec map(mapfun(), list(), integer(), integer()) -> list().
map(Function, List, Timeout, ChunkSize) ->
    do_map(Function, List, Timeout, fun gather/3, ChunkSize). 

-spec filtermap(filtermapfun(), list()) -> list().
filtermap(Function, List) ->
    filtermap(Function, List, ?DEF_TIMEOUT).
-spec filtermap(filtermapfun(), list(), non_neg_integer()) -> list().    
filtermap(Function, List, Timeout) ->
    filtermap(Function, List, Timeout, ?CHUNK_SIZE).
-spec filtermap(filtermapfun(), list(), integer(), integer()) -> list().    
filtermap(Function, List, Timeout, ChunkSize) ->
        do_map(Function, List, Timeout, fun filtergather/3, ChunkSize).

% INTERNALS    

% Execute the function and send the result back to the receiver
-spec execute(pid(), mapfun(), term(), non_neg_integer()) -> ok.
execute(Recv, Function, Element, Timeout) ->
    Self = self(),
    spawn(fun() -> do_execute(Self, Function, Element) end),
    receive
        Res -> Recv ! {Self, Res}
    after trunc(Timeout * ?TIMEOUT_RATIO)->
        Recv ! {Self, {timeout, Element}}
    end.

do_execute(Recv, Function, Element) ->
    Res = try Function(Element) catch _:E -> {error, E} end,
    Recv ! Res.

gather([], Acc, _Timeout) ->
    Acc;
gather([{H, El} | T], Acc, Timeout) ->
    receive
        {H, Ret} ->
            gather(T, [Ret | Acc], Timeout)
    after Timeout ->
        gather(T, [{timeout, El} | Acc], Timeout)
    end.  

filtergather([], Acc, _Timeout) ->
    Acc;
filtergather([{H, El} | T], Acc, Timeout) ->
    receive
        {H, false} ->
            filtergather(T, Acc, Timeout);
        {H, true} ->
            filtergather(T, [El | Acc], Timeout);
        {H, {true, Ret}} ->
            filtergather(T, [Ret | Acc], Timeout)
    after Timeout ->
        filtergather(T, [{timeout, El} | Acc], Timeout)
    end. 

-spec do_map(mapfun(), list(), non_neg_integer(), gatherfun(), pos_integer()) -> list().
do_map(_, [], _, _, _) ->
    [];
do_map(Function, List, Timeout, GatherFun, ChunkSize) ->
    Chunks = chunkify(List, ChunkSize),
    lists:foldl(fun(Chunk, Acc) ->
        S = self(),
        PidsEls = lists:map(fun(El) ->
            {spawn(fun() -> execute(S, Function, El, Timeout) end), El} % spawn a process for each element
        end, Chunk),
        % gather and filter the results of the processes (in order) into a list
        Res = GatherFun(PidsEls, [], Timeout),
        Acc ++ lists:reverse(Res)
    end, [], Chunks).

chunkify(List,Len) ->
  chunkify(lists:reverse(List),[],0,Len).

chunkify([],Acc,_,_) -> Acc;
chunkify([H|T],Acc,Pos,Max) when Pos==Max ->
    chunkify(T,[[H] | Acc],1,Max);
chunkify([H|T],[HAcc | TAcc],Pos,Max) ->
    chunkify(T,[[H | HAcc] | TAcc],Pos+1,Max);
chunkify([H|T],[],Pos,Max) ->
    chunkify(T,[[H]],Pos+1,Max).


