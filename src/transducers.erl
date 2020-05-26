-module(transducers).

-export([transduce/3,
         transduce/4,
         map/1,
         map_worker_pool/2]).

-export([worker_loop/3]).

-type reducer() :: fun(({} | {any()} | {any(), any()}) -> any()).
-type transducer() :: fun((reducer()) -> reducer()).

-spec transduce(transducer(), reducer(), list()) -> any().
transduce(Xform, F, List) ->
    transduce(Xform, F, F({}), List).

-spec transduce(transducer(), reducer(), any(), list()) -> any().
transduce(Xform, F, Init, List) ->
    transduce0(List, Xform(F), Init).

transduce0([], Xf, Acc) ->
    Xf({Acc});
transduce0([H | Rest], Xf, Acc) ->
    transduce0(Rest, Xf, Xf({Acc, H})).

-spec map(fun()) -> transducer().
map(Fun) ->
    fun (Rf) ->
            fun
                ({}) ->
                     Rf({});
                ({Result}) ->
                     Rf({Result});
                ({Result, Input}) ->
                     Rf({Result, Fun(Input)})
             end
    end.

%% worker_map
%% worker_map uses a worker pool to transform the values in the sequence. It may reorder entries.
-spec map_worker_pool(integer(), fun()) -> transducer().
map_worker_pool(WorkerCount, Fun) ->
    fun (Rf) ->
            JobsOutstanding = counters:new(1, []),
            WorkersEts = ets:new(?MODULE, []),
            WorkerIds = lists:seq(1, WorkerCount),
            [erlang:monitor(process,
                            spawn(?MODULE,
                                  worker_loop,
                                  [Id, Fun, self()]))
             || Id <- WorkerIds],
            [receive
                 {ready, Id, Pid} ->
                     ets:insert(WorkersEts, {Id, free, Pid})
             end || Id <- WorkerIds],
            fun
                ({}) ->
                    Rf({});
                ({Result}) ->
                    NewResult = (fun ReduceRemaining(0, Acc) ->
                                         Acc;
                                     ReduceRemaining(Count, Acc) ->
                                         receive
                                             {result, _, WorkerResult} ->
                                                 ReduceRemaining(Count - 1,
                                                                 Rf({Acc, WorkerResult}))
                                         end
                                 end)(counters:get(JobsOutstanding, 1), Result),
                    ets:foldl(fun ({_, _, Pid}, _) -> Pid ! finish end, ok, WorkersEts),
                    ets:delete(WorkersEts),
                    Rf({NewResult});
                ({Result, Input}) ->
                    counters:add(JobsOutstanding, 1, 1),
                    case free_worker(WorkersEts) of
                        {FreeWorkerId, FreeWorkerPid} ->
                            %% io:format("Found free worker ~p with pid ~p~n", [FreeWorkerId, FreeWorkerPid]),
                            true = ets:update_element(WorkersEts, FreeWorkerId, {2, busy}),
                            FreeWorkerPid ! {work, self(), Input},
                            Result;
                        none ->
                            %% io:format("No free workers...~n", []),
                            % should we check for dead workers?
                            receive
                                {result, WorkerId, WorkerResult} ->
                                    counters:sub(JobsOutstanding, 1, 1),
                                    true = ets:update_element(WorkersEts, WorkerId, {2, free}),
                                    {FreeWorkerId, FreeWorkerPid} = free_worker(WorkersEts),
                                    %% io:format("Found free worker ~p with pid ~p~n", [FreeWorkerId, FreeWorkerPid]),
                                    true = ets:update_element(WorkersEts, FreeWorkerId, {2, busy}),
                                    FreeWorkerPid ! {work, self(), Input},
                                    Rf({Result, WorkerResult})
                            end
                    end
            end
    end.

free_worker(WorkersEts) ->
    case ets:match(WorkersEts, {'$1', free, '$2'}, 1) of
        '$end_of_table' -> none;
        {[[Id, Pid]], _} -> {Id, Pid}
    end.

worker_loop(Id, Fun, CoordinatorPid) ->
    CoordinatorPid ! {ready, Id, self()},
    (fun WorkerLoop() ->
             receive
                 {work, Dest, Input} ->
                     Dest ! {result, Id, Fun(Input)},
                     WorkerLoop();
                 finish ->
                     ok
             end
     end)().
