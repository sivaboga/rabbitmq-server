-module(transducers).

-export([transduce/3,
         transduce/4,
         map/1,
         map_worker_pool/2]).

-export([worker_loop/4]).

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
            WorkersEts = ets:new(?MODULE, [public]),
            WorkerIds = lists:seq(1, WorkerCount),
            [erlang:monitor(process,
                            spawn(?MODULE,
                                  worker_loop,
                                  [WorkersEts, Id, Fun, self()]))
             || Id <- WorkerIds],
            [receive {ready, Id} -> ok end || Id <- WorkerIds],
            fun
                ({}) ->
                    Rf({});
                ({Result}) ->
                    NewResult = (fun ReduceRemaining(0, Acc) ->
                                         Acc;
                                     ReduceRemaining(Count, Acc) ->
                                         receive
                                             {result, WorkerResult} ->
                                                 ReduceRemaining(Count - 1,
                                                                 Rf({Acc, WorkerResult}))
                                         end
                                 end)(counters:get(JobsOutstanding, 1), Result),
                    ets:foldl(fun ({_, _, Pid}, _) -> Pid ! finish end, ok, WorkersEts),
                    Rf({NewResult});
                ({Result, Input}) ->
                    counters:add(JobsOutstanding, 1, 1),
                    case ets:match(WorkersEts, {'_', free, '$1'}, 1) of
                        '$end_of_table' ->
                            io:format("No free workers...~n", []),
                            % should we check for dead workers?
                            receive
                                {result, WorkerResult} ->
                                    counters:sub(JobsOutstanding, 1, 1),
                                    {[[FreeWorkerPid]], _} = ets:match(WorkersEts, {'_', free, '$1'}, 1),
                                    FreeWorkerPid ! {work, self(), Input},
                                    Rf({Result, WorkerResult})
                            end;
                        {[[FreeWorkerPid]], _} ->
                            io:format("Found free worker ~p~n", [FreeWorkerPid]),
                            FreeWorkerPid ! {work, self(), Input},
                            Result
                    end
            end
    end.

worker_loop(WorkersEts, Id, Fun, CoordinatorPid) ->
    ets:insert(WorkersEts, {Id, free, self()}),
    CoordinatorPid ! {ready, Id},
    (fun WorkerLoop() ->
             true = ets:update_element(WorkersEts, Id, {2, free}),
             receive
                 {work, Dest, Input} ->
                     true = ets:update_element(WorkersEts, Id, {2, busy}),
                     Dest ! {result, Fun(Input)},
                     WorkerLoop();
                 finish ->
                     ets:delete(WorkersEts, Id)
             end
     end)().
