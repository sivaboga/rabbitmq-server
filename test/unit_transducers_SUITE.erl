%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_transducers_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile(export_all).

all() ->
    [
     {group, map_tests},
     {group, map_worker_pool_tests}
    ].

groups() ->
    [
     {map_tests, [parallel], [map_transducer]},
     {map_worker_pool_tests, [parallel], [map_worker_pool_transducer]}
    ].

suite() ->
    [
     {timetrap, {minutes, 1}}
    ].

%% ---------------------------------------------------------------------------

map_transducer(_Config) ->
    Xf = transducers:map(fun (I) -> I + 1 end),
    ?assertEqual([2, 3, 4], transducers:transduce(Xf,
                                                  fun into_list/1,
                                                  lists:seq(1, 3))).

map_worker_pool_transducer(_Config) ->
    Xf = transducers:map_worker_pool(2, fun (I) ->
                                                timer:sleep(200),
                                                I + I
                                        end),
    {Time, Value} = timer:tc(transducers, transduce,
                             [Xf, fun into_list/1, lists:seq(1, 3)]),
    ?assertEqual([2, 4, 6], lists:sort(Value)),
    ?assertMatch(T when T < 600000, Time,
                        "Should complete faster than serially"),
    ?assertMatch(T when T > 400000, Time,
                        "But not faster than the worker count allows").

%% ---------------------------------------------------------------------------

into_list({}) -> [];
into_list({Acc}) -> Acc;
into_list({Acc, I}) -> Acc ++ [I].
