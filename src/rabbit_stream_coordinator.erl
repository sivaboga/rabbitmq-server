%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([start/0]).
-export([format_ra_event/2]).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/6]).

-export([recover/0,
         start_cluster/1,
         delete_cluster/2,
         add_replica/2,
         delete_replica/2]).

-export([phase_repair_mnesia/2,
         phase_start_cluster/1,
         phase_delete_cluster/2,
         phase_check_quorum/1,
         phase_start_new_leader/1,
         phase_stop_replicas/1,
         phase_start_replica/2,
         phase_delete_replica/2]).

-export([log_overview/1]).

-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).

-record(?MODULE, {streams, monitors}).

start() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    ServerId = {?MODULE, node()},
    case ra:restart_server(ServerId) of
        {error, Reason} when Reason == not_started orelse
                             Reason == name_not_registered -> 
            case ra:start_server(make_ra_conf(node(), Nodes)) of
                ok ->
                    global:set_lock(?STREAM_COORDINATOR_STARTUP),
                    case find_members(Nodes) of
                        [] ->
                            %% We're the first (and maybe only) one
                            ra:trigger_election(ServerId);
                        Members ->
                            %% What to do if we get a timeout?
                            {ok, _, _} = ra:add_member(Members, ServerId, 30000)
                    end,
                    global:del_lock(?STREAM_COORDINATOR_STARTUP),
                    _ = ra:members(ServerId),
                    ok;
                Error ->
                    exit(Error)
            end;
        ok ->
            ok;
        Error ->
            exit(Error)
    end.

find_members([]) ->
    [];
find_members([Node | Nodes]) ->
    case ra:members({?MODULE, Node}) of
        {_, Members, _} ->
            Members;
        {error, noproc} ->
            find_members(Nodes);
        {timeout, _} ->
            %% not sure what to do here
            find_members(Nodes)
    end.

recover() ->
    ra:restart_server({?MODULE, node()}).

start_cluster(Q) ->
    process_command({start_cluster, #{queue => Q}}).

delete_cluster(StreamId, ActingUser) ->
    process_command({delete_cluster, #{stream_id => StreamId, acting_user => ActingUser}}).

add_replica(StreamId, Node) ->
    process_command({start_replica, #{stream_id => StreamId, node => Node}}).

delete_replica(StreamId, Node) ->
    process_command({delete_replica, #{stream_id => StreamId, node => Node}}).

process_command(Cmd) ->
    Servers = ensure_coordinator_started(),
    process_command(Servers, Cmd).

process_command([], _Cmd) ->
    {error, coordinator_unavailable};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd) of
        {timeout, _} ->
            process_command(Servers, Cmd);
        {error, noproc} ->
            process_command(Servers, Cmd);
        Reply ->
            Reply
    end.

ensure_coordinator_started() ->
    Local = {?MODULE, node()},
    AllNodes = all_nodes(),
    case ra:restart_server(Local) of
        {error, Reason} when Reason == not_started orelse
                             Reason == name_not_registered -> 
            OtherNodes = all_nodes() -- [Local],
            %% We can't use find_members/0 here as a process that timeouts means the cluster is up
            case lists:filter(fun(N) -> global:whereis_name(N) =/= undefined end, OtherNodes) of
                [] ->
                    start_coordinator_cluster();
                _ ->
                    %% TODO
                    %% The coordinator process should take care of adding the new rabbit node
                    OtherNodes
            end;
        ok ->
            AllNodes;
        {error, {already_started, _}} ->
            AllNodes;
        _ ->
            AllNodes
    end.

start_coordinator_cluster() ->
    Nodes = rabbit_mnesia:cluster_nodes(running),
    case ra:start_cluster([make_ra_conf(Node, Nodes) || Node <-  Nodes]) of
        {ok, Started, _} ->
            Started;
        {error, cluster_not_formed} ->
            rabbit_log:warning("Stream coordinator cluster not formed", []),
            []
    end.

all_nodes() ->
    Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
    [{?MODULE, Node} || Node <- [node() | Nodes]].

init(_Conf) ->
    #?MODULE{streams = #{},
             monitors = #{}}.

apply(Meta, {_, #{from := From}} = Cmd, State) ->
    ?MODULE:apply(Meta#{from => From}, Cmd, State);
apply(#{from := From}, {start_cluster, #{queue := Q}}, #?MODULE{streams = Streams} = State) ->
    #{name := StreamId} = Conf = amqqueue:get_type_state(Q),
    case maps:is_key(StreamId, Streams) of
        true ->
            {State, '$ra_no_reply', [{reply, From, {error, already_started}}]};
        false ->
            SState = #{state => start_cluster,
                       phase => phase_start_cluster,
                       conf => Conf,
                       reply_to => From,
                       pending => []},
            {State#?MODULE{streams = maps:put(StreamId, SState, Streams)}, '$ra_no_reply', 
             [{aux, {phase, StreamId, phase_start_cluster, [Q]}}]}
    end;
apply(_Meta, {start_cluster_reply, Q}, #?MODULE{streams = Streams,
                                                monitors = Monitors0} = State) ->
    #{name := StreamId,
      leader_pid := LeaderPid,
      replica_pids := ReplicaPids} = Conf = amqqueue:get_type_state(Q),
    SState = maps:put(phase, phase_repair_mnesia,
                      maps:put(conf, Conf, maps:get(StreamId, Streams))),
    Monitors = lists:foldl(fun(Pid, M) ->
                                   maps:put(Pid, {StreamId, follower}, M)
                           end, maps:put(LeaderPid, {StreamId, leader}, Monitors0), ReplicaPids),
    MonitorActions = [{monitor, process, Pid} || Pid <- ReplicaPids ++ [LeaderPid]],
    {State#?MODULE{streams = maps:put(StreamId, SState, Streams),
                   monitors = Monitors}, ok,
     MonitorActions ++ [{aux, {phase, StreamId, phase_repair_mnesia, [new, Q]}}]};
apply(_Meta, {phase_finished, StreamId, Reply}, State) ->
    reply_and_run_pending(StreamId, Reply, State);
apply(#{from := From}, {start_replica, #{stream_id := StreamId, node := Node}} = Cmd,
      #?MODULE{streams = Streams0} = State) ->
    case maps:get(StreamId, Streams0, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {error, not_found}}]};
        #{conf := Conf,
          state := running} = SState0 ->
            SState = update_stream_state(From, start_replica, phase_start_replica, SState0),
            {State#?MODULE{streams = Streams0#{StreamId => SState}}, '$ra_no_reply',
             [{aux, {phase, StreamId, phase_start_replica, [Node, Conf]}}]};
        SState0 ->
            Streams = maps:put(StreamId, add_pending_cmd(From, Cmd, SState0), Streams0),
            {State#?MODULE{streams = Streams}, '$ra_no_reply', []}
    end;
apply(_Meta, {start_replica_reply, Pid, #{name := StreamId} = Conf},
      #?MODULE{streams = Streams, monitors = Monitors0} = State) ->
    SState = maps:put(phase, phase_repair_mnesia, maps:put(conf, Conf, maps:get(StreamId, Streams))),
    {State#?MODULE{streams = Streams#{StreamId => SState},
                   monitors = Monitors0#{Pid => {StreamId, follower}}}, ok,
     [{monitor, process, Pid}, {aux, {phase, StreamId, phase_repair_mnesia, [update, Conf]}}]};
apply(#{from := From}, {delete_replica, #{stream_id := StreamId, node := Node}},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {error, not_found}}]};
        #{conf := Conf0} = SState0 ->
            Replicas0 = maps:get(replica_nodes, Conf0),
            ReplicaPids0 = maps:get(replica_pids, Conf0),
            case lists:member(Node, Replicas0) of
                false ->
                    {State, '$ra_no_reply', [{reply, From, ok}]};
                true ->
                    [Pid] = lists:filter(fun(P) -> node(P) == Node end, ReplicaPids0),
                    ReplicaPids = lists:delete(Pid, ReplicaPids0),
                    Replicas = lists:delete(Node, Replicas0),
                    Conf = Conf0#{replica_pids => ReplicaPids,
                                  replica_nodes => Replicas},
                    SState = update_stream_state(From, delete_replica, phase_delete_replica,
                                                 SState0#{conf => Conf0}),
                    {State#?MODULE{monitors = maps:remove(Pid, Monitors0),
                                   streams = Streams#{StreamId => SState}},
                     '$ra_no_reply',
                     [{demonitor, process, Pid},
                      {aux, {phase, StreamId, phase_delete_replica, [Node, Conf]}}]}
            end
    end;
apply(#{from := From}, {delete_cluster, #{stream_id := StreamId,
                                          acting_user := ActingUser}} = Cmd,
      #?MODULE{streams = Streams0, monitors = Monitors0} = State) ->
    case maps:get(StreamId, Streams0, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {ok, 0}}]};
        #{conf := Conf,
          state := running} = SState0 ->
            ReplicaPids = maps:get(replica_pids, Conf),
            LeaderPid = maps:get(leader_pid, Conf),
            Monitors = lists:foldl(fun(Pid, M) ->
                                           maps:remove(Pid, M)
                                   end, Monitors0, ReplicaPids ++ [LeaderPid]),
            SState = update_stream_state(From, delete_cluster, phase_delete_cluster, SState0),
            Demonitors = [{demonitor, process, Pid} || Pid <- [LeaderPid | ReplicaPids]],
            {State#?MODULE{monitors = Monitors,
                           streams = Streams0#{StreamId => SState}}, '$ra_no_reply',
             Demonitors ++ [{aux, {phase, StreamId, phase_delete_cluster, [Conf, ActingUser]}}]};
        SState0 ->
            %% TODO remove from pending the leader election or automatic restart!
            Streams = maps:put(StreamId, add_pending_cmd(From, Cmd, SState0), Streams0),
            {State#?MODULE{streams = Streams}, '$ra_no_reply', []}
    end;
apply(_Meta, {delete_cluster_reply, StreamId},  #?MODULE{streams = Streams} = State0) ->
    State = State0#?MODULE{streams = maps:remove(StreamId, Streams)},
    case maps:get(StreamId, Streams) of
        #{reply_to := From,
          pending := []} ->
            {State, ok, [{reply, From, {ok, 0}}]};
        #{reply_to := From,
          pending := Pending} ->
            [ra:pipeline_command({?MODULE, node()}, Cmd) || Cmd <- Pending],
            {State, ok, [{reply, From, {ok, 0}}]}
    end;
apply(_Meta, {down, Pid, _Reason} = Cmd, #?MODULE{streams = Streams,
                                                  monitors = Monitors0} = State) ->
    case maps:get(Pid, Monitors0, undefined) of
        {StreamId, Role} ->
            Monitors = maps:remove(Pid, Monitors0),
            case maps:get(StreamId, Streams, undefined) of
                #{state := delete_cluster} ->
                    {State#?MODULE{monitors = Monitors}, ok, []};
                undefined ->
                    {State#?MODULE{monitors = Monitors}, ok, []};
                #{state := running,
                  conf := Conf0,
                  pending := Pending0} = SState0 ->
                    case Role of
                        leader ->
                            SState = update_stream_state(undefined, leader_election,
                                                         phase_check_quorum, SState0),
                            {State#?MODULE{monitors = Monitors,
                                           streams = Streams#{StreamId => SState}},
                             ok, [{aux, {phase, StreamId, phase_check_quorum, [Conf0]}}]};
                        follower ->
                            case rabbit_misc:is_process_alive(maps:get(leader_pid, Conf0)) of
                                true ->
                                    SState = update_stream_state(undefined, replica_restart,
                                                                 phase_start_replica,
                                                                 SState0),
                                    {State#?MODULE{monitors = Monitors,
                                                   streams = Streams#{StreamId => SState}},
                                     ok, [{aux, {phase, StreamId, phase_start_replica, [node(Pid), Conf0]}}]};
                                false ->
                                    SState = SState0#{pending => [Cmd | Pending0]},
                                    {State#?MODULE{streams = Streams#{StreamId => SState}}, ok, []}
                            end
                    end;
                #{pending := Pending0} = SState0 ->
                    SState = SState0#{pending => [Cmd | Pending0]},
                    {State#?MODULE{streams = Streams#{StreamId => SState}}, ok, []}
            end;
        undefined ->
            {State, ok, []}
    end;
apply(_Meta, {start_leader_election, StreamId, NewEpoch, Offsets},
      #?MODULE{streams = Streams} = State) ->
    #{conf := Conf0} = SState0 = maps:get(StreamId, Streams),
    #{leader_node := Leader,
      replica_nodes := Replicas} = Conf0,
    NewLeader = find_max_offset(Offsets),
    Conf = Conf0#{epoch => NewEpoch,
                  leader_node => NewLeader,
                  replica_nodes => lists:delete(NewLeader, Replicas ++ [Leader])},
    SState = SState0#{conf => Conf,
                      phase => phase_start_new_leader},
    {State#?MODULE{streams = Streams#{StreamId => SState}}, ok,
     [{aux, {phase, StreamId, phase_start_new_leader, [Conf]}}]};
apply(_Meta, {restart_replicas, #{name := StreamId,
                                 leader_pid := LeaderPid} = Conf},
      #?MODULE{streams = Streams, monitors = Monitors} = State) ->
    SState0 = maps:get(StreamId, Streams),
    SState = SState0#{conf => Conf,
                      phase => phase_stop_replicas},
    {State#?MODULE{streams = Streams#{StreamId => SState},
                   monitors = maps:put(LeaderPid, {StreamId, leader}, Monitors)}, ok,
     [{monitor, process, LeaderPid}, {aux, {phase, StreamId, phase_stop_replicas, [Conf]}}]};
apply(_Meta, {stream_updated, #{name := StreamId} = Conf}, #?MODULE{streams = Streams} = State) ->
    SState0 = maps:get(StreamId, Streams),
    SState = SState0#{conf => Conf,
                      phase => phase_repair_mnesia},
    {State#?MODULE{streams = Streams#{StreamId => SState}}, ok,
     [{aux, {phase, StreamId, phase_repair_mnesia, [update, Conf]}}]}.

state_enter(leader, #?MODULE{streams = Streams}) ->
    maps:fold(fun(_, #{conf := Conf}, Acc) ->
                      [{monitor, process, maps:get(leader_pid, Conf)}] ++
                          [{monitor, process, Pid} || Pid <- maps:get(replica_pids, Conf)] ++
                          Acc
              end, [], Streams);
state_enter(_, _) ->
    [].

init_aux(_Name) ->
    #{}.

%% TODO ensure the dead writer is restarted as a replica at some point in time, increasing timeout?
handle_aux(leader, _, {phase, _, Fun, Args} = Cmd, Monitors, LogState, _) ->
    Pid = erlang:apply(?MODULE, Fun, Args),
    Actions = [{monitor, process, aux, Pid}],
    {no_reply, maps:put(Pid, Cmd, Monitors), LogState, Actions};
handle_aux(leader, _, {down, Pid, normal}, Monitors, LogState, _) ->
    {no_reply, maps:remove(Pid, Monitors), LogState};
handle_aux(leader, _, {down, Pid, Reason}, Monitors0, LogState, _) ->
    %% The phase has failed, let's retry it
    case maps:get(Pid, Monitors0) of
        {phase, StreamId, phase_start_new_leader, Args} ->
            rabbit_log:warning("Error while starting new leader for stream queue ~p, "
                               "restarting election: ~p", [StreamId, Reason]),
            NewPid = erlang:apply(?MODULE, phase_check_quorum, Args),
            Monitors = maps:put(NewPid, StreamId, maps:remove(Pid, Monitors0)),
            {no_reply, Monitors, LogState};
        {phase, StreamId, Fun, Args} ->
            rabbit_log:warning("Error while executing coordinator phase ~p for stream queue ~p ~p",
                               [Fun, StreamId, Reason]),
            NewPid = erlang:apply(?MODULE, Fun, Args),
            Monitors = maps:put(NewPid, StreamId, maps:remove(Pid, Monitors0)),
            {no_reply, Monitors, LogState}
    end;
handle_aux(_, _, _, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState}.

reply_and_run_pending(StreamId, Reply, #?MODULE{streams = Streams} = State) ->
    #{reply_to := From,
      pending := Pending0} = SState0 = maps:get(StreamId, Streams),
    Pending = case Pending0 of
                  [] ->
                      [];
                  [Cmd | Cmds] ->
                      ra:pipeline_command({?MODULE, node()}, Cmd),
                      Cmds
              end,
    SState = maps:put(pending, Pending, clear_stream_state(SState0)),
    ReplyActions = case From of
                       undefined -> [];
                       _ -> [{reply, From, Reply}]
                   end,
    {State#?MODULE{streams = Streams#{StreamId => SState}}, ok, ReplyActions}.

add_pending_cmd(From, {CmdName, CmdMap}, #{pending := Pending} = StreamState) ->
    maps:put(pending, [{CmdName, maps:put(from, From, CmdMap)} | Pending], StreamState).

clear_stream_state(StreamState) ->
    StreamState#{reply_to => undefined,
                 state => running,
                 phase => undefined}.

update_stream_state(From, State, Phase, StreamState) ->
    StreamState#{reply_to => From,
                 state => State,
                 phase => Phase}.

phase_start_replica(Node, #{replica_nodes := Replicas0,
                            replica_pids := ReplicaPids0,
                            name := StreamId} = Conf0) ->
    spawn(
      fun() ->
              %% TODO start replica could fail and this enter an infinity loop.
              %% We should retry but not block
              %% If a new leader hasn't yet been elected, this will fail with a badmatch
              %% as get_reader_context returns a no proc. An unhandled failure will
              %% crash this monitored process and restart it later.
              %% TODO However, do we want that crash in the log? We might need to try/catch
              %% to provide a log message instead as it's 'expected'. We could try to
              %% verify first that the leader is alive, but there would still be potential
              %% for a race condition in here.
              case osiris_replica:start(Node, Conf0) of
                  {ok, Pid} ->
                      ReplicaPids = [Pid | lists:filter(fun(P) ->
                                                                node(P) =/= Node
                                                        end, ReplicaPids0)],
                      Replicas = [Node | Replicas0],
                      Conf = Conf0#{replica_pids => ReplicaPids,
                                    replica_nodes => Replicas},
                      ra:pipeline_command({?MODULE, node()},
                                          {start_replica_reply, Pid, Conf});
                  {error, already_present} ->
                      ra:pipeline_command({?MODULE, node()}, {phase_finished, StreamId, ok});
                  {error, {already_started, _}} ->
                      ra:pipeline_command({?MODULE, node()}, {phase_finished, StreamId, ok});
                  {error, Reason} ->
                      %% TODO what to do?
                      rabbit_log:warning("Error while starting replica for ~p : ~p",
                                         [maps:get(name, Conf0), Reason]),
                      ok
              end
      end).

phase_delete_replica(Node, Conf) ->
    spawn(
      fun() ->
              ok = osiris_replica:delete(Node, Conf),
              ra:pipeline_command({?MODULE, node()}, {stream_updated, Conf})
      end).

phase_stop_replicas(#{replica_nodes := Replicas} = Conf) ->
    spawn(
      fun() ->
              [try
                   osiris_replica:stop(Node, Conf)
               catch _:{{nodedown, _}, _} ->
                       %% It could be the old leader that is still down, it's normal.
                       ok
               end  || Node <- Replicas],
              ra:pipeline_command({?MODULE, node()}, {stream_updated, Conf})
      end).

phase_start_new_leader(#{leader_node := Node} = Conf) ->
    spawn(fun() ->
                  osiris_replica:stop(Node, Conf),
                  %% If the start fails, the monitor will capture the crash and restart it
                  case osiris_writer:start(Conf) of
                      {ok, Pid} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, maps:put(leader_pid, Pid, Conf)});
                      {error, already_present} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, Conf});
                      {error, {already_started, Pid}} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, maps:put(leader_pid, Pid, Conf)})
                  end
          end).

phase_check_quorum(#{name := StreamId,
                     epoch := Epoch,
                     replica_nodes := Nodes} = Conf) ->
    spawn(fun() ->
                  Offsets = find_replica_offsets(Conf),
                  case is_quorum(length(Nodes), length(Offsets)) of
                      true ->
                          ra:pipeline_command({?MODULE, node()},
                                              {start_leader_election, StreamId, Epoch + 1, Offsets});
                      false ->
                          %% Let's crash this process so the monitor will restart it
                          exit({not_enough_quorum, StreamId})
                  end
          end).

find_replica_offsets(#{replica_nodes := Nodes,
                       leader_node := Leader} = Conf) ->
    lists:foldl(
      fun(Node, Acc) ->
              try
                  %% osiris_log:overview/1 needs the directory - last item of the list
                  %% TODO highest offset and epoch - ask JV
                  %% Node availability is what we need, not the reader process!
                  %% TODO ensure we dont' match the rpc:call response with {error, sthing}
                  case rpc:call(Node, ?MODULE, log_overview, [Conf]) of
                      {badrpc, nodedown} ->
                          Acc;
                      {_Range, Offsets} ->
                          [{Node, select_highest_offset(Offsets)} | Acc]
                  end
              catch
                  _:_ ->
                      Acc
              end
      end, [], Nodes ++ [Leader]).

select_highest_offset([]) ->
    empty;
select_highest_offset(Offsets) ->
    lists:last(Offsets).

log_overview(Config) ->
    Dir = osiris_log:directory(Config),
    osiris_log:overview(Dir).

find_max_offset(Offsets) ->
    [{Node, _} | _] = lists:sort(fun({_, {Ao, E}}, {_, {Bo, E}}) ->
                                         Ao >= Bo;
                                    ({_, {_, Ae}}, {_, {_, Be}}) ->
                                         Ae >= Be;
                                    ({_, empty}, _) ->
                                         false;
                                    (_, {_, empty}) ->
                                         true
                                 end, Offsets),
    Node.

is_quorum(1, 1) ->
    true;
is_quorum(NumReplicas, NumAlive) ->
    NumAlive >= ((NumReplicas div 2) + 1).

phase_repair_mnesia(new, Q) ->
    spawn(fun() ->
                  Reply = rabbit_amqqueue:internal_declare(Q, false),
                  #{name := StreamId} = amqqueue:get_type_state(Q),
                  ra:pipeline_command({?MODULE, node()}, {phase_finished, StreamId, Reply})
          end);

phase_repair_mnesia(update, #{reference := QName,
                              leader_pid := LeaderPid,
                              name := StreamId} = Conf) ->
    Fun = fun (Q) ->
                  amqqueue:set_type_state(amqqueue:set_pid(Q, LeaderPid), Conf)
          end,
    spawn(fun() ->
                  case rabbit_misc:execute_mnesia_transaction(
                         fun() ->
                                 rabbit_amqqueue:update(QName, Fun)
                         end) of
                      not_found ->
                          %% This can happen during recovery
                          [Q] = mnesia:dirty_read(rabbit_durable_queue, QName),
                          rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q));
                      _ ->
                          ok
                  end,
                  ra:pipeline_command({?MODULE, node()}, {phase_finished, StreamId, ok})
          end).

phase_start_cluster(Q0) ->
    spawn(
      fun() ->
              case osiris:start_cluster(amqqueue:get_type_state(Q0)) of
                  {ok, #{leader_pid := Pid} = Conf} ->
                      Q = amqqueue:set_type_state(amqqueue:set_pid(Q0, Pid), Conf),
                      ra:pipeline_command({?MODULE, node()}, {start_cluster_reply, Q});
                  {error, {{already_started, _}, _}} ->
                      ra:pipeline_command({?MODULE, node()}, {start_cluster_finished, {error, already_started}})
              end
      end).

phase_delete_cluster(#{name := StreamId,
                       reference := QName} = Conf, ActingUser) ->
    spawn(
      fun() ->
              ok = osiris:delete_cluster(Conf),
              _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
              ra:pipeline_command({?MODULE, node()}, {delete_cluster_reply, StreamId})
      end).

format_ra_event(ServerId, Evt) ->
    {stream_coordinator_event, ServerId, Evt}.

make_ra_conf(Node, Nodes) ->
    UId = ra:new_uid(ra_lib:to_binary(?MODULE)),
    Formatter = {?MODULE, format_ra_event, []},
    Members = [{?MODULE, N} || N <- Nodes],
    #{cluster_name => ?MODULE,
      id => {?MODULE, Node},
      uid => UId,
      friendly_name => atom_to_list(?MODULE),
      metrics_key => ?MODULE,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => 5000,
      machine => {module, ?MODULE, #{}},
      ra_event_formatter => Formatter}.
