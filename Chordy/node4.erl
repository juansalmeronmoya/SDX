-module(node4).
-export([start/1, start/2]).

-define(Stabilize, 1000).
-define(Timeout, 5000).

start(MyKey) ->
    start(MyKey, nil).

start(MyKey, PeerPid) ->
    timer:start(),
    spawn(fun() -> init(MyKey, PeerPid) end).

init(MyKey, PeerPid) ->
    Predecessor = nil,
    {ok, Successor} = connect(MyKey, PeerPid),
    schedule_stabilize(),    
	Store = storage:create(),
	Replica = storage:create(),
    node(MyKey, Predecessor, Successor,nil, Store, Replica).

connect(MyKey, nil) ->
    {ok, {MyKey, nil, self()}};
connect(_, PeerPid) ->
    Qref = make_ref(),
    PeerPid ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
			Ref = monit(PeerPid),
            {ok, {Skey, Ref, PeerPid}}  
    after ?Timeout ->
        io:format("Timeout: no response from ~w~n", [PeerPid])
    end.

node(MyKey, Predecessor, Successor, Next, Store, Replica) ->
    receive 
        {key, Qref, PeerPid} ->
		    PeerPid ! {Qref, MyKey},
		    node(MyKey, Predecessor, Successor, Next, Store, Replica);
        {notify, New} ->
		    {Pred, Keep} = notify(New, MyKey, Predecessor, Store),
			{_,_,Spid} = Successor,
		    Spid ! {pushreplica, Keep},
		    node(MyKey, Pred, Successor, Next, Keep, Replica);
        {request, Peer} ->
		    request(Peer, Predecessor, Successor),
		    node(MyKey, Predecessor, Successor, Next, Store, Replica);
        {status, Pred, Nx} ->
		    {Succ, Nxt} = stabilize(Pred,Nx, MyKey, Successor),
		    node(MyKey, Predecessor, Succ, Nxt, Store, Replica);
        stabilize ->
		    stabilize(Successor),
		    node(MyKey, Predecessor, Successor,Next, Store, Replica);
        probe ->
		    create_probe(MyKey, Successor),
		    node(MyKey, Predecessor, Successor,Next,Store, Replica);
        {probe, MyKey, Nodes, T} ->
		    remove_probe(MyKey, Nodes, T, Store, Replica),
		    node(MyKey, Predecessor, Successor,Next,Store, Replica);
        {probe, RefKey, Nodes, T} ->
		    forward_probe(RefKey, [MyKey|Nodes], T, Successor),
		    node(MyKey, Predecessor, Successor,Next,Store, Replica);
    	{handover, Elements} ->
		    Merged = storage:merge(Store, Elements),
		    {_,_,Spid} = Successor,
		    Spid ! {pushreplica, Merged},
		    node(MyKey, Predecessor, Successor,Next, Merged, Replica);
	    {add, Key, Value, Qref, Client} ->
		    Added = add(Key, Value, Qref, Client, MyKey, Predecessor, Successor, Store),
		    node(MyKey, Predecessor, Successor,Next, Added, Replica);
    	{'DOWN', Ref, process, _, _} ->
	    	{Pred, Succ, Nxt, NewStore, NewReplica} = down(Ref, Predecessor, Successor, Next, Store, Replica),
			{_,_,Spid} = Successor,
			Spid ! {pushreplica, NewStore},
	    	node(MyKey, Pred, Succ, Nxt, NewStore, NewReplica);
	    {lookup, Key, Qref, Client} ->
	    	lookup(Key, Qref, Client, MyKey, Predecessor, Successor, Store),
	    	node(MyKey, Predecessor, Successor,Next, Store, Replica);
	    {replicate, Key, Value} ->
	    	NewReplica = storage:add(Key, Value, Replica),
	    	node(MyKey, Predecessor, Successor,Next, Store, NewReplica);
	    {pushreplica, NewReplica} ->
	    	node(MyKey, Predecessor, Successor,Next, Store, NewReplica);
	    stop ->
	    	ok
    end.

stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.
   
stabilize(Pred, Next, MyKey, Successor) ->
    {Skey,Sref, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {MyKey,self()}},
            {Successor, Next};
        {MyKey, _, _} ->
            {Successor, Next};
        {Skey, _, _} ->
            Spid ! {notify, {MyKey,self()}},
            {Successor, Next};
        {Xkey, _, Xpid} ->
            case key:between(Xkey, MyKey, Skey) of
                true ->
                    self() ! stabilize,
                    demonit(Sref),
                    Xref = monit(Xpid),
                    {{Xkey, Xref, Xpid}, Successor};
                false ->
                    Spid ! {notify, {MyKey,self()}},
                    {Successor, Next}
            end
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

request(Peer, Predecessor, Successor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil, Successor};
        {Pkey,Pref, Ppid} ->
            Peer ! {status, {Pkey,Pref, Ppid}, Successor}
    end.

notify({Nkey, Npid}, MyKey, Predecessor, Store) ->
    case Predecessor of
        nil ->
			Nref = monit(Npid),
			Keep = handover(Store, MyKey, Nkey, Npid),
            {{Nkey, Nref, Npid}, Keep};
        {Pkey, Pref, Ppid} ->
            case key:between(Nkey, Pkey, MyKey) of
                true ->
					demonit(Pref),
					Nref = monit(Npid),
					Keep = handover(Store, MyKey, Nkey, Npid),
                    {{Nkey, Nref, Npid}, Keep};
                false -> 
                    {{Pkey, Pref, Ppid}, Store}
            end
    end.

create_probe(MyKey, {_, _,Spid}) ->
    Spid ! {probe, MyKey, [MyKey], erlang:now()},
    io:format("Create probe ~w!~n", [MyKey]).
	
remove_probe(MyKey, Nodes, T, Store, Replica) ->
    Time = timer:now_diff(erlang:now(), T) div 1000,
    io:format("Received probe ~w in ~w ms Ring: ~w~n", [MyKey, Time, Nodes]),
    io:format("Store ~w R ~w ~n", [Store, Replica]).
	
forward_probe(RefKey, Nodes, T, {_, _, Spid}) ->
    Spid ! {probe, RefKey, Nodes, T},
    io:format("Forward probe ~w!~n", [RefKey]).

add(Key, Value, Qref, Client, MyKey, {Pkey,_, _}, {_,_, Spid}, Store) ->
	case key:between(Key , MyKey , Pkey) of 
	true ->
		Added = storage:add(Key, Value, Store),
		Spid ! {replicate, Key, Value},
		Client ! {Qref, ok},
		Added;
	false ->
		Spid ! {add, Key, Value, Qref, Client},
		Store
end.

lookup(Key, Qref, Client, MyKey, {Pkey,_, _}, {_,_, Spid}, Store) ->
	case key:between(Key , MyKey , Pkey) of
	    true ->
		    Result = storage:lookup(Key, Store),
		    Client ! {Qref, Result};
	    false ->
	        Spid ! {lookup, Key, Qref, Client}
end.

handover(Store, MyKey, Nkey, Npid) ->
	{Keep, Leave} = storage:split(MyKey, Nkey, Store),
	Npid ! {handover,  Leave},
	Keep.

monit(Pid) ->
	erlang:monitor(process, Pid).
	
demonit(nil) ->
	ok;
demonit(MonitorRef) ->
	erlang:demonitor(MonitorRef, [flush]).

down(Ref, {_, Ref, _}, Successor, Next, Store, Replica) ->
	Merged = storage:merge(Store, Replica),
	{nil, Successor, Next,Merged,storage:create()};
down(Ref, Predecessor, {_, Ref, _}, {Nkey,_, Npid}, Store, Replica) ->
	Nref = monit(Npid),
	self() ! stabilize,
	{Predecessor, {Nkey, Nref, Npid}, nil, Store, Replica}.
	
