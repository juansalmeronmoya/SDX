-module(gms3).
-export([start/1, start/2]).
-define(arghh, 100).

start(Name) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Self) end).

init(Name, Master) ->
	{A1,A2,A3} = now(),
	random:seed(A1, A2, A3),
    leader(Name, Master, 0, []).

start(Name, Grp) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Grp, Self) end).    

init(Name, Grp, Master) ->
    Self = self(), 
	{A1,A2,A3} = now(),
	random:seed(A1, A2, A3),

    Grp ! {join, Self},
    receive
        {view, Leader, N, Last} ->
            Master ! joined,
	    	NewPeers = erlang:monitor(process,Leader),
			slave(Name, Master, Leader, N, Last, NewPeers)
            %%slave(Name, Master, Leader, Slaves, NewRef)
	after 1000 ->
		Master ! {error, "no replay from de lider"}
    end.

leader(Name, Master, N, Peers) ->
    receive
        {mcast, Msg} ->
            bcast(Name, {msg, N+1, Msg}, Peers),
			Master ! {deliver, Msg},
            leader(Name, Master, N+1, Peers);
        {join, Peer} ->
            NewPeers = lists:append(Peers, [Peer]),           
            bcast(Name, {view, self(), NewPeers}, NewPeers),
            leader(Name, Master, N, NewPeers);
        stop ->
            ok;
        Error ->
            io:format("leader ~s: strange message ~w~n", [Name, Error])
    end.

bcast(Name, Msg, Nodes) ->
	lists:foreach(fun(Node) ->
		Node ! Msg,
		crash(Name, Msg)
	end,
Nodes).

	
crash(Name, Msg) ->
	case random:uniform(?arghh) of
	?arghh ->
	io:format("leader ~s CRASHED: msg ~w~n", [Name, Msg]),
	exit(no_luck);
	_ ->
	ok
end.

%N -> n de sec. esperado para el siguiente msg
%Last -> copia del ultimo msg recibido del lider

slave(Name, Master, Leader, N, Last, Peers) ->   
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
			slave(Name, Master, Leader, N, Msg, Peers);
        {join, Peer} ->
            Leader ! {join, Peer},
			slave(Name, Master, Leader, N, Last, Peers);
        {msg, I, Msg} when I >= N ->
			Master ! {deliver, Msg},
            slave(Name, Master, Leader, I, Msg, Peers);
        {view, Leader, I, Last} when I >= N ->
	    	erlang:demonitor(Peers,[flush]),
	    	NewPeers = erlang:monitor(process,Leader),
			slave(Name, Master, Leader, I, Last, NewPeers);
        {'DOWN', _Ref,process, Leader, _Reason} ->
            election(Name, Master, N, Last, Peers);
        stop ->
            ok;
        Error ->
            io:format("slave ~s: strange message ~w~n", [Name, Error])
    end.
election(Name, Master, N, Last, Peers) ->
	Self = self(),
	case Peers of 
		[Self|Rest] ->
			bcast(Name, {view, self(), N, Rest}, Rest),
			leader(Name, Master, N, Rest);
		[NewLeader|Rest] ->
			NewPeer = erlang:monitor(process,NewLeader),
			slave(Name, Master, NewLeader, N, Last, [NewPeer|Rest])
	end.