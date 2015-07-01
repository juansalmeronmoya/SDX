-module(gms2).
-export([start/1, start/2]).
-define(arghh, 100).

start(Name) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Self) end).

init(Name, Master) ->
	{A1,A2,A3} = now(),
	random:seed(A1, A2, A3),

    leader(Name, Master, []).

start(Name, Grp) ->
    Self = self(),
    spawn_link(fun()-> init(Name, Grp, Self) end).    

init(Name, Grp, Master) ->
    Self = self(), 
	{A1,A2,A3} = now(),
	random:seed(A1, A2, A3),

    Grp ! {join, Self},
    receive
        {view, Leader, Slaves} ->
            Master ! joined,
	    NewRef = erlang:monitor(process,Leader),
            slave(Name, Master, Leader, Slaves, NewRef)
	after 1000 ->
		Master ! {error, "no replay from de lider"}
    end.

leader(Name, Master, Slaves) ->    
	
    receive
        {mcast, Msg} ->
            bcast(Name, {msg, Msg}, Slaves),
			Master ! {deliver, Msg},
            leader(Name, Master, Slaves);
        {join, Peer} ->
            NewSlaves = lists:append(Slaves, [Peer]),           
            bcast(Name, {view, self(), NewSlaves}, NewSlaves),
            leader(Name, Master, NewSlaves);
        stop ->
            ok;
        Error ->
            io:format("leader ~s: strange message ~w~n", [Name, Error])
    end.
    
%bcast(_, Msg, Nodes) ->
%    lists:foreach(fun(Node) -> Node ! Msg end, Nodes).

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

	
slave(Name, Master, Leader, Slaves,Ref) ->   
io:format("Eslavo"),
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Name, Master, Leader, Slaves,Ref);
        {join, Peer} ->
            Leader ! {join, Peer},
            slave(Name, Master, Leader, Slaves,Ref);
        {msg, Msg} ->
            Master ! {deliver, Msg},
            slave(Name, Master, Leader, Slaves,Ref);
        {view, Leader, NewSlaves} ->
	    erlang:demonitor(Ref,[flush]),
	    NewRef = erlang:monitor(process,Leader),
            slave(Name, Master, Leader, NewSlaves,NewRef);
        {'DOWN', _Ref,process, Leader, _Reason} ->
            election(Name,Master,Slaves);
        stop ->
            ok;
        Error ->
            io:format("slave ~s: strange message ~w~n", [Name, Error])
    end.
election(Name,Master,Slaves) ->
	Self = self(),
	case Slaves of 
		[Self|Rest] ->
			
			bcast(Name, {view, self(), Rest}, Rest),
			leader(Name, Master, Rest);
		[NewLeader|Rest] ->
			 NewRef = erlang:monitor(process,NewLeader),
			slave(Name, Master, NewLeader, Rest, NewRef)
	end.