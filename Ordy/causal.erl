-module(causal).
-export([start/3]).

start(Id, Master, Jitter) ->
    spawn(fun() -> init(Id, Master, Jitter) end).

init(Id, Master, Jitter) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    receive
        {peers, Nodes} ->
			VC = newVC(length(Nodes), []),
			%%io:format("1 VC(~w);~n", [VC]),
            server(Id, Master, lists:delete(self(), Nodes), Jitter, VC, [])
    end.

server(Id, Master, Nodes, Jitter, VC, Queue) ->
    receive
        {send, Msg} ->
		NVC = setelement(Id, VC, element(Id, VC) +1),
		multicast(Msg, Nodes, Jitter, Id, NVC),
		Master ! {deliver, Msg},
		%%io:format("2 VC(~w);~n", [NVC]),
		server(Id, Master, Nodes, Jitter, NVC, Queue);
	{multicast, Msg, FromId, MsgVC} ->
		%%io:format("RECIBO(~w);~n", [Msg]),
		case checkMsg(FromId, MsgVC, VC, size(VC)) of
			ready ->
				Master ! {deliver, Msg},
				NewVC = setelement(FromId, VC, element(FromId, VC) +1),
				%%io:format("4 VC(~w);~n", [NewVC]),
				{NewerVC, NewQueue} = deliverReadyMsgs(Master, NewVC, Queue, Queue),
				server(Id, Master, Nodes, Jitter, NewerVC, NewQueue);
			wait ->
				server(Id, Master, Nodes, Jitter, VC, [{FromId, MsgVC, Msg}|Queue])
		end;
        stop ->
            ok
    end.

multicast(Msg, Nodes, 0, Id, VC) ->
    lists:foreach(fun(Node) -> 
                      Node ! {multicast, Msg, Id, VC} 
                  end, 
                  Nodes);
multicast(Msg, Nodes, Jitter, Id, VC) ->
    lists:foreach(fun(Node) -> 
                      T = random:uniform(Jitter),
                      timer:send_after(T, Node, {multicast, Msg, Id, VC})
                  end, 
                  Nodes).

newVC(0, List) ->
	list_to_tuple(List);
newVC(N, List) ->
	newVC(N-1, [0|List]).

checkMsg(_, _, _, 0) -> ready;
checkMsg(FromId, MsgVC, VC, FromId) ->
	if (element(FromId, VC)+1 == element(FromId, MsgVC)) ->
		checkMsg(FromId, MsgVC, VC, FromId-1);
	true -> wait
	end;
checkMsg(FromId, MsgVC, VC, N) ->
	%%io:format("FromId(~w) ; MsgVC(~w) ; VC(~w); N(~w);~n", [FromId, MsgVC, VC, N]),
	if (element(N, MsgVC) =< element(N, VC)) -> 
		checkMsg(FromId, MsgVC, VC, N-1);
	true -> wait
	end.

deliverReadyMsgs(_, VC, [], Queue) ->
	{VC, Queue};
deliverReadyMsgs(Master, VC, [{FromId, MsgVC, Msg}|Rest], Queue) ->
	case checkMsg(FromId, MsgVC, VC, size(VC)) of
		ready ->
			Master ! {deliver, Msg},
			NewVC = setelement(FromId, VC, element(FromId, VC) +1),
			NewQueue = lists:delete({FromId, MsgVC, Msg}, Queue),
			deliverReadyMsgs(Master, NewVC, NewQueue, NewQueue);
		wait ->
			deliverReadyMsgs(Master, VC, Rest, Queue)
	end.