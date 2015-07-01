-module(worker).
-export([start/4]).
-define(withdrawal, 8000).

start(Name, Lock, Sleep, Work) ->
    spawn(fun() -> init(Name, Lock, Sleep, Work) end).

init(Name, Lock, Sleep, Work) ->
    Gui = gui:start(Name),
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Taken = worker(Name, Lock, [], Sleep, Work, Gui),
    Gui ! stop,
    Lock ! stop,
    terminate(Name, Taken).

worker(Name, Lock, Taken, Sleep, Work, Gui) ->
    Wait = random:uniform(Sleep),
    receive 
       stop ->
            Taken
    after Wait ->
            T = critical(Name, Lock, Work, Gui),
            worker(Name, Lock, [T|Taken], Sleep, Work, Gui)
    end.

critical(Name, Lock, Work, Gui) ->
  T1 = now(),
  Gui ! waiting,
  Lock ! {take, self()},
  receive
      taken ->
          T2 = now(),
          T = timer:now_diff(T2, T1) div 1000,
          io:format("~s: lock taken in ~w ms~n", [Name, T]),
          Gui ! taken,
          timer:sleep(random:uniform(Work)),
          io:format("~s: lock released~n", [Name]),
          Gui ! leave,
          Lock ! release,
          {taken, T}
  after ?withdrawal ->
          io:format("~s: giving up~n", [Name]),
          Lock ! release,
          Gui ! leave,
          no
  end.

terminate(Name, Taken) ->
    {Locks, Time, Dead} = 
       lists:foldl(
          fun(Entry,{L,T,D}) -> 
            case Entry of 
               {taken,I} -> 
                   {L+1,T+I,D}; 
                _ -> 
                   {L,T,D+1} 
            end
          end, 
          {0,0,0}, Taken),
    if 
       Locks > 0 ->
           Average = Time / Locks;
       true ->
           Average = 0
    end,
    io:format("~s: ~w locks taken, ~w ms (avg) for taking, ~w withdrawals~n", 
              [Name, Locks, Average, Dead]).
