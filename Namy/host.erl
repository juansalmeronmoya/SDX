-module(host).
-export([start/3, stop/1]).

start(Name, Domain, Parent) ->
    register(Name, spawn(fun()-> init(Domain, Parent) end)).

stop(Name) ->
    Name ! stop,
    unregister(Name).

init(Domain, Parent) ->
    io:format("Host: create domain ~w at ~w~n", [Domain, Parent]),
    Parent ! {register, Domain, {host, self()}},
    host(Parent, Domain).

host(Parent,Domain) ->
    receive
        {ping, From} ->
            io:format("Host: Ping from ~w~n", [From]),
            From ! pong,
            host(Parent,Domain);
        stop ->
			Parent ! {deregister, Domain},
            io:format("Host: Closing down~n", []),
            ok;
        Error ->
            io:format("Host: reception of strange message ~w~n", [Error]),
            host(Parent,Domain)
    end.
