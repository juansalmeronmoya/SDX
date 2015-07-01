-module(resolver).
-export([start/1, stop/0]).

start(Root) ->
    register(resolver, spawn(fun()-> init(Root) end)).

stop() ->
    resolver ! stop,
    unregister(resolver).

init(Root) ->
    Empty = cache:new(),
    Inf = time:inf(),
    Cache = cache:add([], Inf, {domain, Root}, Empty),
    resolver(Cache).

resolver(Cache) ->
    receive
        {request, From, Req}->
            io:format("Resolver: request from ~w to solve ~w~n", [From, Req]),
            {Reply, Updated} = resolve(Req, Cache),
            From ! {reply, Reply},
            resolver(Updated);
        status ->
            io:format("Resolver: cache content: ~w~n", [Cache]),
            resolver(Cache);
        stop ->
            io:format("Resolver: closing down~n", []),
            ok;
        Error ->
            io:format("Resolver: reception of strange message ~w~n", [Error]),
            resolver(Cache)
    end.

resolve(Name, Cache)->
    io:format("Resolve ~w: ", [Name]),
    case cache:lookup(Name, Cache) of
        unknown ->
            io:format("unknown ~n", []),
            recursive(Name, Cache);
        invalid ->
            io:format("invalid ~n", []),
            NewCache = cache:remove(Name, Cache),
            recursive(Name, NewCache);
        Reply ->
            io:format("found ~w~n", [Reply]),
            {Reply, Cache}
    end.

recursive([Name|Domain], Cache) ->
    io:format("Recursive ~w: ", [Domain]),
    case resolve(Domain, Cache) of
        {unknown, Updated} ->
            io:format("unknown ~n", []),
            {unknown, Updated};
        {{domain, Srv}, Updated} ->
            Srv ! {request, self(), Name},
            io:format("Resolver: sent request to solve [~w] to ~w~n", [Name, Srv]),
            receive
                {reply, unknown, _} ->
                    {unknown, Updated};
                {reply, Reply, TTL} ->
                    Expire = time:add(time:now(), TTL),
                    NewCache = cache:add([Name|Domain], Expire, Reply, Updated),
                    {Reply, NewCache}
            end
    end.