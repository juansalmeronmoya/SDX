-module(cache).
-export([lookup/2, add/4, remove/2, new/0]).

lookup(Req, Cache) ->
	case lists:keyfind(Req, 1, Cache) of
	    false -> 
			unknown;
		
		{Req,Entry,Expire} ->
			
			Gambas = time:valid(Expire,time:now()),
			if
				Gambas ->
					Entry;
				true ->
					invalid
			end
	end.

new () ->
[].

add(Name, Expire, Entry, Cache) ->
	lists:keystore(Name, 1, Cache, {Name, Entry, Expire}).
remove(Name, Cache) ->
	lists:keydelete(Name,1,Cache).