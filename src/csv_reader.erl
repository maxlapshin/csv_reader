-module(csv_reader).

-on_load(init_nif/0).

-export([init/2, next/1, next/2, init_nif/0]).
-export([date_to_ms/2]).

date_to_ms({YY,MM,DD},{H,M,S,MS}) ->
  date_to_ms_nif(YY, MM, DD, H, M, S, MS);

date_to_ms(_,_) ->
  undefined.


date_to_ms_nif(YY, MM, DD, H, M, S, MS) ->
  Date = {YY, MM, DD},
  Time = {H,M,S},
  Timestamp = (calendar:datetime_to_gregorian_seconds({Date,Time}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
  Timestamp*1000 + MS.
  
init_nif() ->
  Path = filename:dirname(code:which(?MODULE)) ++ "/../priv",
  Load = erlang:load_nif(Path ++ "/csv_reader", 0),
  io:format("Load csv_reader: ~p~n", [Load]),
  ok.


init(Path, Options) ->
  {ok, Reader} = csv_open(Path, Options),
  {ok, Reader}.

csv_open(_Path, _Options) ->
  erlang:error(unimplemented).


next(Reader) ->
  csv_next(Reader).

next(Reader, Count) ->
  csv_next_batch(Reader, Count).

csv_next(_Reader) ->
  erlang:error(unimplemented).

csv_next_batch(_Reader, _Count) ->
  erlang:error(unimplemented).
