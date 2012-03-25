-module(csv_reader).

-on_load(init_nif/0).
-define(D(X), io:format("~p:~p ~p~n", [?MODULE, ?LINE, X])).

-export([init/2, next/2]).
-export([date_to_ms/2]).

-export([start_loader/3]).

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
  % {ok, Reader} = csv_open(Path, Options),
  % {ok, Reader}.
  ets:new(csv_entries, [public, named_table]),
  {ok, Reader} = proc_lib:start(?MODULE, start_loader, [Path, Options, self()]),
  {ok, Reader}.

-record(loader, {
  file,
  offset,
  limit,
  header,
  pattern,
  cols,
  client
}).


parse_line(Bin, _Pattern) ->
  case binary:split(Bin, <<"\n">>) of
    [A,B] -> {A, B};
    [A] -> {undefined, A}
  end.

filter(float) -> $f;
filter(date) -> $d;
filter(time) -> $t;
filter(_) -> $u.

compile_pattern(Cols1, Options) ->
  Cols = [binary_to_list(H) || H <- Cols1],
  Record = atom_to_binary(proplists:get_value(header, Options, csv), latin1),
  RecordSize = proplists:get_value(size, Options, length(Cols) + 1),
  Index = lists:zip(Cols, lists:seq(0, length(Cols) - 1)),
  Map = lists:map(fun(H) ->
    case lists:keyfind(H, 1, Options) of
      {H, OutPos, Filter} ->
        <<OutPos, (filter(Filter)), -1>>;
      {H, OutPos, Filter, Add} ->
        <<OutPos, (filter(Filter)), (proplists:get_value(Add, Index, -1))>>;
      false ->
        <<-1, $s, -1>>  
    end    
  end, Cols),
  iolist_to_binary([<<(size(Record)), Record/binary, RecordSize, (length(Map))>>, Map]).


start_loader(Path, _Options, Parent) ->

  try start_loader0(Path, _Options, Parent) of
    Result -> Result
  catch
    Class:Error ->
      ?D({failed_loader, Class, Error, erlang:get_stacktrace()})
  end.    
      

start_loader0(Path, Options, Parent) ->
  
  {ok, F} = file:open(Path, [raw, binary, {read_ahead, 1024*1024}]),
  proc_lib:init_ack(Parent, {ok, self()}),
  {ok, Header1} = file:read_line(F),
  [Header2, <<>>] = binary:split(Header1, [<<"\n">>]),
  Header = binary:split(Header2, [<<",">>], [global]),
  Pattern = compile_pattern(Header, Options),
  file:position(F, size(Header1)),
  
  ?D({init_loader,Path,Parent, Header}),
  Loader = #loader{
    file = F,
    header = Header,
    offset = size(Header1),
    cols = length(Header),
    pattern = Pattern,
    client = Parent
  },
  loader(Loader).


loader(#loader{file = F, client = Client, offset = Offset, pattern = Pattern} = Loader) ->
  case file:pread(F, Offset, 128*1024) of
    {ok, Bin} ->
      {Lines, Rest} = split_lines(Bin, Pattern),
      ?D({loader, size(Bin), length(Lines), size(Rest)}),
      loader(Loader#loader{offset = Offset + size(Bin) - size(Rest)});
    eof ->
      ?D({loader,eof}),
      Client ! eof,
      ok
  end.


split_lines(Bin, Pattern) -> split_lines(Bin, [], Pattern).

split_lines(Bin, Acc, Pattern) ->
  case parse_line(Bin, Pattern) of
    {undefined, Rest} ->
      {lists:reverse(Acc), Rest};
    {Line, Rest} ->
      split_lines(Rest, [Line|Acc], Pattern)
  end.

      
  
next(_Reader, _Count) ->
  receive
    {csv, Lines} -> Lines;
    eof -> undefined
  end.
