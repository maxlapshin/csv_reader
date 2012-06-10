-module(csv_reader).

-on_load(init_nif/0).
-define(D(X), io:format("~p:~p ~p~n", [?MODULE, ?LINE, X])).
% -define(D(X), ok).

-export([init/2, next/1, total_lines/1]).
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
  case Load of
    ok -> ok;
    {error, {Reason,Text}} -> io:format("Load csv_reader failed. ~p:~p~n", [Reason, Text])
  end,
  ok.

-record(loader, {
  file,
  fd,
  options = [],
  offset,
  limit,
  header,
  pattern,
  cols,
  count = 0,
  total,
  buffer = <<>>
}).


parse_line(Bin, _Pattern) ->
  case binary:split(Bin, <<"\n">>) of
    [A,B] -> {A, B};
    [A] -> {undefined, A}
  end.

filter(float) -> $f;
filter(int) -> $i;
filter(date) -> $d;
filter(time) -> $t;
filter(utc) -> $g;
filter(_) -> $u.


total_lines(#loader{total = Total}) -> Total.

compile_pattern(Cols1, Options) ->
  Cols = [binary_to_list(H) || H <- Cols1],
  Record = atom_to_binary(proplists:get_value(header, Options, csv), latin1),
  RecordSize = proplists:get_value(size, Options, length(Cols) + 1),
  Map = lists:map(fun(H) ->
    case lists:keyfind(H, 1, Options) of
      {H, OutPos, Filter} ->
        <<OutPos, (filter(Filter))>>;
      false ->
        <<-1, $s>>
    end    
  end, Cols),
  iolist_to_binary([<<(size(Record)), Record/binary, RecordSize, (length(Map))>>, Map]).

init(Path, Options) ->
  {OpenOptions, TotalCmd} = case re:run(Path, "\\.gz$") of
    nomatch -> {[{read_ahead, 1024*1024}], "wc -l \""++Path++"\""};
    _ -> {[compressed], "gzcat \""++Path++"\" | wc -l"}
  end,
  {match, [Tot]} = re:run(os:cmd(TotalCmd), "(\\d+)", [{capture,all_but_first,list}]),
  Total = list_to_integer(Tot),
  case file:open(Path, [raw, binary|OpenOptions]) of
    {ok, F} ->
      start_loader1(Path, Options, F, Total);
    {error, Error} ->
      {error, Error}
  end.

start_loader1(Path, Options, F, Total) ->
  {ok, Header1} = file:read_line(F),
  [Header2, <<>>] = binary:split(Header1, [<<"\n">>]),
  Header = binary:split(Header2, [<<",">>], [global]),
  Pattern = compile_pattern(Header, Options),
  
  Loader1 = #loader{
    file = Path,
    header = Header,
    offset = size(Header1),
    cols = length(Header),
    pattern = Pattern,
    fd = F,
    total = Total,
    options = Options
  },
  {ok, Loader1}.

next(#loader{fd = F, buffer = Buffer, pattern = Pattern, count = Count} = Loader) ->
  case file:read(F, 65536) of
    {ok, Bin} ->
      {Lines, Rest} = split_lines(<<Buffer/binary, Bin/binary>>, Pattern),
      {ok, Lines, Loader#loader{buffer = Rest, count = Count + length(Lines)}};
    {error, Error} ->
      {error, Error};
    eof when Buffer == <<>> ->
      {eof, Count};
    eof ->  
      {Lines, _} = split_lines(<<Buffer/binary, "\n">>, Pattern),
      {ok, Lines, Loader#loader{buffer = <<>>, count = Count + length(Lines)}}
  end.    


split_lines(Bin, Pattern) -> split_lines(Bin, [], Pattern).

split_lines(Bin, Acc, Pattern) ->
  case parse_line(Bin, Pattern) of
    {undefined, Rest} ->
      {lists:reverse(Acc), Rest};
    {Line, Rest} ->
      % ?D(Line),
      split_lines(Rest, [Line|Acc], Pattern)
  end.
