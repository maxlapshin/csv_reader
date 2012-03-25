-module(csv_reader).

-on_load(init_nif/0).
-define(D(X), io:format("~p:~p ~p~n", [?MODULE, ?LINE, X])).

-export([init/2, next/2]).
-export([date_to_ms/2]).

-export([start_loader/3, start_splitter/1, start_formatter/2]).

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
  % Path = filename:dirname(code:which(?MODULE)) ++ "/../priv",
  % Load = erlang:load_nif(Path ++ "/csv_reader", 0),
  % io:format("Load csv_reader: ~p~n", [Load]),
  ok.


init(Path, Options) ->
  % {ok, Reader} = csv_open(Path, Options),
  % {ok, Reader}.
  {ok, Reader} = proc_lib:start(?MODULE, start_loader, [Path, Options, self()]),
  {ok, Reader}.

-record(loader, {
  file,
  header,
  cols,
  splitter,
  formatter,
  client
}).

start_loader(Path, _Options, Parent) ->

  try start_loader0(Path, _Options, Parent) of
    Result -> Result
  catch
    Class:Error ->
      ?D({failed_loader, Class, Error, erlang:get_stacktrace()})
  end.    
      

start_loader0(Path, _Options, Parent) ->
  
  {ok, F} = file:open(Path, [raw, binary, {read_ahead, 1024*1024}]),
  proc_lib:init_ack(Parent, {ok, self()}),
  {ok, Header1} = file:read_line(F),
  [Header2, <<>>] = binary:split(Header1, [<<"\n">>]),
  
  Header = binary:split(Header2, [<<",">>], [global]),
  file:position(F, size(Header1)),
  {ok, Formatter} = proc_lib:start_link(?MODULE, start_formatter, [length(Header), Parent]),
  erlang:monitor(process, Formatter),
  {ok, Splitter} = proc_lib:start_link(?MODULE, start_splitter, [Formatter]),
  erlang:monitor(process, Splitter),
  ?D({init_loader,Path,Parent, Header}),
  Loader = #loader{
    file = F,
    header = Header,
    cols = length(Header),
    splitter = Splitter,
    formatter = Formatter,
    client = Parent
  },
  loader(Loader).


loader(#loader{splitter = Splitter, file = F, formatter = Formatter, client = Client} = Loader) ->
  case process_info(Splitter, message_queue_len) of
    {message_queue_len, Len} when Len > 20 ->
      ?D({loader_delay, Len}),
      timer:sleep(100),
      loader(Loader);
    {message_queue_len, _} ->
      case file:read(F, 128*1024) of
        {ok, Bin} ->
          % ?D({loader, size(Bin)}),
          io_lib:fread("~s,~4..0B~2..0B~2..0B,~2..0B:~2..0B:~2..0B.~3..0B,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~f,~s~n", Bin),
          % Splitter ! {bin, Bin},
          loader(Loader);
        eof ->
          ?D({loader,eof}),
          Splitter ! eof,
          % receive
          %   {'DOWN', _, _, Splitter, _} -> ok
          % end,
          % ?D(splitter_down),
          % receive
          %   {'DOWN', _, _, Formatter, _} -> ok
          % end,
          Client ! eof,
          ok
      end
  end.    


start_splitter(Formatter) ->
  Pattern = binary:compile_pattern([<<"\n">>, <<",">>]),
  proc_lib:init_ack({ok, self()}),
  splitter(Pattern, Formatter).

splitter(Pattern, Formatter) ->
  receive
    {bin, Bin} ->
      % ?D({split,size(Bin)}),
      % [Head1|Parts1] = binary:split(Bin, Pattern, [global]),
      % {Parts2, [Rem]} = lists:split(length(Parts1) - 1, Parts1),
      % Head2 = case Remaining of
      %   <<>> -> Head1;
      %   _ -> <<Remaining/binary, Head1/binary>>
      % end,
      % Parts3 = [Head2|Parts2],
      Parts = binary:split(Bin, Pattern, [global]),
      % ?D({send, length(Parts)}),
      send_splitted_parts(Formatter, Parts),
      splitter(Pattern, Formatter);
    eof ->
      ?D({splitter, eof}),
      Formatter ! eof,
      ok
  end.      

send_splitted_parts(Formatter, Parts) ->
  case process_info(Formatter, message_queue_len) of
    {message_queue_len, Len} when Len > 20 ->
      timer:sleep(300),
      ?D({splitter_delay,Len}),
      send_splitted_parts(Formatter, Parts);
    {message_queue_len, _} ->
      Formatter ! {parts, Parts},
      ok
  end.    

start_formatter(Cols, Consumer) ->
  proc_lib:init_ack({ok, self()}),
  formatter(Cols, Consumer, [], <<>>).

formatter(Cols, Consumer, Acc, Rem) ->
  receive
    {parts, [H|T] = Parts1} ->
      Parts2 = case Rem of
        <<>> -> Parts1;
        _ -> [<<Rem/binary, H/binary>>|T]
      end,
      Parts3 = Acc ++ Parts2,
      Acc2 = split_and_send(Consumer, Cols, Parts3),
      {Acc3, [Rem1]} = lists:split(length(Acc2) - 1, Acc2),
      formatter(Cols, Consumer, Acc3, Rem1);
      % formatter(Cols, Consumer, Acc, Rem);
    eof ->
      case {Acc, Rem} of
        {[], <<>>} -> ok;
        _ -> Consumer ! {csv, Acc ++ [Rem]}
      end,
      ok
  end.    

split_and_send(Consumer, Cols, Parts) when length(Parts) > Cols ->
  {Line, Rest} = lists:split(Cols, Parts),
  Consumer ! {csv, Line},
  split_and_send(Consumer, Cols, Rest);

split_and_send(_Consumer, _Cols, Rest) ->
  Rest.
      
  
next(_Reader, _Count) ->
  receive
    {csv, Line} -> Line;
    eof -> undefined
  end.
