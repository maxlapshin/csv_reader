#!/usr/bin/env escript
%%! -pa ebin -smp enable +K true +A 8

-mode(compile).

-record(evt, {
key,date,time,
l_1,l_2,l_3,l_4,l_5,l_6,l_7,l_8,l_9,l_10,
l_11,l_12,l_13,l_14,l_15,l_16,l_17,l_18,l_19,l_20,
l_21,l_22,l_23,l_24,l_25,l_26,l_27,l_28,l_29,l_30,
l_31,l_32,l_33,l_34,l_35,l_36,l_37,l_38,l_39,l_40,
last
}).

main([Path]) ->
  code:add_pathz("./ebin"),
  
  {ok, F} = csv_reader:init(Path, [{header, evt}, {size, size(#evt{})},
    {"KEY", #evt.key, undefined},
    {"Date", #evt.date, date, "Time"},
    {"Time", #evt.time, time, "Date"},
    
    {"l_1",#evt.l_1,float},{"l_2",#evt.l_2,float},{"l_3",#evt.l_3,float},{"l_4",#evt.l_4,float},{"l_5",#evt.l_5,float},
    {"l_6",#evt.l_6,float},{"l_7",#evt.l_7,float},{"l_8",#evt.l_8,float},{"l_9",#evt.l_9,float},{"l_10",#evt.l_10,float},
    {"l_11",#evt.l_11,float},{"l_12",#evt.l_12,float},{"l_13",#evt.l_13,float},{"l_14",#evt.l_14,float},{"l_15",#evt.l_15,float},
    {"l_16",#evt.l_16,float},{"l_17",#evt.l_17,float},{"l_18",#evt.l_18,float},{"l_19",#evt.l_19,float},{"l_20",#evt.l_20,float},
    {"l_21",#evt.l_21,float},{"l_22",#evt.l_22,float},{"l_23",#evt.l_23,float},{"l_24",#evt.l_24,float},{"l_25",#evt.l_25,float},
    {"l_26",#evt.l_26,float},{"l_27",#evt.l_27,float},{"l_28",#evt.l_28,float},{"l_29",#evt.l_29,float},{"l_30",#evt.l_30,float},
    {"l_31",#evt.l_31,float},{"l_32",#evt.l_32,float},{"l_33",#evt.l_33,float},{"l_34",#evt.l_34,float},{"l_35",#evt.l_35,float},
    {"l_36",#evt.l_36,float},{"l_37",#evt.l_37,float},{"l_38",#evt.l_38,float},{"l_39",#evt.l_39,float},{"l_40",#evt.l_40,float},
    {"Last", #evt.last, undefined}
  ]),
  
  ets:new(entries, [public,named_table,{keypos,#evt.time}]),
  T1 = erlang:now(),
  % Events = fprof:apply(fun() -> load(F) end, []),
  _Events = load(F),
  T2 = erlang:now(),
  io:format("NIF: ~p~n", [timer:now_diff(T2, T1) div 1000]),
  
  % T3 = erlang:now(),
  % 
  % erfc_4180:parse_file("rfc.csv", [
  % {4,float},{5,float},{6,float},{7,float},{8,float},
  % {9,float},{10,float},{11,float},{12,float},{13,float},
  % {14,float},{15,float},{16,float},{17,float},{18,float},
  % {19,float},{20,float},{21,float},{22,float},{23,float},
  % {24,float},{25,float},{26,float},{27,float},{28,float},
  % {29,float},{30,float},{31,float},{32,float},{33,float},
  % {34,float},{35,float},{36,float},{37,float},{38,float},
  % {39,float},{40,float},{41,float},{42,float},{43,float}
  % ]),
  % T4 = erlang:now(),
  % io:format("RFC: ~p~n", [timer:now_diff(T4,T3) div 1000]),
  % fprof:profile(),
  % fprof:analyse(),
  ok.

load(F) ->
  case csv_reader:next(F, 100) of
    undefined ->
      ok;
    % Events when is_list(Events) ->
    %   % ets:insert(entries, Events),
    %   load(F);
    Evt ->
      % io:format("~p~n", [Evt]),
      load(F)  
    % #evt{type = Type, date = Date, time = Time, offset = GMTOfft} = Event ->
      % {YY,MM,DD} = Date,
      % {H,M,S,MS} = Time,
      % TimeS = lists:flatten(io_lib:format("~4.. B/~2..0B/~2..0B ~2..0B:~2..0B:~2..0B.~3..0B", [YY,MM,DD,H,M,S,MS])),
      % 
      % TimeS = "",
      % UTC = csv_reader:date_to_ms(Date, Time) - GMTOfft*3600,
      % Event1 = Event#evt{type = urka_loader:convert_type(Type), utc = UTC, date = {Date,Time}, time = TimeS},
      % if
      %   LastTime == undefined orelse LastTime + Skip < UTC -> Event1;
      %   true -> next(Loader)
      % end;
      % load(F, [Event1|Acc])
  end.
  
