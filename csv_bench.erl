#!/usr/bin/env escript
%%! -pa ebin -smp enable +K true +A 8

-mode(compile).

-record(evt, {
  instrument,
  date,
  time,
  utc,
  type,
  l1_bid_price,l1_bid_size,l1_ask_price,l1_ask_size,
  l2_bid_price,l2_bid_size,l2_ask_price,l2_ask_size,
  l3_bid_price,l3_bid_size,l3_ask_price,l3_ask_size,
  l4_bid_price,l4_bid_size,l4_ask_price,l4_ask_size,
  l5_bid_price,l5_bid_size,l5_ask_price,l5_ask_size,
  l6_bid_price,l6_bid_size,l6_ask_price,l6_ask_size,
  l7_bid_price,l7_bid_size,l7_ask_price,l7_ask_size,
  l8_bid_price,l8_bid_size,l8_ask_price,l8_ask_size,
  l9_bid_price,l9_bid_size,l9_ask_price,l9_ask_size,
  l10_bid_price,l10_bid_size,l10_ask_price,l10_ask_size
}).

main([Path]) ->
  code:add_pathz("./ebin"),
  ets:new(csv_entries, [public,named_table,{keypos,#evt.time}]),
  
  LoadFun = fun(Lines) -> 
    ets:insert(csv_entries, Lines)
  end,
  
  
  {ok, F} = csv_reader:init(Path, [{header, evt}, {size, size(#evt{})}, {loader, LoadFun},
    {"#RIC", #evt.instrument, undefined}, {"<TICKER>", #evt.instrument, undefined},
    {"Date[G]", #evt.date, date}, {"<Date>", #evt.date, date},
    {"Time[G]", #evt.time, time}, {"<Time>", #evt.date, date},
    {"GMT Offset", #evt.utc, utc},
    {"Type", #evt.type, undefined},
  
    {"Bid Price",#evt.l1_bid_price,float},{"Bid Size",#evt.l1_bid_size,int},{"Ask Price",#evt.l1_ask_price,float},{"Ask Size",#evt.l1_ask_size,int},
    {"L1-BidPrice",#evt.l1_bid_price,float},{"L1-BidSize",#evt.l1_bid_size,int},{"L1-AskPrice",#evt.l1_ask_price,float},{"L1-AskSize",#evt.l1_ask_size,int},
    {"L2-BidPrice",#evt.l2_bid_price,float},{"L2-BidSize",#evt.l2_bid_size,int},{"L2-AskPrice",#evt.l2_ask_price,float},{"L2-AskSize",#evt.l2_ask_size,int},
    {"L3-BidPrice",#evt.l3_bid_price,float},{"L3-BidSize",#evt.l3_bid_size,int},{"L3-AskPrice",#evt.l3_ask_price,float},{"L3-AskSize",#evt.l3_ask_size,int},
    {"L4-BidPrice",#evt.l4_bid_price,float},{"L4-BidSize",#evt.l4_bid_size,int},{"L4-AskPrice",#evt.l4_ask_price,float},{"L4-AskSize",#evt.l4_ask_size,int},
    {"L5-BidPrice",#evt.l5_bid_price,float},{"L5-BidSize",#evt.l5_bid_size,int},{"L5-AskPrice",#evt.l5_ask_price,float},{"L5-AskSize",#evt.l5_ask_size,int},
    {"L6-BidPrice",#evt.l6_bid_price,float},{"L6-BidSize",#evt.l6_bid_size,int},{"L6-AskPrice",#evt.l6_ask_price,float},{"L6-AskSize",#evt.l6_ask_size,int},
    {"L7-BidPrice",#evt.l7_bid_price,float},{"L7-BidSize",#evt.l7_bid_size,int},{"L7-AskPrice",#evt.l7_ask_price,float},{"L7-AskSize",#evt.l7_ask_size,int},
    {"L8-BidPrice",#evt.l8_bid_price,float},{"L8-BidSize",#evt.l8_bid_size,int},{"L8-AskPrice",#evt.l8_ask_price,float},{"L8-AskSize",#evt.l8_ask_size,int},
    {"L9-BidPrice",#evt.l9_bid_price,float},{"L9-BidSize",#evt.l9_bid_size,int},{"L9-AskPrice",#evt.l9_ask_price,float},{"L9-AskSize",#evt.l9_ask_size,int},
    {"L10-BidPrice",#evt.l10_bid_price,float},{"L10-BidSize",#evt.l10_bid_size,int},{"L10-AskPrice",#evt.l10_ask_price,float},{"L10-AskSize",#evt.l10_ask_size,int}
  ]),
  
  T1 = erlang:now(),
  % Events = fprof:apply(fun() -> load(F) end, []),
  {ok, Count} = csv_reader:wait(F),
  T2 = erlang:now(),
  Time = timer:now_diff(T2, T1),
  io:format("NIF: ~p, ~8.2. f us per line~n", [Time div 1000, Time / Count]),
  
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
 
