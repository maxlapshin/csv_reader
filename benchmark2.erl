#!/usr/bin/env escript
%%! -pa ebin

-mode(compile).

-record(evt, {
key,date,time,
l_1,l_2,l_3,l_4,l_5,l_6,l_7,l_8,l_9,l_10,
l_11,l_12,l_13,l_14,l_15,l_16,l_17,l_18,l_19,l_20,
l_21,l_22,l_23,l_24,l_25,l_26,l_27,l_28,l_29,l_30,
l_31,l_32,l_33,l_34,l_35,l_36,l_37,l_38,l_39,l_40,
last
}).

-record(csv_field, {
   name                :: atom() | string(),
   type = 'binary'     :: atom(),  %% TODO: use erl_types:type()
   converter           :: fun((binary()) -> term())
}).

-type transform_fun() :: fun((term()) -> term()).

-record(csv_parser, {
   io_device       :: file:io_device(),
   collector       :: pid(),
   input_file      :: file:name(),
   header          :: boolean(),
   strict = true   :: boolean(),
   size            :: non_neg_integer(),
   fields          :: [#csv_field{} | constant()],
   buffer_size = 16 * 1024
                   :: non_neg_integer(),
   transform = fun(E) -> E end
                   :: transform_fun(),
   delimiter =
       binary:compile_pattern(<<",">>)
                   :: binary:cp(),
   supported_newlines =
       binary:compile_pattern([<<"\n">>, <<"\r\n">>])
                   :: binary:cp()
}).

main([Path]) ->
   %% TODO: don't be so naive
   try
       T1 = erlang:now(),
       chunker(make_parser(Path)),
       T2 = erlang:now(),
       io:format("read_chunk time: ~p~n", [timer:now_diff(T2, T1) div 1000])
   catch
       Ex:R ->
           io:format("~p~n", [erlang:get_stacktrace()]),
           throw({Ex, R})
%    after
%        file:close(Fd)
   end.

make_parser(F) ->
   %% TODO: write a function that takes a 'typed record' and
   %% generates the csv_field mappings for us using erl_types or whatever...
   Fields = [
       #csv_field{ name="KEY" },
       #csv_field{ name="Date" },   %% NB: we will worry about converting dates later on...
       #csv_field{ name="Time" },   %% NB: we will worry about converting time(s) later on...
       #csv_field{ name="l_1", type=float }, #csv_field{ name="l_2", type=float },
       #csv_field{ name="l_3", type=float }, #csv_field{ name="l_4", type=float },
       #csv_field{ name="l_5", type=float }, #csv_field{ name="l_6", type=float },
       #csv_field{ name="l_7", type=float }, #csv_field{ name="l_8", type=float },
       #csv_field{ name="l_9", type=float }, #csv_field{ name="l_10", type=float },
       #csv_field{ name="l_11", type=float }, #csv_field{ name="l_12", type=float },
       #csv_field{ name="l_13", type=float }, #csv_field{ name="l_14", type=float },
       #csv_field{ name="l_15", type=float }, #csv_field{ name="l_16", type=float },
       #csv_field{ name="l_17", type=float }, #csv_field{ name="l_18", type=float },
       #csv_field{ name="l_19", type=float }, #csv_field{ name="l_20", type=float },
       #csv_field{ name="l_21", type=float }, #csv_field{ name="l_22", type=float },
       #csv_field{ name="l_23", type=float }, #csv_field{ name="l_24", type=float },
       #csv_field{ name="l_25", type=float }, #csv_field{ name="l_26", type=float },
       #csv_field{ name="l_27", type=float }, #csv_field{ name="l_28", type=float },
       #csv_field{ name="l_29", type=float }, #csv_field{ name="l_30", type=float },
       #csv_field{ name="l_31", type=float }, #csv_field{ name="l_32", type=float },
       #csv_field{ name="l_33", type=float }, #csv_field{ name="l_34", type=float },
       #csv_field{ name="l_35", type=float }, #csv_field{ name="l_36", type=float },
       #csv_field{ name="l_37", type=float }, #csv_field{ name="l_38", type=float },
       #csv_field{ name="l_39", type=float }, #csv_field{ name="l_40", type=float },
       #csv_field{ name="Last" }
   ],
   #csv_parser{
       input_file=F,
       header=true,
       strict=true,
       transform=fun(L) -> list_to_tuple(['evt'] ++ L) end,
       size=length(Fields),
       fields=Fields
   }.

%parse(P=#csv_parser{ iodevice=Fd, buffer_size=Size }) ->
%    parse(P, file:read(Fd, Size), 0, undefined, []).

%% the butcher process chops up the binary into chunks
chunker(P=#csv_parser{ input_file=F, buffer_size=Size }) ->
   {ok, Fd} = file:open(F, [raw, binary, {read_ahead, 1024 * 1024}]),
   CPid = spawn(fun loop/0),
   read(P#csv_parser{ io_device=Fd, collector=CPid }, start, 0, <<>>, []).

loop() ->
   receive {ok, _Pid, _X} ->
       % io:format("Got ~p parts!~n", [length(X)])
       loop()
   end.

read(P=#csv_parser{ io_device=Fd, buffer_size=Size }, start, Idx, Rem, Pids) ->
   read(P, file:read(Fd, Size), Idx, Rem, Pids);
read(P=#csv_parser{ io_device=Fd, buffer_size=Size,
                   delimiter=Delim, collector=Parent,
                   supported_newlines=NL },
                   {ok, Chunks},
                   Idx, Rem, Pids) ->
   % io:format("read ~p characters~n", [size(Chunk)]),
   Lines = binary:split(Chunks, NL, [global]),
   {Data, [Remaining]} = lists:split(length(Lines) - 1, Lines),
   Pid = spawn(fun() ->
                   [H|T] = Data,
                   Sz = size(H),
                   %% TODO: fix this code so it properly deals with
                   %% left over data between processed segments
                   %% i.e., where is \n in relation to Rem ....
                   WorkingSet = case size(Rem) of
                       X when X < Sz ->
                           [<<Rem/binary, H/binary>>|T];
                       _ ->
                           [Rem|Data]
                   end,
                   [ return(Parent, binary:split(L, Delim, [global])) ||
                                   L <- WorkingSet, is_binary(L) ]
               end),
   read(P, file:read(Fd, Size), Idx + 1, Remaining, [{Pid, Idx}|Pids]);
read(_P, eof, _, _Rem, _Pids) ->
   %% TODO: in strict mode, fail unless size(Rem) == 0
   ok.

return(Collector, Matches) ->
   Collector ! {ok, self(), Matches}.

collect_results(Results, []) ->
   array:to_list(Results);
collect_results(Results, Pids) ->
   receive
       {ok, Pid, Data} ->
           {value, {Pid, Idx}, RemainingPids} = lists:keytake(Pid, 1, Pids),
           collect_results(array:set(Idx, Data, Results), RemainingPids);
       Other ->
           throw(Other)
   end.

process(P=#csv_parser{ strict=Strict, fields=Fields }, Data) ->
   case lists:foldl(fun process_field/2, {Fields, [], Strict}, Data) of
       {[], Acc, _} ->
           (P#csv_parser.transform)(Acc);
       {MissingFields, Result, _} when is_list(MissingFields) ->
           case Strict of
               true ->
                   throw({parse_error, {unexpected_eol, MissingFields}});
               false ->
                   Result
           end
   end.

process_field(_E, {[], Acc, false}) ->
   Acc;
process_field(E, {[], _Acc, true}) ->
   throw({parse_error, {expected_eol, E}});
process_field(E, {[#csv_field{ type=float }|Rest], Acc, Strict}) ->
   {Rest, [list_to_float(binary_to_list(E))|Acc], Strict};
process_field(E, {[#csv_field{ type=binary }|Rest], Acc, Strict}) ->
   {Rest, [E|Acc], Strict};
process_field(E, {[#csv_field{ type=list }|Rest], Acc, Strict}) ->
   {Rest, [binary_to_list(E)|Acc], Strict}.
