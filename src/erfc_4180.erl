%% -*- mode: Erlang; fill-column: 80; comment-column: 75; -*-
%%%---------------------------------------------------------------------------
%%% @author ppolv (http://pplov.wordpress.com)
%%% @author Gordon Guthrie
%%% @author Gerald Gutierrez
%%% @author Luke Krasnoff
%%%
%%% @doc This file implements rfc 4180 - the format used for
%%%  Comma-Separated Values (CSV) files and registers the associated
%%%  MIME type "text/csv".
%%% @end
%%%-------------------------------------------------------------------

-module(erfc_4180).

-export([parse_file/4,parse_file/2,parse/4,parse/2]).

-record(ecsv,{
          state = field_start, % field_start|normal|quoted|post_quoted
          cols = undefined,    % how many fields per record
          col = 1, % current col in record
          current_field  = [],
          current_record = [],
          lines=0,
          columns=0,
          type_specs=undefined,
          fold_state,
          fold_fun             % user supplied fold function
         }).

%% ——— Exported ——————————
parse_file(FileName,InitialState,Fun,Opts) ->
    {ok, Binary} = file:read_file(FileName),
    parse(Binary,InitialState,Fun,Opts).

parse_file(FileName,Opts) ->
    {ok, Binary} = file:read_file(FileName),
    parse(Binary,Opts).

parse(Binary, Opts) ->
    R = parse(Binary,[],fun(Fold,Record) -> [Record|Fold] end,Opts),
    lists:reverse(R).

parse(Binary,InitialState,Fun,Opts) ->
    TypeSpecs = create_converters(1,
                                  lists:sort(fun(A, B) ->
                                                     element(1, A) <
                                                         element(1, B)
                                             end, Opts), []),
    do_parse(Binary,#ecsv{fold_state=InitialState,fold_fun=Fun,
                          type_specs=TypeSpecs}).

%% ——— Converters ———————
create_converters(C, [{C, Type} | Rest], Converter) ->
    create_converters(C + 1, Rest, [create_converter(Type) | Converter]);
create_converters(C, All=[{IC, _} | _], Converters) when C < IC ->
    create_converters(C + 1, All, [fun default_converter/1 | Converters]);
create_converters(C, [{IC, _} | _Rest], _Converters) when C > IC ->
    erlang:throw({?MODULE, invalid_type_spec});
create_converters(_, [], Converters) ->
    erlang:list_to_tuple(lists:reverse(Converters)).

default_converter(String) ->
    String.

create_converter(existing_atom) ->
    fun(Atom) ->
            erlang:list_to_existing_atom(Atom)
    end;
create_converter(atom) ->
    fun(Atom) ->
            erlang:list_to_atom(Atom)
    end;
create_converter(integer) ->
    fun([]) ->
            0;
       (Int) ->
            erlang:list_to_integer(string:strip(Int))
    end;
create_converter(currency) ->
    fun([]) ->
            0.0;
       (Float0) ->
            Float1 =
                lists:filter(fun($.) ->
                                     true;
                                ($-) ->
                                     true;
                                (Char) when Char >= $0 andalso Char =< $9 ->
                                     true;
                                (_) ->
                                     false
                             end, Float0),
            erlang:list_to_float(string:strip(Float1))
    end;
create_converter(float) ->
    fun([]) ->
            0.0;
       (Float) ->
            erlang:list_to_float(string:strip(Float))
    end;
create_converter(string) ->
    fun(String) ->
           String
    end;
create_converter(date) ->
    fun(String) ->
            ec_date:parse(String)
    end.

convert(Index, Element, Converters) when Index =< erlang:size(Converters) ->
    (erlang:element(Index, Converters))(Element);
convert(_, Element, _) ->
    default_converter(Element).

%% ——— Field_start state ———————
%%whitespace, loop in field_start state
do_parse(<<$\s,Rest/binary>>,S = #ecsv{state=field_start,current_field=Field,columns=Cols})->
    do_parse(Rest,S#ecsv{current_field=[$\s|Field],columns=Cols+1});

do_parse(<<$\t,Rest/binary>>,S = #ecsv{state=field_start,current_field=Field,columns=Cols})->
    do_parse(Rest,S#ecsv{current_field=[$\t|Field], columns=Cols+1});

%%its a quoted field, discard previous whitespaces
do_parse(<<$",Rest/binary>>,S = #ecsv{state=field_start,columns=Cols})->
    do_parse(Rest,S#ecsv{state=quoted,current_field=[],columns=Cols+1});

%%anything else, is a unquoted field
do_parse(Bin,S = #ecsv{state=field_start})->
    do_parse(Bin,S#ecsv{state=normal});

%% ——— Quoted state ———————
%%Escaped quote inside a quoted field
do_parse(<<$",$",Rest/binary>>,S = #ecsv{state=quoted,current_field=Field,columns=Cols})->
    do_parse(Rest,S#ecsv{current_field=[$"|Field],columns=Cols+2});

%%End of quoted field
do_parse(<<$",Rest/binary>>,S = #ecsv{state=quoted,columns=Cols})->
    do_parse(Rest,S#ecsv{state=post_quoted,columns=Cols+1});

%%Anything else inside a quoted field
do_parse(<<X,Rest/binary>>,S = #ecsv{state=quoted,current_field=Field,columns=Cols})->
    do_parse(Rest,S#ecsv{current_field=[X|Field],columns=Cols+1});

do_parse(<<>>, #ecsv{state=quoted,columns=Cols,lines=Lines})->
    throw({ecsv_exception,unclosed_quote, Lines, Cols});

%% ——— Post_quoted state ———————
%%consume whitespaces after a quoted field
do_parse(<<$\s,Rest/binary>>,S = #ecsv{state=post_quoted,columns=Cols})->
    do_parse(Rest,S#ecsv{columns=Cols+1});
do_parse(<<$\t,Rest/binary>>,S = #ecsv{state=post_quoted,columns=Cols})->
    do_parse(Rest,S#ecsv{columns=Cols+1});

%%———Comma and New line handling. ——————
%%———Common code for post_quoted and normal state—

%%EOF in a new line, return the records
do_parse(<<>>, #ecsv{current_record=[],fold_state=State})->
    State;
%%EOF in the last line, add the last record and continue
do_parse(<<>>,S)->
    do_parse(<<>>,new_record(S));

%% new record windows
do_parse(<<$\r,$\n,Rest/binary>>,S = #ecsv{})->
    do_parse(Rest,new_record(S));

%% new record pre Mac OSX 10
do_parse(<<$\r,Rest/binary>>,S = #ecsv{}) ->
    do_parse(Rest,new_record(S));

%% new record Unix
do_parse(<<$\n,Rest/binary>>,S = #ecsv{}) ->
    do_parse(Rest,new_record(S));

do_parse(<<$, ,Rest/binary>>,S = #ecsv{current_field=Field,col=Col,
                                       current_record=Record,columns=Cols,
                                       type_specs=Specs})->
    do_parse(Rest,S#ecsv{state=field_start,
                         current_field=[],
                         col=Col+1,
                         columns=Cols+1,
                         current_record=[convert(Col, lists:reverse(Field), Specs) |
                                         Record]});

%%A double quote in any other place than the already managed is an error
do_parse(<<$",_Rest/binary>>, #ecsv{lines=Lines,columns=Cols})->
    throw({ecsv_exception,bad_record,Lines,Cols});

%%Anything other than whitespace or line ends in post_quoted state is an error
do_parse(<<_X,_Rest/binary>>, #ecsv{state=post_quoted,lines=Lines,columns=Cols})->
    throw({ecsv_exception,bad_record,Lines,Cols});

%%Accumulate Field value
do_parse(<<X,Rest/binary>>,S = #ecsv{state=normal,current_field=Field,columns=Cols})->
    do_parse(Rest,S#ecsv{current_field=[X|Field],columns=Cols+1}).

%%check the record size against the previous, and actualize state.
new_record(S=#ecsv{cols=Cols,current_field=Field,current_record=Record,
                   fold_state=State,fold_fun=Fun,lines=Lines}) ->
    RecList = [lists:reverse(Field)|Record],
    NewRecord = list_to_tuple(lists:reverse(RecList)),
    if
        (tuple_size(NewRecord) =:= Cols) or (Cols =:= undefined) ->
            NewState = Fun(State,NewRecord),
            S#ecsv{state=field_start,cols=tuple_size(NewRecord),
                   lines=Lines+1, columns=0,col=1,
                   current_record=[],current_field=[],fold_state=NewState};

        (tuple_size(NewRecord) =/= Cols) ->
            throw({ecsv_exception,bad_record_size, Lines, Cols})
    end.

%% ——– Regression tests ————————
%% From the erl interpreter run csv:test() to run regression tests.
%% See eunit for more information.
-ifndef(NOTEST).
-include_lib("eunit/include/eunit.hrl").

csv_test() ->
    %% empty binary
    ?assertEqual([], parse(<<>>, [])),
    %% Unix LF
    ?assertEqual([{"1A","1B","1C"},{"2A","2B","2C"}],
                 parse(<<"1A,1B,1C\n2A,2B,2C">>, [])),
    %% Unix LF with extra spaces after quoted element stripped
    ?assertEqual([{"1A","1B","1C"},{"2A","2B","2C"}],
                 parse(<<"\"1A\"   ,\"1B\" ,\"1C\"",10,"\"2A\" ,\"2B\",\"2C\"">>, [])),
    %% Unix LF with extra spaces preserved in unquoted element
    ?assertEqual([{" 1A ","1B","1C"},{"2A","2B","2C"}],
                 parse(<<" 1A ,1B,1C\n2A,2B,2C">>, [])),
    %% Pre Mac OSX 10 CR
    ?assertEqual([{"1A","1B","1C"},{"2A","2B","2C"}],
                 parse(<<"1A,1B,1C\r2A,2B,2C">>, [])),
    %% Windows CRLF
    ?assertEqual([{"1A","1B","1C"},{"2A","2B","2C"}],
                 parse(<<"1A,1B,1C\r\n2A,2B,2C">>, [])),

    %% Quoted element
    ?assertEqual([{"1A","1B"}],
                 parse(<<"1A,1B">>, [])),
    %% Nested quoted element
    ?assertEqual([{"1A","\"1B\""}],
                 parse(<<"\"1A\",\"\"\"1B\"\"\"">>, [])),
    %% Quoted element with embedded LF
    ?assertEqual([{"1A","1\nB"}],
                 parse(<<"\"1A\",\"1\nB\"">>, [])),
    %% Quoted element with embedded quotes (1)
    ?assertThrow({ecsv_exception,bad_record,0,7},
                 parse(<<"\"1A\",","\"\"B\"">>, [])),
    %% Quoted element with embedded quotes (2)
    ?assertEqual([{"1A","blah\"B"}],
                 parse(<<"\"1A\",\"blah\"",$","B\"">>, [])), %"
    %% Missing 2nd quote
    ?assertThrow({ecsv_exception,unclosed_quote,0,8},
                 parse(<<"\"1A\",\"2B">>, [])),
    %% Bad record size
    ?assertThrow({ecsv_exception,bad_record_size, _, _},
                 parse(<<"1A,1B,1C\n2A,2B\n">>, [])).

-endif.
