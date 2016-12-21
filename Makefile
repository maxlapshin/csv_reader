
all:
    ./rebar3 eunit


bench: all example.csv
    ./csv_bench.erl example.csv


example.csv:
    ./fill.rb
