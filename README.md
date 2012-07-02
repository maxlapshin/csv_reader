CSV reader
==========



This is a fast csv reader for erlang. At least it must be very fast.


Compile
-------

```
make
```

Run benchmark
-------------

Ruby is required for simple script fill.rb that creates example.csv
Target is 300 000 lines and about 70 MB size.

```
csv_reader$ time wc -l example.csv 
  300001 example.csv

real	0m0.105s
user	0m0.078s
sys	0m0.027s
```

So it means that we should take at most 200 milliseconds for reading 300 000 lines of CSV in erlang.

But now it takes 4000 milliseconds:

```
csv_reader$ make bench
./rebar compile
==> csv_reader (compile)
./csv_bench.erl example.csv
Load csv_reader: ok
Load time: 4559
```

Plain naive reading and parsing in erlang takes ages.
