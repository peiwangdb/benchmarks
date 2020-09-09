Scripts for running TPC-DS on OpenLookeng
====================================

This directory contains the scripts we use for producing TPC-DS results on OpenLookeng.
The scripts are a bit rough, use with caution!

Includes:
  1. Set of TPCDS queries that can be ran on Presto, based on TPCDS v2.4 queries from spark-sql-perf.
    * 3 queries (26, 70, 86) are disabled, because they use unsupported "grouping" in rollup.
    * Several queries have been rewritten to have explicit casts between string and date, that didn't work automatically in Presto.
    * Several queries have been rewritten to use double-quotes (") instead of backticks (`) for alias names.
    * No more advanced rewrites have been made.
  2. Python script providing convenience functions to run the queries.

Usage
-----

Results of a run will be saved in `results/timestamp=NNN`.
