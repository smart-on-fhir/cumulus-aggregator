# Test data

Since it is not explict, here's a quick breakdown on the data files in this directory:

## cube_simple_example.csv

This is a toy example of the kind of powerset data the Cumulus libary produces. It is used for checking invalid type uploads - but it is also a human-readable example (without special tooling) of the parquet dataset.

## cube_simple_example.parquet

This is the compressed columnar version of cube_simple_example.csv. This file is used to actually test the powerset merging functionality.

## cube_*.json

These are example responses for the site_upload set of lambdas - i.e. they represent the response format that the Cumulus dashboard is expecting.
