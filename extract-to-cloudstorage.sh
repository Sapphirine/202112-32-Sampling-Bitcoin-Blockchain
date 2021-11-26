#!/usr/bin/env bash
# Script to extract from bigquery table to Cloud Storage so it can be used
# to load up into HDFS
bq extract \
    --destination_format PARQUET \
    'eecs6893-325714:btc_copy.data' \
    'gs://eecs6893-btc-project/raw-btc-export/*'
