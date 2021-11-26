#!/usr/bin/env bash
# Script used to copy the subset of the data we want to export into bigquery so we can extract
# it for processing in HDFS
bq query \
    --use_legacy_sql=false \
    --destination_table eecs6893-325714:btc_copy.data \
    --replace  \
    --allow_large_results \
    "SELECT *
    FROM bigquery-public-data.crypto_bitcoin.transactions
    WHERE block_timestamp_month >= '2021-01-01' AND block_timestamp_month <= '2021-11-01'
    ORDER BY lock_time ASC"

