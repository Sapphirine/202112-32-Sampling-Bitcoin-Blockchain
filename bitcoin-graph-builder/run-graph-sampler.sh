#!/usr/bin/env bash

# Helper script to run with a local Spark/HDFS setup
# To run it, simply place the "raw-btc-transactions" directory into the "test-data" directory.
$SPARK_HOME/bin/spark-submit \
    --master "local[*]" \
    --total-executor-cores 5 \
    --driver-memory 2g \
    --executor-memory  10g \
    --name GraphSampler \
    --class edu.columbia.eecs6893.btc.graph.GenericGraphSampler \
    ./target/scala-2.12/bitcoin-graph-builder-assembly-1.0.jar \
    -i test-data/address-graph \
    -o test-data/address-graph-sampled \
    -g 1 \
    -s 1

