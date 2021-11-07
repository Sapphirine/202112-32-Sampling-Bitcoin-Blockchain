#!/usr/bin/env bash

# Helper script to run with a local Spark/HDFS setup
# To run it, simply place the "raw-btc-transactions" directory into the "test-data" directory.
$SPARK_HOME/bin/spark-submit \
    --master "local" \
    --driver-memory 2g \
    --executor-memory  2g \
    --name GraphBuilder \
    --class edu.columbia.eecs6893.btc.graph.GenericGraphBuilder \
    ./target/scala-2.12/bitcoin-graph-builder-assembly-1.0.jar \
    -i test-data/raw-btc-transactions \
    -o test-data/address-graph
