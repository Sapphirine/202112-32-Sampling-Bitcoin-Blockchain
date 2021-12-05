#!/usr/bin/env bash

if [[ -z "$JAR_PATH" ]]; then
    JAR_PATH="."
fi

PASSTHROUGH=("--name GraphBuilder")
INPUT=test-data/raw-btc-transactions
OUTPUT=test-data/address-graph

# CLI parser
while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--input)
            INPUT=$2
            shift
            shift
            ;;
        -o|--output)
            OUTPUT=$2
            shift
            shift
            ;;
        *)
            PASSTHROUGH+=("$1")
            shift
            ;;
    esac
done

# Print help if we don't have what we need to execute
if [[ -z "$INPUT" || -z "$OUTPUT" ]]; then
    echo "Help: This application builds an address graph"
    echo "-i|--input          Input path to graph for analysis"
    echo "-o|--output         Output path for analysis results"
    echo ""
    exit 1
fi

# Helper script to run with a local Spark/HDFS setup
# To run it, simply place the "raw-btc-transactions" directory into the "test-data" directory.
$SPARK_HOME/bin/spark-submit \
    $PASSTHROUGH[@] \
    --class edu.columbia.eecs6893.btc.graph.GenericGraphBuilder \
    $JAR_PATH/bitcoin-graph-builder-assembly-1.0.jar \
    -p 1 \
    -i $INPUT \
    -o $OUTPUT
