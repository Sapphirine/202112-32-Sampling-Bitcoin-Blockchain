#!/usr/bin/env bash

PASSTHROUGH=("--name GraphSampler")
INPUT=test-data/address-graph
OUTPUT=test-data/address-graph-sampled-random-node
SAMPLER_TYPE=2 # Random node

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
        -s|--sampler-type)
            ANALYSIS_TYPE=$2
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
if [[ -z "$INPUT" || -z "$OUTPUT" || -z "$SAMPLER_TYPE" ]]; then
    echo "Help: This application samples a graph"
    echo "-i|--input          Input path to graph for analysis"
    echo "-o|--output         Output path for analysis results"
    echo "-s|--sampler-type   Sampler type (1=random edge, 2=random node)"
    echo ""
    exit 1
fi

# Helper script to run with a local Spark/HDFS setup
$SPARK_HOME/bin/spark-submit \
    $PASSTHROUGH[@] \
    --class edu.columbia.eecs6893.btc.graph.GenericGraphSampler \
    ./target/scala-2.12/bitcoin-graph-builder-assembly-1.0.jar \
    -g 1 \
    -i $INPUT \
    -o $OUTPUT \
    -s $SAMPLER_TYPE

