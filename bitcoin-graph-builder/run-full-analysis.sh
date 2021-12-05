#!/usr/bin/env bash

# Exit if any command fails:
set -e


PASSTHROUGH=("")
DIR_PREFIX=./test-data
RAW_DATA=raw-btc-transactions
SKIP_BUILD=0
SKIP_UNSAMPLED_ANALYSIS=0
SKIP_SAMPLING=0
SKIP_SAMPLED_ANALYSIS=0
SKIP_STATISTIC=0

# CLI parser
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p|--prefix)
            DIR_PREFIX=$2
            shift
            shift
            ;;
        -d|--directory)
            RAW_DATA=$2
            shift
            shift
            ;;
        -s1)
            SKIP_BUILD=1
            shift
            ;;
        -s2)
            SKIP_UNSAMPLED_ANALYSIS=1
            shift
            ;;
        -s3)
            SKIP_SAMPLING=1
            shift
            ;;
        -s4)
            SKIP_SAMPLED_ANALYSIS=1
            shift
            ;;
        -s5)
            SKIP_STATISTIC=1
            shift
            ;;
        -h|--help)
            HELP=1
            shift
            ;;
        *)
            PASSTHROUGH+=("$1")
            shift
            ;;
    esac
done

# Print help if we don't have what we need to execute
if [[ -z "$DIR_PREFIX" || -z "$RAW_DATA" || ! -z "$HELP" ]]; then
    echo "Help: This appliation runs a full analysis pipeline"
    echo "-d|--directory  Raw input directory name. (default=raw-btc-transactions)"
    echo "-p|--prefix     Data input/output prefix. (default=./test-data)"
    echo "-s1             Skip initial graph building step."
    echo "-s2             Skip graph analysis on unsampled graph."
    echo "-s3             Skip graph sampling."
    echo "-s4             Skip analysis on sampled graph."
    echo "-s5             Skip statistic analysis and comparison."
    echo ""
    exit 1
fi

# Setup pref# Build address graph
RAW_INPUT_PATH="$DIR_PREFIX/$RAW_DATA"
ADDRESS_GRAPH_PATH="$DIR_PREFIX/address-graph"

if [[ ! $SKIP_BUILD -eq 1 ]]; then
    echo "Building graph..."
    ./run-graph-builder.sh \
        $PASSTHROUGH[@] \
        -i $RAW_INPUT_PATH \
        -o $ADDRESS_GRAPH_PATH
else
    echo "Skipping the build graph step."
fi

# Run graph analysis on unsampled address-graph
function run_analysis() {
    INPUT_GRAPH=$1
    OUTPUT_PATH=$2
    ANALYSIS=$3
    ./run-graph-analyzer.sh \
        $PASSTHROUGH[@] \
        -i $INPUT_GRAPH \
        -o $OUTPUT_PATH \
        -t $ANALYSIS &
}

INDEGREE_UNSAMPLED_PATH="$DIR_PREFIX/unsampled-indegree"
OUTDEGREE_UNSAMPLED_PATH="$DIR_PREFIX/unsampled-outdegree"

if [[ ! $SKIP_UNSAMPLED_ANALYSIS -eq 1 ]]; then
    echo "Running unsampled analysis..."
    ./run-graph-analyzer.sh \
        $PASSTHROUGH[@] \
        -i $ADDRESS_GRAPH_PATH \
        -o $INDEGREE_UNSAMPLED_PATH \
        -t 2 &
    run_analysis $ADDRESS_GRAPH_PATH $INDEGREE_UNSAMPLED_PATH 2
    P1=$!
    
    run_analysis $ADDRESS_GRAPH_PATH $OUTDEGREE_UNSAMPLED_PATH 3
    P2=$!

    wait $P1
    wait $P2
else
    echo "Skipping unsampled graph analysis steps."
fi

# Sample graph
RANDOM_NODE_SAMPLED_GRAPH="$DIR_PREFIX/random-node-sampled-graph"
RANDOM_EDGE_SAMPLED_GRAPH="$DIR_PREFIX/random-edge-sampled-graph"
if [[ ! $SKIP_SAMPLING -eq 1 ]]; then
    echo "Running graph sampling..."
    ./run-graph-sampler.sh \
        $PASSTHROUGH[@] \
        -i $ADDRESS_GRAPH_PATH \
        -o $RANDOM_NODE_SAMPLED_GRAPH \
        -s 2 &
    P1=$!

    ./run-graph-sampler.sh \
        $PASSTHROUGH[@] \
        -i $ADDRESS_GRAPH_PATH \
        -o $RANDOM_EDGE_SAMPLED_GRAPH \
        -s 1 &
    P2=$!

    wait $P1
    wait $P2
else
    echo "Skipping graph sampling..."
fi

# Run analysis on sample graphs
RANDOM_NODE_INDEGREE="$DIR_PREFIX/random-node-indegree"
RANDOM_NODE_OUTDEGREE="$DIR_PREFIX/random-node-outdegree"
RANDOM_EDGE_INDEGREE="$DIR_PREFIX/random-edge-indegree"
RANDOM_EDGE_OUTDEGREE="$DIR_PREFIX/random-edge-outdegree"
if [[ ! $SKIP_SAMPLED_ANALYSIS -eq 1 ]]; then
    echo "Analyzing sampled graphs..."

    run_analysis $RANDOM_NODE_SAMPLED_GRAPH $RANDOM_NODE_INDEGREE 2
    P1=$!
    run_analysis $RANDOM_NODE_SAMPLED_GRAPH $RANDOM_NODE_OUTDEGREE 3
    P2=$!

    run_analysis $RANDOM_EDGE_SAMPLED_GRAPH $RANDOM_EDGE_INDEGREE 2
    P3=$!
    run_analysis $RANDOM_EDGE_SAMPLED_GRAPH $RANDOM_EDGE_OUTDEGREE 3
    P4=$!

    wait $P1
    wait $P2
    wait $P3
    wait $P4
else
    echo "Skipping sampled graph analysis."
fi

# TODO: Comparison
