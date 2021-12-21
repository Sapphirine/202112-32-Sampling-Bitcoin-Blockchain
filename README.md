# Bitcoin Blockchain Explorer

For EECS6893, our group has build a large graph analyzer and sampler which works on Raw Bitcoin transaction data. The intent of our project is to understand whether or not we can faithfully downsample the Bitcoin graph to make analysis easier. If we can achieve this goal, it will help for faster, more efficient analysis with less time and energy. This improves both the accessibility of analysis to improve human factos on the blockchain as well as reducing the carbon footprint of analysis.

**NOTE:** The entire project is contained within the `bitcoin-graph-builder` project. This includes the graph builder, sampler, and analyzer components.

# Prerequisites

You must have Java 11 and SBT to build the application.

Additionally, a Spark installation is required to run the scripts (either local or cluster). In particular, your `JAVA_HOME` should be set to your Java 11 JDK and `SPARK_HOME` should be set to your spark installation.

Also, the `JAR_PATH` must be set appropriately. If it's not set, it is assumed to be `pwd`. In local mode, the jar path should be set to the build location:

```
JAR_PATH=./target/scala-2.12/bitcoin-graph-builder-assembly-1.0.jar
```

where `pwd` is within the `bitcoin-graph-builder` directory.

# Building the Application

The application consists of a single jar and several different runner scripts. To build the application, run the following commands:

```
$ cd bitcoin-graph-builder
$ sbt assembly
```

# Fetching the Data

**NOTE:** For convenience, we have included a _small_, sample [raw dataset](https://github.com/DennisMcWherter/EECS6893_Project/releases/download/sample-data/raw-btc-transactions-small-50k-txns.tar) for quick download to run through the entire process as a demo. If you wish to gather your own data, please follow the instructions below.

We use the `blockchain_bigquery_loader.py` file to fetch the raw blockchain transaction data. Since Google has already stored this data in an easy-to-use format in big query, this script downloads the data from there. It then places the data into an HDFS cluster for our heavier access/computational workloads. This works well to gather a small sample set.

To gather our large dataset, we use the `copy-bigquery.sh` script which copies the data from the public BigQuery table into our own. We then use our `extract-to-cloudstorage.sh` script to copy from the BigQuery table into Cloudstorage. From here, we manually download the data from Cloudstorage and place it into HDFS for analysis.

# Running the Application

To run the application, you can either use any of the `run-graph-*.sh` scripts directly or run the entire pipeline using `run-full-analysis.sh`. We will demonstrate using the `run-full-analysis.sh` script. 

## Local Mode

In local mode, ensure that your data is located in `./test-data` (or pass script args to change. See `-h` for more).

```
$ cd bitcoin-graph-builder
$ export JAR_PATH=./target/scala-2.12
$ ./run-full-analysis.sh
```

This script will run Spark reading and writing from your local directories. The outputs will existing in the ./test-data directory.

## Cluster Mode

For larger datasets, local mode is not likely possible. As a result, you'll first want to place your raw data somewhere in HDFS (by default, the script looks for `raw-btc-transactions`). You can run a remote analysis pipeline with data in HDFS as follows:

```
$ cd bitcoin-graph-builder
$ ./run-full-analysis.sh -p hdfs:///btc-analysis -d raw-btc-transactions
```

The first parameter is the prefix for all data jobs and the second parameter is the name of the directory containing raw data _within the prefix_. Therefore, raw data for our example lives in: `hdfs:///btc-analysis/raw-btc-transactions`. All of the results can be found in `hdfs:///btc-analysis`.

# Components
## Graph Builder

The graph builder component takes the raw data from the previous step and turns it into a variety of graphs. 

## Graph Analyzer

This component takes in a graph (sampled or unsampled) and produces a set of analysis results/statistics about the graph structure.

## Graph Sampler

The graph sampler takes an input graph and samples it according to some algorithm.

# Visualization

To visualize the results, you must install python >= 3.8 with the following libraries: `matplotlib`, `Path`, and `pandas`. You can then run the `basic-visualization.py` script to visualize the in- and out-degrees. To do so, run a command like the following:

```
$ ./basic-visualization.py <INDEGREE_RESULT_PATH> <OUTDEGREE_RESULT_PATH>
```
