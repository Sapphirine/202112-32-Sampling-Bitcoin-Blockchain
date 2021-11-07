# Bitcoin Blockchain Explorer

For EECS6893, our group is building a Bitcoin Blockchain Explorer. We will investigate the raw data and structure of the BTC blockchain data.

# Fetching the Data

We use the `blockchain_bigquery_loader.py` file to fetch the raw blockchain transaction data. Since Google has already stored this data in an easy-to-use format in big query, this script downloads the data from there. It then places the data into an HDFS cluster for our heavier access/computational workloads.

# Graph Builder

The graph builder component takes the raw data from the previous step and turns it into a variety of graphs. This is a spark jar which can be compiled as follows:

```
$ sbt assembly
```
