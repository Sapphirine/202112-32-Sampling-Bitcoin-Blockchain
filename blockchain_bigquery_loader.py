# To run, you can submit to your cluster as using the following command:
#   gcloud dataproc jobs submit pyspark --cluster btc-project blockchain_bigquery_loader.py --region us-west4 --jars="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
# NOTE: The connector jar is important or else you won't be able to load from bigquery.
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,SparkSession

if __name__ == "__main__":
    # Configure spark
    # NOTE: The jars line is only helpful if you're running this in a pure python environment instead of pyspark.
    spark = SparkSession \
            .builder \
            .master('yarn') \
            .appName('RawBigQueryBtcDataLoader') \
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
            .getOrCreate()

    # Parse transactions
    raw_tx_df = spark.read.format('bigquery') \
            .option('table', 'bigquery-public-data:crypto_bitcoin.transactions') \
            .load()
    raw_tx_df.createOrReplaceTempView('transactions')

    # Take 1MM record sample
    transactions = spark.sql("""
    SELECT *
    FROM transactions
    WHERE block_timestamp_month >= '2021-10-01' AND block_timestamp_month <= '2021-10-20'
    ORDER BY lock_time ASC
    LIMIT 350000
    """)

    # Save data
    transactions.write.format('parquet').mode('overwrite').save("hdfs:///raw-btc-transactions")
