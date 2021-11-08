package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.builder.AddressGraphBuilder
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser

/**
 * TODO: We should make the general interface of a "Graph Builder" more generic
 *       and pass CLI args to change the processing.
 */
object GenericGraphBuilder {
  private final val APP_NAME = "AddressGraphBuilder"
  private final val TX_VIEW = "Transactions"

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)

    if (options == null) {
      return
    }

    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .getOrCreate()

    val rawTxDf = spark.read.parquet(options.rawDataLoadPath)
    rawTxDf.createOrReplaceTempView(TX_VIEW)

    rawTxDf.printSchema()

    // TODO: Need to fix this. Currently on "AddressGraph" supported
    val graphBuilder = new AddressGraphBuilder
    val graph = graphBuilder.buildGraph(rawTxDf)

    println("--- Graph nodes: " + graph.vertices.count())
    println("--- Graph edges: " + graph.edges.count())

    // TODO: Save graph components
  }

  private def parseArgs(args: Array[String]): GraphBuilderArguments = {
    val builder = OParser.builder[GraphBuilderArguments]
    import builder._
    val sequence = OParser.sequence(
      opt[String]('i', "input-path")
        .action((x, c) => c.copy(rawDataLoadPath = x))
        .text("Input path for the raw BTC blockchain transaction data (e.g., hdfs://<server>/<path>)")
        .required(),
      opt[String]('o', "output-path")
        .action((x, c) => c.copy(graphOutputPath = x))
        .text("Output path for specified graph (e.g., hdfs://<server>/<path>).")
        .required(),
      opt[Unit]('n', "no-overwrite")
        .action((_, c) => c.copy(overwrite = SaveMode.ErrorIfExists))
        .text("Disable overwrite on the output (overwrite enabled by default)")
    )
    OParser.parse(sequence, args, GraphBuilderArguments()).orNull
  }
}
