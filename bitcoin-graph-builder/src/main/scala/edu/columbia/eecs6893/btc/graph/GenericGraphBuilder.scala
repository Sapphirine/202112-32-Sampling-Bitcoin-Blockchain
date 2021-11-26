package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.builder.{AddressGraphBuilder, HyperGraphBuilder, TransactionGraphBuilder}
import edu.columbia.eecs6893.btc.graph.builder.config.GraphBuilderArguments
import edu.columbia.eecs6893.btc.graph.builder.config.GraphType.{ADDRESS_GRAPH, GraphType, HYPER_GRAPH, TRANSACTION_GRAPH}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser

/**
 * Generic graph builder entry point. It provides command line options to build a graph from raw bitcoin blockchain
 * data from a raw dump provided by Google's BigQuery table.
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

    val graphBuilder = options.graphType match {
      case ADDRESS_GRAPH => new AddressGraphBuilder
      case TRANSACTION_GRAPH => new TransactionGraphBuilder
      case HYPER_GRAPH => new HyperGraphBuilder
      case _ => throw new RuntimeException("Unknown graph type requested to build")
    }
    val (nodes, edges) = graphBuilder.constructGraphComponents(rawTxDf)

    println("--- Graph nodes: " + nodes.count())
    println("--- Graph edges: " + edges.count())

    // Specify number of partitions if desired.
    val (edgesOut, nodesOut) = options.numPartitions match {
      case None => (edges, nodes)
      case Some(x) => (edges.coalesce(x), nodes.coalesce(x))
    }

    // Save graph components
    nodesOut.write.mode(options.overwrite).parquet(s"${options.graphOutputPath}/nodes")
    edgesOut.write.mode(options.overwrite).parquet(s"${options.graphOutputPath}/edges")
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
        .text("Disable overwrite on the output (overwrite enabled by default)"),
      opt[Int]('t', "graph-type")
        .action((x, c) => c.copy(graphType = parseGraphType(x)))
        .validate(x => if (x >= 1 && x <= 3) success
                       else failure("Valid values are between 1 and 3. See -h for more"))
        .text("Choose graph type to builder. Values: 1 = AddressGraph, 2 = TransactionGraph, 3 = HyperGraph (default=1)"),
      opt[Int]('p', "num-partitions")
        .action((x, c) => c.copy(numPartitions = Some(x)))
        .validate(x => if (x > 0) success else failure("Number of partitions must be positive"))
        .text("Specify the number of output partitions for the results.")
    )
    OParser.parse(sequence, args, GraphBuilderArguments()).orNull
  }

   def parseGraphType(value: Int): GraphType = {
    value match {
      case 1 => ADDRESS_GRAPH
      case 2 => TRANSACTION_GRAPH
      case 3 => HYPER_GRAPH
      case _ => throw new RuntimeException("Bad argument, cannot parse graph type")
    }
  }
}
