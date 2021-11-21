package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.GenericGraphBuilder.parseGraphType
import edu.columbia.eecs6893.btc.graph.analysis.{ClusterCoefficientDistributionAnalyzer, InDegreeDistributionAnalyzer, OutDegreeDistributionAnalyzer, StronglyConnectedComponentAnalyzer}
import edu.columbia.eecs6893.btc.graph.analysis.config.AnalysisType.{AnalysisType, CLUSTERING_COEFFICIENT, IN_DEGREE_DISTRIBUTION, OUT_DEGREE_DISTRIBUTION, STRONGLY_CONNECTED_COMPONENT}
import edu.columbia.eecs6893.btc.graph.analysis.config.GraphAnalysisArguments
import edu.columbia.eecs6893.btc.graph.builder.{AddressGraphBuilder, HyperGraphBuilder, TransactionGraphBuilder}
import edu.columbia.eecs6893.btc.graph.builder.config.GraphType.{ADDRESS_GRAPH, HYPER_GRAPH, TRANSACTION_GRAPH}
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser

/**
 * Generic graph analysis entry point.
 */
object GenericGraphAnalysis {
  private val APP_NAME = "GenericGraphAnalysis"

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)

    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .getOrCreate()

    // Load edges and nodes
    val edgesDf = spark.read.parquet(options.edgesPath)
    val nodesDf = spark.read.parquet(options.verticesPath)

    // Build graph
    val graphBuilder = options.graphType match {
      case ADDRESS_GRAPH => new AddressGraphBuilder
      // TODO: Fix types
//      case TRANSACTION_GRAPH => new TransactionGraphBuilder
//      case HYPER_GRAPH => new HyperGraphBuilder
      case _ => throw new RuntimeException("Unexpected graph type")
    }

    nodesDf.cache()
    edgesDf.cache()
    val graph = graphBuilder.buildGraph(nodesDf, edgesDf)

    // Run analysis
    val analyzer = options.analysisType match {
      // TODO: Fix types.
      case STRONGLY_CONNECTED_COMPONENT => new StronglyConnectedComponentAnalyzer(spark)
      case IN_DEGREE_DISTRIBUTION => new InDegreeDistributionAnalyzer(spark)
      case OUT_DEGREE_DISTRIBUTION => new OutDegreeDistributionAnalyzer(spark)
      case CLUSTERING_COEFFICIENT => new ClusterCoefficientDistributionAnalyzer(spark)
      case _ => throw new RuntimeException("Unexpected analysis type")
    }

    val results = analyzer.analyze(graph)

    // Save results
    results.write.mode(options.overwrite).parquet(options.outputPath)
  }

  private def parseArgs(args: Array[String]): GraphAnalysisArguments = {
    val builder = OParser.builder[GraphAnalysisArguments]
    import builder._
    val sequence = OParser.sequence(
      opt[String]('e', "edge-path")
        .text("Path to load graph edges dataframe")
        .action((x, c) => c.copy(edgesPath = x))
        .required(),
      opt[String]('v', "vertices-path")
        .text("Path to load graph vertices dataframe")
        .action((x, c) => c.copy(verticesPath = x))
        .required(),
      opt[Int]('g', "graph-type")
        .action((x, c) => c.copy(graphType = parseGraphType(x)))
        .validate(x => if (x >= 1 && x <= 3) success
                       else failure("Valid values are between 1 and 3. See -h for more"))
        .text("Choose graph type to builder. Values: 1 = AddressGraph, 2 = TransactionGraph, 3 = HyperGraph (default=1)"),
      opt[String]('o', "output-path")
        .text("Path to store analysis result dataframe")
        .action((x, c) => c.copy(outputPath = x))
        .required(),
      opt[Unit]('n', "no-overwrite")
        .text("Disable overwrite on output (overwrite enabled by default)")
        .action((_, c) => c.copy(overwrite = SaveMode.ErrorIfExists)),
      opt[Int]('t', "analysis-type")
        .text("Type of graph analysis to run. Values: 1 = strongly connected components (default=1)")
        .validate(x => if (x >= 1 && x <= 4) success
                       else failure("Valid values are between 1 and 4. See -h for more."))
        .action((x, c) => c.copy(analysisType = parseAnalysisType(x)))
    )
    OParser.parse(sequence, args, GraphAnalysisArguments()).orNull
  }

  def parseAnalysisType(analysisType: Int): AnalysisType = {
    analysisType match {
      case 1 => STRONGLY_CONNECTED_COMPONENT
      case 2 => IN_DEGREE_DISTRIBUTION
      case 3 => OUT_DEGREE_DISTRIBUTION
      case 4 => CLUSTERING_COEFFICIENT
      case _ => throw new RuntimeException("Unexpected analysis type")
    }
  }
}
