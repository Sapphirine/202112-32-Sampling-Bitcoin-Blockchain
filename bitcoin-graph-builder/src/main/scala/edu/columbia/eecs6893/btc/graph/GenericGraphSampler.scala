package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.builder.{AddressGraphBuilder, GraphBuilder}
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import edu.columbia.eecs6893.btc.graph.sampler.{GraphSampler, RandomEdgeSampler, RandomNodeSampler}
import edu.columbia.eecs6893.btc.graph.sampler.config.GraphSamplerOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser

/**
 * Entry point for graph sampler.
 */
object GenericGraphSampler {
  private final val APP_NAME = "GenericGraphSampler"

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)

    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .getOrCreate()

    // Load edges and nodes
    val edgesPath = s"${options.graphInputPath}/edges"
    val nodesPath = s"${options.graphInputPath}/nodes"
    val edgesDf = spark.read.parquet(edgesPath)
    val nodesDf = spark.read.parquet(nodesPath)

    // Load graph
    val builder = options.builder
    val graph = builder.buildGraph(nodesDf, edgesDf)

    // Sample graph
    val sampled = options.samplerCreator(options.sampleRate).sampleGraph(graph)

    // Save results
    val (sampledNodes, sampledEdges) = builder.toDataFrames(sampled)(spark)

    sampledNodes.write.mode(options.overwrite).parquet(s"${options.graphOutputPath}/nodes")
    sampledEdges.write.mode(options.overwrite).parquet(s"${options.graphOutputPath}/edges")
  }

  private def parseArgs(args: Array[String]): GraphSamplerOptions = {
    val builder = OParser.builder[GraphSamplerOptions]
    import builder._
    val sequence = OParser.sequence(
      opt[Int]('s', "sampler-type")
        .text("Sampler type (1 = random edge, 2 = random node)")
        .validate(x => if (x >= 1 && x <= 2) success else failure("Valid sampler values are between 1 and 2"))
        .action((x, c) => c.copy(samplerCreator = parseSampler(x)))
        .required(),
      opt[String]('i', "input-path")
        .text("Path to input graph")
        .action((x, c) => c.copy(graphInputPath = x))
        .required(),
      opt[String]('o', "output-path")
        .text("Path to output graph")
        .action((x, c) => c.copy(graphOutputPath = x))
        .required(),
      opt[Boolean]('n', "no-overwrite")
        .text("Disable output overwriting")
        .action((_, c) => c.copy(overwrite = SaveMode.ErrorIfExists)),
      opt[Int]('g', "graph-type")
        .text("Graph type (1 = address graph)")
        .validate(x => if (x >= 1 && x <= 1) success else failure("Valid graph type values are between 1 and 1"))
        .action((x, c) => c.copy(builder = parseGraphType(x)))
        .required(),
      opt[Double]('r', "sample-rate")
        .text("Sample rate to keep a portion of sampled graph (valid values between (0, 1), default=0.15)")
        .validate(x => if (x > 0 && x < 1) success else failure("Sample rate must be greater than 0 and less than 1."))
        .action((x, c) => c.copy(sampleRate = x))
    )
    OParser.parse(sequence, args, GraphSamplerOptions()).orNull
  }

  private def parseSampler(value: Int)(sampleRate: Double): GraphSampler[AddressGraphNode, AddressGraphEdge] = {
    value match {
      case 1 => new RandomEdgeSampler(sampleRate)
      case 2 => new RandomNodeSampler(sampleRate)
      case _ => throw new RuntimeException("Invalid sampler")
    }
  }

  private def parseGraphType(value: Int): GraphBuilder[AddressGraphNode, AddressGraphEdge] = {
    value match {
      case 1 => new AddressGraphBuilder
      case _ => throw new RuntimeException("Invalid graph type")
    }
  }
}
