package edu.columbia.eecs6893.btc.graph.analysis
import edu.columbia.eecs6893.btc.graph.analysis.models.FlatGraphOutputRecord
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Analyzer which produces a result for strongly connected components.
 */
// TODO: Fix types
class StronglyConnectedComponentAnalyzer(
                                          spark: SparkSession,
                                          numIters: Int = 10
                                        ) extends GraphAnalyzer[AddressGraphNode, AddressGraphEdge] {

  override def analyze(graph: Graph[AddressGraphNode, AddressGraphEdge]): DataFrame = {
    graph.cache()
    val scc = graph.stronglyConnectedComponents(numIters)
    val edgesRdd = scc.edges.map(e => FlatGraphOutputRecord("edge", e.srcId, e.dstId, e.attr))
    val verticesRdd = graph.mask(scc).vertices.map(v => FlatGraphOutputRecord("vertex", null, null, v._2))
    val edgesDf = spark.createDataFrame(edgesRdd).toDF("recordType", "srcId", "destId", "data")
    val verticesDf = spark.createDataFrame(verticesRdd).toDF("recordType", "srcId", "destId", "data")
    edgesDf.union(verticesDf)
  }
}
