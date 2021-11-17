package edu.columbia.eecs6893.btc.graph.analysis
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

/**
 * Analyzer which produces a result for strongly connected components.
 */
class StronglyConnectedComponentAnalyzer[VD,ED](numIters: Int = 10) extends GraphAnalyzer[VD,ED] {

  override def analyze(graph: Graph[VD, ED]): DataFrame = {
//    val scc = graph.stronglyConnectedComponents(numIters)
    // TODO:
    ???
  }
}
