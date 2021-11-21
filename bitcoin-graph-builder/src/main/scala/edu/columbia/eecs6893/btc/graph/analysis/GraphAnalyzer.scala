package edu.columbia.eecs6893.btc.graph.analysis

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

/**
 * Graph analysis trait which takes a graph and produces a result in the form of a dataframe.
 */
trait GraphAnalyzer[VD,ED] extends Serializable {

  /**
   * Analyze a graph and produce a result.
   *
   * @param graph Graph to analyze
   * @return Result
   */
  def analyze(graph: Graph[VD,ED]): DataFrame
}
