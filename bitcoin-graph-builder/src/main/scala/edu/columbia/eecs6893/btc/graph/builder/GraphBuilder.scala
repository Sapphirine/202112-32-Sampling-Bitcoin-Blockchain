package edu.columbia.eecs6893.btc.graph.builder

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}

trait GraphBuilder[VD,ED] {
  /**
   * Construct graph components without building the graph.
   *
   * @param rawTransactionDataFrame Raw transaction data
   * @return Tuple containing (nodes, edges) as a DataFrame.
   */
  def constructGraphComponents(rawTransactionDataFrame: DataFrame): (DataFrame, DataFrame)

  /**
   * Build a graph from its raw components.
   *
   * @param nodes Nodes dataframe
   * @param edges Edges dataframe
   * @return Constructed graph
   */
  def buildGraph(nodes: DataFrame, edges: DataFrame): Graph[VD, ED]

  /**
   * Build a graph directly from the raw transaction dataframe.
   * <strong>NOTE:</strong> This graph will not be easy to save directly.
   *
   * @param rawTransactionDataFrame Raw transaction dataframe
   * @return Constructed graph
   */
  def buildGraph(rawTransactionDataFrame: DataFrame): Graph[VD, ED] = {
    val components = constructGraphComponents(rawTransactionDataFrame)
    buildGraph(components._1, components._2)
  }

  /**
   * Save a graph into its constituent components.
   * @param graph
   */
  def toDataFrames(graph: Graph[VD,ED])(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    throw new RuntimeException("Not implemented by default.")
  }
}
