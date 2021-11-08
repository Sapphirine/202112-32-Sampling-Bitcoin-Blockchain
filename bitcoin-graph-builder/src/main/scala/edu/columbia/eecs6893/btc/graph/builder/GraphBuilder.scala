package edu.columbia.eecs6893.btc.graph.builder

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

trait GraphBuilder[VD,ED] {
  def buildGraph(rawTransactionDataFrame: DataFrame): Graph[VD,ED]
}
