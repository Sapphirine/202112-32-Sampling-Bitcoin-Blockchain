package edu.columbia.eecs6893.btc.graph.builder

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

trait GraphBuilder[ED,VD] {
  def buildGraph(rawTransactionDataFrame: DataFrame): Graph[ED, VD]
}
