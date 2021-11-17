package edu.columbia.eecs6893.btc.graph.builder
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.DataFrame

// TODO: Implement
class HyperGraphBuilder extends GraphBuilder[Unit, Unit] {

  override def buildGraph(nodes: DataFrame, edges: DataFrame): Graph[Unit, Unit] = ???

  override def constructGraphComponents(rawTransactionDataFrame: DataFrame): (DataFrame, DataFrame) = ???
}
