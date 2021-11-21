package edu.columbia.eecs6893.btc.graph.analysis

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Compute out-degree distribtion for graph.
 */
class OutDegreeDistributionAnalyzer(spark: SparkSession) extends GraphAnalyzer[AddressGraphNode, AddressGraphEdge] {

  override def analyze(graph: Graph[AddressGraphNode, AddressGraphEdge]): DataFrame = {
    val distribution = graph.outDegrees.map(x => (x._2, 1)).reduceByKey(_+_)
    spark.createDataFrame(distribution).toDF("outdegree", "count")
  }
}
