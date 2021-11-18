package edu.columbia.eecs6893.btc.graph.analysis

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Analyzer which provides an in-degree distribution.
 */
class InDegreeDistributionAnalyzer(spark: SparkSession) extends GraphAnalyzer[AddressGraphNode, AddressGraphEdge] {

  override def analyze(graph: Graph[AddressGraphNode, AddressGraphEdge]): DataFrame = {
    val distribution = graph.inDegrees.map(x => (x._2, 1)).reduceByKey(_+_)
    spark.createDataFrame(distribution).toDF("indegree", "count")
  }
}
