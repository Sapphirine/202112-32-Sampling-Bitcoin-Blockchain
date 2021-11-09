package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.builder.AddressGraphBuilder
import org.apache.spark.sql.SparkSession

/**
 * Sandbox to play with some of the graphs.
 */
object Sandbox {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Sandbox")
      .getOrCreate()

    // Load edges and nodes
    val edgesDf = spark.read.parquet("test-data/address-graph/edges")
    val nodesDf = spark.read.parquet("test-data/address-graph/nodes")

    val graphBuilder = new AddressGraphBuilder
    val graph = graphBuilder.buildGraph(nodesDf, edgesDf)

    println("Num edges: " + graph.numEdges)
    println("Num vertices: " + graph.numVertices)
  }
}
