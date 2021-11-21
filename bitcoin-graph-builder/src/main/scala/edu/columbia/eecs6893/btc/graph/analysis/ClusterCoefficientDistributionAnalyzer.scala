package edu.columbia.eecs6893.btc.graph.analysis

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.EdgeDirection.{In, Out}
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Compute the cluster coefficients for all node degrees d. In short, the cluster coefficient for degree d
 * is represented by the average cluster coefficient for all nodes of degree d. The cluster coefficient represents
 * the fraction of actual number of edges that exist in the graph from all possible edges (e.g., d(d-1) possible for
 * directed graphs).
 *
 * For a more detailed definition, see definition in these papers:
 * https://snap.stanford.edu/class/cs224w-readings/watts98smallworld.pdf
 * https://cs.stanford.edu/people/jure/pubs/sampling-kdd06.pdf (metric S9)
 *
 * or wikipedia:
 * https://en.wikipedia.org/wiki/Clustering_coefficient
 */
class ClusterCoefficientDistributionAnalyzer(spark: SparkSession) extends GraphAnalyzer[AddressGraphNode, AddressGraphEdge] {

  override def analyze(graph: Graph[AddressGraphNode, AddressGraphEdge]): DataFrame = {
    // Basic approach:
    //   1. Label each vertex with its degree
    //   2. For each neighbor and all k of its neighbors, count edges between those k nodes
    //   3. Divide count from all possible k(k-1) (directed graph) to compute cluster coefficient
    //   4. Group by degree and average clustering coefficient
    val inNeighbors = graph.collectNeighbors(In)
    val outNeighbors = graph.collectNeighbors(Out)
    val allNeighbors = inNeighbors.fullOuterJoin(outNeighbors)
    val emptyArray: Array[(VertexId, AddressGraphNode)] = Array()
    val coefficients = allNeighbors
      // Combine all neighbors to a single array
      .map(x => (x._1, x._2._1.getOrElse(emptyArray) ++ x._2._2.getOrElse(emptyArray)))
      // Compute total degree of each vertex
      .map(x => (x._1, x._2, x._2.count(x => true)))
      // New tuple: (vertexId, (degree, eligible nodes list))
      .flatMap(x => x._2.map(y => (y._1, (x._3, x._2.map(_._1) ++ Array(x._1)))))
      .map(x => x)
      // Join exploded vertices with all their neighbors
      .join(allNeighbors)
      // Compute the number of total eligible edges. Key=degree, value=Number of neighbors
      .map(x => (x._2._1._1, (
        countNumberOfNeighbors(
          x._2._2._1.getOrElse(emptyArray) ++ x._2._2._2.getOrElse(emptyArray),
          x._2._1._2
        ), 1)))
      // Sum total number of neighbors and total number of nodes with degree d (group by degree)
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      // Average number of neighbors for degree and divide by total possible
      .map(x => (x._1, computeClusterCoefficient(x._1, x._2)))
    spark.createDataFrame(coefficients).toDF("degree", "cluster_coefficient")
  }

  private def possibleDirectedEdges(degree: Int): Int = degree * (degree-1)

  private def countNumberOfNeighbors(neighbors: Array[(VertexId, AddressGraphNode)],
                                     validNeighbors: Array[Long]): Long = {
    // Take all of our neighbors from "id" and then for each of them check how many edges they have connected to each
    // other in total.
    neighbors
      .map(_._1)
      .count(validNeighbors.contains(_))
  }

  private def computeClusterCoefficient(degree: Int,
                                        data: (Long, Int)): Double = {
    val totalConnections = data._1
    val totalNodes = data._2
    val possibleConnections = possibleDirectedEdges(degree)
    val averageConnections = totalConnections / totalNodes
    averageConnections / possibleConnections
  }
}
