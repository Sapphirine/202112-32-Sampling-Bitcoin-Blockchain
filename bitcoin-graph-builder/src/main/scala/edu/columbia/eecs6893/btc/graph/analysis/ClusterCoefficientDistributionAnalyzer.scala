package edu.columbia.eecs6893.btc.graph.analysis

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Compute the local cluster coefficients for all node degrees d. In short, the cluster coefficient for degree d
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

    // Valid edges
    val edgeEndpoints = graph.edges.map(e => (e.srcId, e.dstId))

    val coefficients = graph
      // Aggregate vertices to calculate neighborhood
      .aggregateMessages[Set[VertexId]](et => et.sendToSrc(Set(et.dstId)), _++_)
      // Explode all possible edges for neighborhood
      // Output: (RootVertex, Degree, Src, Dst)
      .flatMap(n => for (x <- n._2; y <- n._2) yield (n._1, n._2.size, x, y))
      // Take cartesian product with all valid edges
      .cartesian(edgeEndpoints)
      // Filter to valid edges within each neighborhood
      .filter(t => t._1._3 == t._2._1 && t._1._4 == t._2._2)
      // Simplify data for counting by vertex id ("root" id)
      // Output: (RootVertex, (Degree, 1))
      .map(t => (t._1._1, (t._1._2, 1)))
      .reduceByKey((a,b) => (a._1, a._2 + b._2))
      .map(x => (x._1, computeClusterCoefficient(x._2._1, x._2._2)))

    spark.createDataFrame(coefficients).toDF("degree", "cluster_coefficient")
  }

  private def possibleDirectedEdges(degree: Int): Int = degree * (degree-1)

  private def computeClusterCoefficient(degree: Int,
                                        totalConnections: Int): Double = {
    val possibleConnections = possibleDirectedEdges(degree)
    totalConnections / possibleConnections
  }
}
