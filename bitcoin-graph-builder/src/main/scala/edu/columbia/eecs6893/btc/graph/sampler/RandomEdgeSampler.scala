package edu.columbia.eecs6893.btc.graph.sampler

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph

import scala.util.Random

/**
 * Sampler which performs a random edge sampling to retrieve the result graph.
 */
class RandomEdgeSampler(val sampleSize: Double = 0.15) extends GraphSampler[AddressGraphNode, AddressGraphEdge] {

  override def sampleGraph(graph: Graph[AddressGraphNode, AddressGraphEdge]): Graph[AddressGraphNode, AddressGraphEdge] = {

    // Random sample the edges
    val sampledGraph = graph
      .mapEdges(_ => {
        val randVal = new Random().nextDouble()
        randVal <= sampleSize
      })

    // Filter un-selected edges
    val selectedEdges = sampledGraph.edges.filter(_.attr)

    // Filter vertices with no edges
    val selectedVertices = sampledGraph.aggregateMessages[Boolean](
        ec => {
          ec.sendToSrc(ec.attr)
          ec.sendToDst(ec.attr)
        },
        (a, b) => a || b
      )
      .filter(_._2)

    // Build final graph
    val result = Graph(selectedVertices, selectedEdges)

    // Mask over the original
    graph.mask(result)
  }
}
