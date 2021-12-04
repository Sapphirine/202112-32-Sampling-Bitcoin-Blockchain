package edu.columbia.eecs6893.btc.graph.sampler

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph

import scala.util.Random

/**
 * Sampler which produces a new graph by selecting nodes at random.
 * <strong>NOTE:</strong> Edges are preserved only between selected nodes.
 */
class RandomNodeSampler(val sampleRate: Double = 0.15) extends GraphSampler[AddressGraphNode, AddressGraphEdge] {

  override def sampleGraph(graph: Graph[AddressGraphNode, AddressGraphEdge]): Graph[AddressGraphNode, AddressGraphEdge] = {
    graph
      .filter[AddressGraphNode, AddressGraphEdge](
        preprocess = g => g,
        vpred = (_,_) => {
        val randVal = new Random().nextDouble()
        randVal <= sampleRate
      })
  }
}
