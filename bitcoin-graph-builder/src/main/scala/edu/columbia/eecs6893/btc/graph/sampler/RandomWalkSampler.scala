package edu.columbia.eecs6893.btc.graph.sampler

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph

class RandomWalkSampler extends GraphSampler[AddressGraphNode, AddressGraphEdge] {

  override def sampleGraph(graph: Graph[AddressGraphNode, AddressGraphEdge]): Graph[AddressGraphNode, AddressGraphEdge] = {
    // TODO: Implement
    graph
  }
}
