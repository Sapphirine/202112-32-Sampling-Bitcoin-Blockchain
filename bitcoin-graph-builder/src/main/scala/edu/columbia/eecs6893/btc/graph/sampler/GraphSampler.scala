package edu.columbia.eecs6893.btc.graph.sampler

import org.apache.spark.graphx.Graph

/**
 * Generic graph sampling algorithm
 */
trait GraphSampler[VD,ED] extends Serializable {

  /**
   * Sample a graph and produce a new graph.
   *
   * @param graph Graph to sample
   * @return Sampled graph
   */
  def sampleGraph(graph: Graph[VD,ED]): Graph[VD,ED]
}
