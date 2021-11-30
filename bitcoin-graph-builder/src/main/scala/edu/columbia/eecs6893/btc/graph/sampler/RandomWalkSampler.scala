package edu.columbia.eecs6893.btc.graph.sampler

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

import scala.util.Random
import scala.util.control.Breaks.break

class RandomWalkSampler extends GraphSampler[AddressGraphNode, AddressGraphEdge] {
  private final val SAMPLE_SIZE = 0.15
  private final val RESTART_PROB = 0.15

  override def sampleGraph(graph: Graph[AddressGraphNode, AddressGraphEdge]): Graph[AddressGraphNode, AddressGraphEdge] = {
    graph.cache()
    val random = new Random
    val numNodes = graph.vertices.count()
    val desiredNodes = numNodes * SAMPLE_SIZE
    var resultEdges = Array[(VertexId, VertexId)]()
    var startNode: VertexId = -1

    // We start over if we don't reach the desired end state
    while (resultEdges.length < desiredNodes) {
      val numSteps = (100 * desiredNodes).intValue()

      startNode = graph.pickRandomVertex()
      var currentNode = startNode
      resultEdges = Array()

      for (i <- new Range(0, numSteps, 1)) {
        val neighbors = graph.collectNeighborIds(EdgeDirection.Out).filter(x => x._1 == currentNode).map(x => x._1).collect()
        val nextNode = neighbors(random.nextInt(neighbors.length))
        resultEdges :+ (currentNode, nextNode)
        currentNode = nextNode
        // Should we return to start node?
        if (random.nextDouble() <= RESTART_PROB) {
          currentNode = startNode
        }
        if (resultEdges.length >= desiredNodes) {
          break
        }
      }
    }

    // Fetch the sampled subgraph
    graph.subgraph(epred = et => resultEdges.contains((et.srcId, et.dstId)))
  }
}
