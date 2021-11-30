package edu.columbia.eecs6893.btc.graph.sampler

import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

class RandomWalkSampler extends GraphSampler[AddressGraphNode, AddressGraphEdge] {
  private final val SAMPLE_SIZE = 0.15
  private final val RESTART_PROB = 0.15

  override def sampleGraph(graph: Graph[AddressGraphNode, AddressGraphEdge]): Graph[AddressGraphNode, AddressGraphEdge] = {
    graph.cache()
    val random = new Random
    val numNodes = graph.vertices.count()
    val desiredNodes = numNodes * SAMPLE_SIZE
    var resultEdges = Set[(VertexId, VertexId)]()
    var startNode: VertexId = graph.pickRandomVertex()
    var newStartNode = false

    // We start over if we don't reach the desired end state
    while (resultEdges.size < desiredNodes) {
      val numSteps = (100 * desiredNodes).intValue()
      var stepsTaken = 0

      if (newStartNode) {
        newStartNode = false
        startNode = graph.pickRandomVertex()
      }

      var currentNode = startNode

      breakable {
        for (i <- new Range(0, numSteps, 1)) {
          stepsTaken += 1
          val neighbors = graph.edges.filter(x => x.srcId == currentNode).map(x => x.dstId).collect()
          if (neighbors.length == 0) {
            println(s"Found a sink, starting over. (Current result size: ${resultEdges.size})")
            break
          }
          val nextNode = neighbors(random.nextInt(neighbors.length))
          resultEdges = resultEdges ++ Set((currentNode, nextNode))
          currentNode = nextNode
          // Should we return to start node?
          if (random.nextDouble() <= RESTART_PROB) {
            currentNode = startNode
          }
          if (resultEdges.size >= desiredNodes) {
            break
          }
          if (i % 100 == 0) {
            println(s"Iteration ${i}/${numSteps}. Array is ${resultEdges.size} full (desired: ${desiredNodes}.")
          }
        }
      }

      if (stepsTaken >= numSteps && resultEdges.size < desiredNodes) {
        // Maybe start node is a sync? Try a new start node on the next iteration.
        println(s"Did not find expected number of nodes within ${numSteps}. Selecting a new start node.")
        newStartNode = true
      }
    }

    val allNodes = resultEdges.flatMap(x => Seq(x._1, x._2))

    // Fetch the sampled subgraph
    graph.subgraph(epred = et => resultEdges.contains((et.srcId, et.dstId)), vpred = (vt, _) => allNodes.contains(vt))
  }
}
