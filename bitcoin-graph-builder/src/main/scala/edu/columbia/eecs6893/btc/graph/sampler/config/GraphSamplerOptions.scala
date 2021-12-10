package edu.columbia.eecs6893.btc.graph.sampler.config

import edu.columbia.eecs6893.btc.graph.builder.GraphBuilder
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import edu.columbia.eecs6893.btc.graph.sampler.GraphSampler
import org.apache.spark.sql.SaveMode

/**
 * Options for the graph sampler.
 */
case class GraphSamplerOptions(graphInputPath: String = "",
                               graphOutputPath: String = "",
                               overwrite: SaveMode = SaveMode.Overwrite,
                               builder: GraphBuilder[AddressGraphNode, AddressGraphEdge] = null,
                               samplerCreator: Double => GraphSampler[AddressGraphNode, AddressGraphEdge] = null,
                               sampleRate: Double = 0.15)
