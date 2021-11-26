package edu.columbia.eecs6893.btc.graph.builder.config

import edu.columbia.eecs6893.btc.graph.builder.config.GraphType.{ADDRESS_GRAPH, GraphType}
import org.apache.spark.sql.SaveMode

/**
 * Command line arguments for graph builder.
 */
case class GraphBuilderArguments(rawDataLoadPath: String = "",
                                 graphOutputPath: String = "",
                                 overwrite: SaveMode = SaveMode.Overwrite,
                                 graphType: GraphType = ADDRESS_GRAPH,
                                 numPartitions: Option[Int] = None) {
}
