package edu.columbia.eecs6893.btc.graph.analysis.config

import edu.columbia.eecs6893.btc.graph.analysis.config.AnalysisType.{AnalysisType, STRONGLY_CONNECTED_COMPONENT}
import edu.columbia.eecs6893.btc.graph.builder.config.GraphType.{ADDRESS_GRAPH, GraphType}
import org.apache.spark.sql.SaveMode

/**
 * Application arguments to run graph analysis.
 */
case class GraphAnalysisArguments(edgesPath: String = "",
                                  verticesPath: String = "",
                                  outputPath: String = "",
                                  analysisType: AnalysisType = STRONGLY_CONNECTED_COMPONENT,
                                  graphType: GraphType = ADDRESS_GRAPH,
                                  overwrite: SaveMode = SaveMode.Overwrite)
