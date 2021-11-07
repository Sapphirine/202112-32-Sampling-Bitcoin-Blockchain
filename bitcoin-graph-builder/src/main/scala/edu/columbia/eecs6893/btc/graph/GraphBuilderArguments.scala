package edu.columbia.eecs6893.btc.graph

import org.apache.spark.sql.SaveMode

/**
 * Command line arguments for graph builder.
 */
case class GraphBuilderArguments(val rawDataLoadPath: String = "",
                                 val graphOutputPath: String = "",
                                 val overwrite: SaveMode = SaveMode.Overwrite)
