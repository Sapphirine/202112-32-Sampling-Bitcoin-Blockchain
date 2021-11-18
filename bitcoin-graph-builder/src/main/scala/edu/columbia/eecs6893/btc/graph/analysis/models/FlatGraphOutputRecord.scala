package edu.columbia.eecs6893.btc.graph.analysis.models

/**
 * Output record to store a generic graph dataframe after analysis
 */
case class FlatGraphOutputRecord(recordType: String,
                                 srcId: java.lang.Long,
                                 destId: java.lang.Long,
                                 dataType: Serializable)
