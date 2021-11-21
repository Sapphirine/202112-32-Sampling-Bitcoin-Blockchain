package edu.columbia.eecs6893.btc.graph.analysis.config

/**
 * Analysis type enum.
 */
object AnalysisType extends Enumeration {
  type AnalysisType = Value

  final val STRONGLY_CONNECTED_COMPONENT = Value("StronglyConnectedComponent")
  final val IN_DEGREE_DISTRIBUTION = Value("InDegreeDistribution")
  final val OUT_DEGREE_DISTRIBUTION = Value("OutDegreeDistribution")
  final val CLUSTERING_COEFFICIENT = Value("ClusteringCoefficient")
}
