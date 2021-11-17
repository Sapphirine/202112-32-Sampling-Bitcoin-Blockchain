package edu.columbia.eecs6893.btc.graph

import edu.columbia.eecs6893.btc.graph.analysis.config.GraphAnalysisArguments
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser

/**
 * Generic graph analysis entry point.
 */
object GenericGraphAnalysis {
  private val APP_NAME = "GenericGraphAnalysis"

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)

    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .getOrCreate()

    // TODO:
  }

  def parseArgs(args: Array[String]): GraphAnalysisArguments = {
    val builder = OParser.builder[GraphAnalysisArguments]
    import builder._
    // TODO:
    val sequence = OParser.sequence(
      opt[Int]('t', "analysis-type")
        .text("Type of graph analysis to run")
    )
    OParser.parse(sequence, args, GraphAnalysisArguments()).orNull
  }
}
