package edu.columbia.eecs6893.btc.graph.builder
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Address graph builder which constructs an address graph.
 * <ul>
 *   <li><strong>Nodes:</strong> Addresses</li>
 *   <li><strong>Edges:</strong> Related transactions</li>
 * </ul>
 */
class AddressGraphBuilder extends GraphBuilder[AddressGraphEdge, AddressGraphNode] {
  private final val TX_VIEW = "Transactions"
  private final val INPUT_NODE_QUERY =
    s"""SELECT EXPLODE(col.addresses) AS address
        FROM (SELECT EXPLODE(inputs)
              FROM ${TX_VIEW})
       """
  private final val OUTPUT_NODE_QUERY =
    s"""SELECT EXPLODE(col.addresses) AS address
        FROM (SELECT EXPLODE(outputs)
              FROM ${TX_VIEW})
       """

  override def buildGraph(rawTransactionDataFrame: DataFrame): Graph[AddressGraphEdge, AddressGraphNode] = {
    val spark = rawTransactionDataFrame.sparkSession

    // Make transaction data available in spark sql
    rawTransactionDataFrame.createOrReplaceTempView(TX_VIEW)

    // Fetch nodes and edges
    val nodes = getNodes(spark)
    val edges = getEdges(spark)

    // Convert to graph representations
    // TODO: Nodes need to have an id and edges need to use these id's

    // TODO: Not finished.
    null
  }

  private def getNodes(spark: SparkSession): RDD[AddressGraphNode] = {
    // Gather a list of input and output addresses
    val inputNodeDf = spark.sql(INPUT_NODE_QUERY)
    val outputNodeDf = spark.sql(OUTPUT_NODE_QUERY)

    // Combine all inputs and outputs to make a list of nodes
    val nodesDf = inputNodeDf.union(outputNodeDf).distinct()

    // Convert nodes to graph representation
    nodesDf.rdd.map(row => AddressGraphNode(row.getString(0)))
  }

  private def getEdges(sparkSession: SparkSession): RDD[AddressGraphEdge] = {
    // TODO
    null
  }
}
