package edu.columbia.eecs6893.btc.graph.builder
import edu.columbia.eecs6893.btc.graph.builder.models.{AddressGraphEdge, AddressGraphNode}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * Address graph builder which constructs an address graph.
 * <ul>
 *   <li><strong>Nodes:</strong> Addresses</li>
 *   <li><strong>Edges:</strong> Related transactions</li>
 * </ul>
 */
class AddressGraphBuilder extends GraphBuilder[AddressGraphNode, AddressGraphEdge] {
  private final val RAW_TRANSACTION_VIEW = "Transactions"
  private final val GRAPH_VIEW = "Graph"
  private final val NODE_VIEW = "Nodes"
  private final val GRAPH_DATA_QUERY =
    s"""SELECT hash, block_hash, block_number, input_value, output_value, input_address, output_address
       FROM ${RAW_TRANSACTION_VIEW}
       LATERAL VIEW EXPLODE(inputs) exploded_inputs AS input
       LATERAL VIEW EXPLODE(outputs) exploded_outputs AS output
       LATERAL VIEW EXPLODE(input.addresses) input_addresses AS input_address
       LATERAL VIEW EXPLODE(output.addresses) output_addresses AS output_address
       """
  private final val NODE_QUERY =
    s"""(SELECT input_address AS address
         FROM ${GRAPH_VIEW})
        UNION ALL
        (SELECT output_address AS address
         FROM ${GRAPH_VIEW})
       """
  private final val EDGE_QUERY =
    s"""
       SELECT hash, block_hash, block_number, input_value, output_value,
              n1.vertexId AS input_vertex, n2.vertexId AS output_vertex
       FROM ${GRAPH_VIEW} g
       INNER JOIN ${NODE_VIEW} n1 ON n1.address = g.input_address
       INNER JOIN ${NODE_VIEW} n2 ON n2.address = g.output_address
       """

  override def constructGraphComponents(rawTransactionDataFrame: DataFrame): (DataFrame, DataFrame) = {
    val spark = rawTransactionDataFrame.sparkSession

    // Make transaction data available in spark sql
    rawTransactionDataFrame.createOrReplaceTempView(RAW_TRANSACTION_VIEW)

    // Extract a dataframe that has denormalized data for graph creation
    val graphDf = spark.sql(GRAPH_DATA_QUERY)
    graphDf.createOrReplaceTempView(GRAPH_VIEW)

    // Fetch nodes and edges
    val nodesDf = getNodes(spark)
    val edgesDf = getEdges(spark)

    (nodesDf, edgesDf)
  }

  override def buildGraph(nodesDf: DataFrame, edgesDf: DataFrame): Graph[AddressGraphNode, AddressGraphEdge] = {
    // Convert to graph representations
    val nodesRdd = getNodesRdd(nodesDf)
    val edgesRdd = getEdgesRdd(edgesDf)

    Graph(nodesRdd, edgesRdd)
  }

  override def toDataFrames(graph: Graph[AddressGraphNode, AddressGraphEdge])(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    val nodesDf = sparkSession
      .createDataFrame(graph.vertices.map(x => (x._1, x._2.addressHash)))
      .toDF("vertexId", "address")
    val edgesDf = sparkSession.
      createDataFrame(graph.edges.map(x => (x.srcId, x.dstId, x.attr.transactionHash)))
      .toDF("output_vertex", "input_vertex", "hash")
    (nodesDf, edgesDf)
  }

  private def getNodes(spark: SparkSession): DataFrame = {
    // Combine all inputs and outputs to make a list of nodes
    val result = spark.sql(NODE_QUERY).distinct().withColumn("vertexId", monotonically_increasing_id())
    result.createOrReplaceTempView(NODE_VIEW)
    result
  }

  private def getEdges(spark: SparkSession): DataFrame = {
    // Fetch data with vertex id's
    spark.sql(EDGE_QUERY)
  }

  private def getNodesRdd(nodeDf: DataFrame): RDD[(VertexId, AddressGraphNode)] = {
    nodeDf.select(functions.col("vertexId"), functions.col("address")).rdd
      .map(row => (row.getLong(0), AddressGraphNode(row.getString(1))))
  }

  private def getEdgesRdd(edgeDf: DataFrame): RDD[Edge[AddressGraphEdge]] = {
    // TODO: Incorporate other attributes into node
    edgeDf.select(functions.col("output_vertex"), functions.col("input_vertex"), functions.col("hash")).rdd
      .map(row => Edge(row.getLong(0), row.getLong(1), AddressGraphEdge(row.getString(2))))
  }
}
