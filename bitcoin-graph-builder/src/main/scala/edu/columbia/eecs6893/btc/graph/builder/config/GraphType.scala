package edu.columbia.eecs6893.btc.graph.builder.config

object GraphType extends Enumeration {
  type GraphType = Value

  val ADDRESS_GRAPH = Value("AddressGraph")
  val TRANSACTION_GRAPH = Value("TransactionGraph")
  val HYPER_GRAPH = Value("HyperGraph")
}
