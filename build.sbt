lazy val root = (project in file("."))
    .aggregate(graphBuilder)

lazy val graphBuilder = (project in file("./bitcoin-graph-builder"))
