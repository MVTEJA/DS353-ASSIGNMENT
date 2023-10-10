import org.apache.spark.graphx.{Graph, GraphLoader}

// Load the graph from a space-separated dataset
val graph = GraphLoader.edgeListFile(sc,"./data/graphx/higgs-retweet_network.edgelist", false)

// Run PageRank algorithm
val ranks = graph.pageRank(0.0001).vertices

// Print the top ten nodes based on PageRank score
println("Top 20 nodes by PageRank:")
ranks.top(20)(Ordering.by(_._2)).foreach { case (id, rank) =>
  println(s"Node $id has rank: $rank")
}

// Run connected components algorithm
val connectedComponents = graph.connectedComponents().vertices

// Calculate the size of each connected component
val componentSizes = connectedComponents
  .map { case (id, componentId) => (componentId, 1) }
  .reduceByKey(_ + _)

// Print the top 20 sized connected components
println("\nTop 20 sized connected components:")
componentSizes.takeOrdered(20)(Ordering[Int].reverse.on { case (componentId, size) => size })
  .foreach { case (componentId, size) =>
    println(s"Component $componentId has size: $size")
  }


// Print the top 20 nodes with the highest triangle counts
println("\nTop 20 nodes by triangle count:")
val joinedGraph = graph.outerJoinVertices(graph.degrees) {
  (vid, _, degOpt) => degOpt.getOrElse(0)
}

// Compute the number of triangles for each vertex
val triangleCounts = joinedGraph.aggregateMessages[Long](
  triplet => {
    triplet.sendToDst(1L)
    triplet.sendToSrc(1L)
  },
  (a, b) => a + b
)

// Print the top 20 nodes with the highest triangle counts
val top20Nodes = triangleCounts.map { case (id, count) => (id, count / 2) } // Divide by 2 as each triangle is counted 3 times
  .sortBy { case (id, count) => count }
  .takeOrdered(20)(Ordering[Long].reverse.on { case (id, count) => count })

top20Nodes.foreach { case (id, count) =>
  println(s"Node $id has $count triangles")
}

// Stop the Spark context
sc.stop()

