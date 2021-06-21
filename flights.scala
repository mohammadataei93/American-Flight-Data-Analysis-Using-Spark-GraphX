

import data.Flight
import data.Flight.Times
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.dmg.pmml.False
import org.apache.spark.graphx.lib.ShortestPaths

import scala.util.hashing.MurmurHash3

object flights {

  var quiet = false

  def main(args: Array[String]) {

    val input = "C:/Users/amir/Desktop/flight/flight.csv"
    val conf = new SparkConf()
      .setAppName("GraphX")
      .setMaster("local[*]")
      .set("spark.app.id", "GraphX") // To silence Metrics warning.

    val sc = new SparkContext(conf)
    try{
      val flights = for {
        line <- sc.textFile(input)
        flight <- Flight.parse(line)
      } yield flight

      //create vertices out of airport codes for both origin and dest
      val airportCodes = flights.flatMap { f => Seq(f.origin, f.dest) }
      val airportVertices: RDD[(VertexId, String)] =
        airportCodes.distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))

      //create edges between origin -> dest pair and the set the edge attribute
      //to count of number of flights between given pair of origin and dest
      val flightEdges = flights.map(f =>
        ((stringHash(f.origin), stringHash(f.dest)), f.distance))
        .map {
          case ((src, dest), attr) => Edge(src, dest, attr)
        }

      val graph = Graph(airportVertices, flightEdges)
      if (!quiet) {
        println(graph.numVertices)
        println("\nNumber of airports in the graph:")
        println(graph.numEdges)
        println("\nNumber of flights in the graph:")
      }

      println("\nAirports with dist >1000")
      graph.triplets.filter(f => f.attr>1000).sortBy(_.attr, ascending=false).map(triplet =>
        "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").take(1).foreach(println)

      //what airport has the most in degrees or unique flights into it?
      val incoming: RDD[(VertexId, (PartitionID, String))] = graph.inDegrees.join(airportVertices)
      incoming.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey(ascending = false).take(10).foreach(println)
      println("\nAirports with most number of distinct incoming flights")


      val outgoing: RDD[(VertexId, (PartitionID, String))] = graph.outDegrees.join(airportVertices)

      outgoing.map {
        case (_, (count, airport)) => (count, airport)
      }.sortByKey(ascending = false).take(10).foreach(println)
      println("\nAirports with most number of distinct outgoing flights")

      val rank=graph.pageRank(0.0001).vertices.collect()
      rank.sortBy(_._2)(Ordering[Double].reverse).foreach(println)
      println("ranking ")

      val flightEdgesdelay = flights.map(f =>
        ((stringHash(f.origin), stringHash(f.dest)), f.times.arrDelay))
        .map {
          case ((src, dest), attr) => Edge(src, dest, attr)
        }
      val graphdelay = Graph(airportVertices, flightEdgesdelay)

      graphdelay.triplets.filter(f => f.srcId== -1737322424 ).sortBy(_.attr, ascending=false).map(triplet =>
        "Delay " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr + ".").collect().foreach(println)

      val v1: Long = 1883277119
      val v2: Long = -1737322424
      val result = ShortestPaths.run(graph,Seq(v2))
      result.vertices.filter(f => f._1==v1).map(f => f._2).collect().foreach(println)
      result.edges.filter(f => f.srcId == v1).take(1).foreach(println)



    } finally {
      sc.stop()
    }
  }

  def stringHash(str: String): Int = MurmurHash3.stringHash(str)
}
