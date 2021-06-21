package course3.module

import data.Flight
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3

object GraphingFlights {

  var quiet = false

  def main(args: Array[String]) {
    val input = "/flight.csv"
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

      ///////////////////////////////////////////////////////////////////
                your code
      ///////////////////////////////////////////////////////////////////
    } finally {
      sc.stop()
    }
  }

  def stringHash(str: String): Int = MurmurHash3.stringHash(str)
}