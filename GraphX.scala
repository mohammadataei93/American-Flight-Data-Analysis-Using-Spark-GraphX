package main

import  data.Flight
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.PartitionStrategy._
import scala.util.hashing.MurmurHash3
import org.apache.log4j._
import scala.reflect.ClassTag
import scala.collection.{mutable, Map,immutable}


object GraphX {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  var quiet = false
  def main(args: Array[String]) {
    val input = "../2008.csv"
    val conf = new SparkConf()
      .setAppName("graphx")
      .setMaster("local[*]")
      .set("spark.app.id", "GraphX") 
        val sc = new SparkContext(conf)
        val flights = for {
        line <- sc.textFile(input) 
        flight <-Flight.parse(line)
          } yield flight
          
          
////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
          
       
   val Vertices = flights.flatMap { f => Seq(f. origin, f.origin) }
          .distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))          

   val Edges = flights.map(e=> Edge(MurmurHash3.stringHash(e.origin)
       .toLong,MurmurHash3.stringHash(e.dest).toLong, e.distance.toInt))
   
   val graph = Graph(Vertices,Edges,"")

   
///////////////////////////////////////////////////////////////////////////////////////////////////////////

   // a)
   
   val graphA=graph.partitionBy(CanonicalRandomVertexCut).groupEdges( (EDGE , EDGE1) => EDGE)   
   val NumberOfAirports= graphA.numVertices
   val NumberOfRoutes = graphA.numEdges
   
   println("   ")
   println("number of airports = " + NumberOfAirports)
   println("   ")
   println("number of point to point routes = " + NumberOfRoutes)  
   
  
//////////////////////////////////////////////////////////////////////////////////////////////////////////
   
   //  b)
   
   val Over1000Dist=  graphA.edges.filter{case(Edge(org, dest ,dist))=> dist > 1000}.count()
   
   println("number of over 1000 miles routes = " + Over1000Dist)
   println("   ")
   
   graphA.triplets.sortBy(_.attr, ascending=false)
   .map(t =>"Longest Distance is  from " + t.srcAttr + " to " + t.dstAttr + " wiht " + t.attr.toString + " Miles.")
   .collect.take(1).foreach(println)
 
   
////////////////////////////////////////////////////////////////////////////////////////////////////////////
   
   //c,d
   
   
   val graphB=graph.groupEdges( (EDGE , EDGE1) => EDGE)  
   
   val in: RDD[(VertexId, (PartitionID, String))] = graphB.inDegrees.join(Vertices)
   in.map { case (_, (count, airport)) => (count, airport)}
   .sortByKey(ascending = false).map{case(count,airport) => s" $airport with $count input routes "}
   .take(10).foreach(println)
   println("   ")
      
   val out: RDD[(VertexId, (PartitionID, String))] = graphB.outDegrees.join(Vertices)
   out.map { case (_, (count, airport)) => (count, airport)}
   .sortByKey(ascending = false).map{case(count,airport) => s" $airport with $count output routes "}
   .take(10).foreach(println)
   println("   ")

   val hub: RDD[(VertexId, (PartitionID, String))] = graphB.degrees.join(Vertices)
   hub.map { case (_, (count, airport)) => (count, airport)}
   .sortByKey(ascending = false).map{case(count,airport) => s" $airport with total $count routes "}
   .take(10).foreach(println)
      
      
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
    // e)
      
      val ranks = graph.pageRank(0.001).vertices
      val temp= ranks.join(Vertices)
      val temp2 = temp.map(t => t._2).sortBy(_._1, false)
      .map{case(point , airport) => f" $airport : $point%.2f %% "}
      .take(10).foreach(println)
      

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
    // f)
      
          
    val jfk = flights.filter(_.origin == "JFK")    
    val delay = jfk.
    map(flight => (flight.carrierDelay,flight.weatherDelay,flight.nasDelay,flight.lateAircraftDelay,
        flight.securityDelay,(flight.dest))).distinct
    val delays= delay.map(x => (x._1 + x._2 + x._3 + x._4 + x._5 , x._6))
    delays.sortBy(x=>x._1, false)
    .map{ case (x,y) => f" from JFK to $y with $x min delay"}
    .take(5).foreach(println)
      
   
   
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      
    //g
    

          
    val graphC=graph.groupEdges( (EDGE , EDGE1) => EDGE)
    
    val v1: Long = MurmurHash3.stringHash("ATL")
    val v2: Long = MurmurHash3.stringHash("LAS")
    
  def Distance[VD](g:Graph[String,Int], origin:VertexId) = {
  var g2 = g.mapVertices(
    (vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue,
                 List[VertexId]()))

  for (i <- 1L to g.vertices.count-1) {
    val currentVertexId =
      g2.vertices.filter(!_._2._1)
        .fold((0L,(false,Double.MaxValue,List[VertexId]())))((a,b) =>
           if (a._2._2 < b._2._2) a else b)
        ._1

    val newDistances = g2.aggregateMessages[(Double,List[VertexId])](
        ctx => if (ctx.srcId == currentVertexId)
                 ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
                                ctx.srcAttr._3 :+ ctx.srcId)),
        (a,b) => if (a._1 < b._1) a else b)

    g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
      val newSumVal =
        newSum.getOrElse((Double.MaxValue,List[VertexId]()))
      (vd._1 || vid == currentVertexId,
       math.min(vd._2, newSumVal._1),
       if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)})
  }

  g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
    (vd, dist.getOrElse((false,Double.MaxValue,List[VertexId]()))    
             .productIterator.toList.tail))
}
          
   val x = Distance(graphC,v1).vertices.map(_._2).filter({case(vId, _) => vId =="LAS"})
   
   val dest= x.take(1).map{case(dest ,dis) => dest}
   val list1 = x.take(1).map{case(dest ,dis) => dis}.flatten.toList
   println(" ")
   val dis = println("Shortest Distence is : " + list1(0))
   val list2 = list1(1).asInstanceOf[List[Long]]
   println("All Stations:")
   for (vall <- list2){
     println(Vertices.lookup(vall))
   }
   println(Vertices.lookup(v2))

      
        
      
     
        
  }
}