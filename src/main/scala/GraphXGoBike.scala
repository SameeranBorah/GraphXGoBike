import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphXGoBike {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("GraphX GoBike")
      .setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val df = sqlContext.read.format("csv").option("inferSchema", "true").option("header","true").load("src/main/data/newdata.csv")
    //df.collect().foreach(println);
    df.printSchema()
    val justStations = df
      .selectExpr("float(start_station_id) as station_id", "start_station_name")
      .distinct()
    justStations.printSchema()
    val stations = df
      .select("start_station_id", "end_station_id")
      .rdd
      .distinct() // helps filter out duplicate trips
      .flatMap(x => Iterable(x(0).asInstanceOf[Number].longValue, x(1).asInstanceOf[Number].longValue)) // helps us maintain types
      .distinct()
      .toDF() // return to a DF to make merging + joining easier
    val stationVertices: RDD[(VertexId, String)] = stations
      .join(justStations, stations("value") === justStations("station_id"))
      .select("station_id", "start_station_name")
      .rdd
      .map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String])) // maintain type information
    stationVertices.collect().foreach(println)
    stationVertices.take(1)// this is just a stationVertex at this point
    val stationEdges:RDD[Edge[Long]] = df
      .select("start_station_id", "end_station_id")
      .rdd
      .map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))
    stationEdges.collect().foreach(println)
    val defaultStation = ("Missing Station")
    val stationGraph = Graph(stationVertices, stationEdges, defaultStation)
    stationGraph.cache()
    println("Total Number of Stations: " + stationGraph.numVertices)
    println("Total Number of Trips: " + stationGraph.numEdges)
    // sanity check
    println("Total Number of Trips in Original Data: " + df.count)
    val ranks = stationGraph.pageRank(10).vertices
    ranks
      .join(stationVertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .take(10) // get the top 10
      .foreach(x => println(x._2._2))
    stationGraph
      .groupEdges((edge1, edge2) => edge1 + edge2)
      .triplets
      .sortBy(_.attr, ascending=false)
      .map(triplet =>
        "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
      .take(10)
      .foreach(println)
    stationGraph
      .inDegrees // computes in Degrees
      .join(stationVertices)
      .sortBy(_._2._1, ascending=false)
      .take(10)
      .foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))
    stationGraph
      .outDegrees // out degrees
      .join(stationVertices)
      .sortBy(_._2._1, ascending=false)
      .take(10)
      .foreach(x => println(x._2._2 + " has " + x._2._1 + " out degrees."))
    stationGraph
      .inDegrees
      .join(stationGraph.outDegrees) // join with out Degrees
      .join(stationVertices) // join with your other stations
      .map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)) // ratio of in to out
      .sortBy(_._1, ascending=false)
      .take(5)
      .foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))
    stationGraph
      .outDegrees
      .join(stationGraph.inDegrees) // join with out Degrees
      .join(stationVertices) // join with your other stations
      .map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)) // ratio of in to out
      .sortBy(_._1)
      .take(5)
      .foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))

    sc.stop()
  }
}
