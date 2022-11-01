package manohar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show

object friendsByAgeRdd {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local[1]").appName("friendsByAge").getOrCreate()
    def parseLine(line:String) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val noOfFriends = fields(3).toInt
      (age,noOfFriends)
    }
    val sc = spark.sparkContext
    val lines = sc.textFile("C:\\SparkScalaCourse\\data\\fakefriends-noheader.csv")
    val rdd = lines.map(parseLine)
    val totalsByAge = rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=> (x._1+y._1,x._2+y._2))
    val averagesByAge = totalsByAge.mapValues(x=>x._1/x._2)
    val sortedAveragesByAge = averagesByAge.sortByKey(ascending = true)
    sortedAveragesByAge.foreach(println)

  }
}
