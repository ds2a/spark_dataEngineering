package manohar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.math.min

object minTemperatures {
  def main(args: Array[String]): Unit = {


  def parseLine(line:String):(String,String,Float)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat*0.1f*(9.0f/5.0f)+32.0f
    (stationID,entryType,temperature)
  }

  val spark:SparkSession = SparkSession.builder().master("local[1]").appName("minTemperatures").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  val lines: RDD[String] = spark.sparkContext.textFile("C:\\SparkScalaCourse\\data\\1800.csv")
  val parsedLines: RDD[(String, String, Float)] = lines.map(parseLine)
  val minTemps: RDD[(String, String, Float)] = parsedLines.filter(x=> x._2 == "TMIN")
  val stationTemps: RDD[(String, Float)] = minTemps.map(x=>(x._1, x._3))
  val minTempsByStation: RDD[(String, Float)] = stationTemps.reduceByKey((x, y) => min(x,y))
  val results: Array[(String, Float)] = minTempsByStation.collect()
  for(result <- results){
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"
    println {
      s"$station minimum temperature:$formattedTemp"
    }
  }

}}
