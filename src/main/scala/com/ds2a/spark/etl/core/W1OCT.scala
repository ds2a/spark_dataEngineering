package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession
import com.crealytics.spark.excel._

object W1OCT extends App {
  //
  //  val spark = SparkSession.builder().master("local[*]").appName("1st_day").getOrCreate()
  //val RDD = spark.sparkContext.textFile("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\Gopi_bio")
  ////val RDD2 = RDD.flatMap(line =>line.split(" ")).map(word=>(word,1)).groupBy(key=>key._1).
  //  val RDD2 = RDD.flatMap(line => line.split(" ")).map(word=>(word,1)).reduceByKey(_+_).
  //  foreach(println)

  val spark = SparkSession.builder().master("local").getOrCreate()

//  spark.sparkContext.setLogLevel("ERROR")
//  val RDD = spark.sparkContext.textFile("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\Gopi_bio")
//
//  //  RDD.take(10).foreach(println)
//  //  val rdd2 = RDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
//
//  //  val rdd3 = rdd2.take(5).foreach(println)
//
//  val f1 = RDD.flatMap(line => line.split(" ")).
//    filter(s => s.contains("my"))
//  //    map(word=>(word,1)).reduceByKey(_+_)
//  //  f1.foreach(println)
//  println(f1.count())

  //excel read
   val excel_read = spark.read.excel(true).load("C:\\Users\\sonti\\OneDrive\\Documents\\Book1.xlsx")

}
