package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

object rddsample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //     val inputPath="C:\\Users\\Hemas\\OneDrive\\Documents\\Hemardd.txt"
    //      val rdd1=spark.sparkContext.textFile(inputPath)
    //    println(rdd1.partitions.length)
    //    println(rdd1.getNumPartitions)

    //      val rddwithoutheader=rdd1.filter(line=> !line.startsWith("id"))
    //      // rddwithoutheader.foreach(println)
    //                           //or
    //      val header=rdd1.first()
    //     // println(header)
    //    val rddwithoutheader1=rdd1.filter(line=> !line.contains(header))
    //    //rddwithoutheader1.foreach(println)
    //    val rddwithoutempty=rddwithoutheader.filter(row=> !row.startsWith(null))
    //    rddwithoutempty.foreach(println)

    //Map vs FlatMap
    val GeneralRDD = spark.sparkContext.textFile("C:\\Users\\Hemas\\OneDrive\\Documents\\MapVsFlatmap.txt")
    GeneralRDD.foreach(println)

    val MapRDD = GeneralRDD.map(x => x.split(","))

    println(MapRDD)

    val FlatMapRDD = GeneralRDD.flatMap(x => x.split(","))
    FlatMapRDD.foreach(println)
  }
}
