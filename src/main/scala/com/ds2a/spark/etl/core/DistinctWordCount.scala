package com.ds2a.spark.etl.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DistinctWordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("C:\\Users\\akivi\\OneDrive\\Desktop\\my demos\\manohar.txt")
    /*println("Intial partition count:" + rdd.getNumPartitions)

    val reparrdd = rdd.repartition(4)
    println("re-partition count: " + reparrdd.getNumPartitions)*/

    val rdd2: RDD[String] = rdd.flatMap(f => f.split(" "))

    val rdd3 = rdd2.map(f => (f, 1))
    println("No of words: " + rdd3.count())

    val rdd4 = rdd3.reduceByKey(_ + _)
    println("No of words: " + rdd4.count())
    rdd4.foreach(println)

    val rdd5 = rdd.filter(f => f.startsWith("M"))
    rdd5.foreach(println)
    println("No of words: " + rdd5.count()

    )
  }
}