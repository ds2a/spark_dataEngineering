package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object EmptyRDD {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val rdd = spark.sparkContext.emptyRDD
    val rddString = spark.sparkContext.emptyRDD[String]
    println(rdd)
    println(rddString)
    println("Num of Partitions: "+ rdd.getNumPartitions)
  }

}
