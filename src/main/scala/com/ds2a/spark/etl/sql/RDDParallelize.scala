package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object RDDParallelize {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(List(1,2,3,4,5))
    val rddCollect = rdd.collect()
    println("Number of Partitions: "+rdd.getNumPartitions)
    rddCollect.foreach(println)
  }
}
