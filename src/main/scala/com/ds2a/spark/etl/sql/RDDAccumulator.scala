package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object RDDAccumulator {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4))
    rdd.foreach(x=>longAcc.add(x))
    println(longAcc.value)

  }

}
