package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object sparksessiontest2 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      //      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val x = spark.sparkContext.parallelize(Array("b", "a", "c"))
    val y = x.map(z => (z, 1))
    println(x.collect().mkString(", "))
    println(y.collect().mkString(", "))

  }
}
