package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

object RatingsCounter {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local[1]").appName("Movieratings").getOrCreate()
    val lines = spark.sparkContext.textFile("C:\\ml-100k\\u.data")
    val ratings = lines.map(x=>x.split("\t")(2))
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)

     }



}
