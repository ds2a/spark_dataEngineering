package com.ds2a.spark.etl.core


import org.apache.spark.sql.SparkSession

object word_count {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder().master("local").appName("word").getOrCreate()

    val textFile = spark.sparkContext.textFile("C:\\Users\\gsuresh\\Downloads\\sample.txt")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)
  }

}
