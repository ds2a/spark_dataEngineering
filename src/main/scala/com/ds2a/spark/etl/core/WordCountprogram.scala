package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

object WordCountprogram {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("WordCountExample").getOrCreate()

    val inputRdd = spark.sparkContext.textFile("C:\\Users\\akivi\\OneDrive\\Desktop\\interview\\SQL.txt")


    val inputSplitRdd = inputRdd.flatMap(line => line.split(" ")).map(word => (word, 1))

    val inputSplitRdd1 = inputSplitRdd.reduceByKey(_+_)

    inputSplitRdd1.foreach(println)
    println("word count: "+inputSplitRdd1.count())
    //inputSplitRdd.foreach(println)

  }

}
