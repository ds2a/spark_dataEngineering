package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession


object DataFrame {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()

    val df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\members.csv")
    df.printSchema()
    df.show(false)

    df.write.csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\output\\members.csv")
  }

}
