package com.ds2a.spark.etl.sql.json

import org.apache.spark.sql.SparkSession

object DFJson {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val df = spark.read.option("multiline","true").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\JsonData.json")
    df.printSchema()
    df.show(false)

    df.write.json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\output\\JsonData.json")
  }
}
