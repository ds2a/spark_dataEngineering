package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession


object DataFrameParquet {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val df =  spark.read.parquet("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\userdata1.parquet")
    df.printSchema()
    df.show {
      false
    }

    df.createOrReplaceTempView("ParquetTable")
    val parkSQl = spark.sql("select * from ParquetTable where (gender = 'Male')and(country ='Russia')")
    parkSQl.show()
    parkSQl.printSchema()
    df.write.parquet("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\output\\parquet2")
    //df.write.partitionBy("gender","country").parquet("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\output\\parquet1")

  }
}
