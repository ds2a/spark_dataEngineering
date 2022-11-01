package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object SparkSessionTest {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .getOrCreate()

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    val df = spark.read.csv("C:\\Users\\anred\\Desktop\\fl\\roy-australia\\Assignment_Files\\Task_1\\spark-training-etl\\MOCK_DATA.csv")

    df.show()


  }
}
