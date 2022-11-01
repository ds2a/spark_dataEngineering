package com.ds2a.spark.etl.core

import org.apache.spark.sql.SparkSession

object readJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()

    val inputDf=spark.read.csv("C:\\Users\\Hemas\\Downloads\\MOCK_DATA (5).csv").show()
  }

}
