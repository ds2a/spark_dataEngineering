package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

object update2 {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sourceDF = spark.read.option("delimiter",",").option("header",value = true).csv("C:\\Users\\akivi\\OneDrive\\Desktop\\my demos\\sourcefamily.csv")

    val targetDF = spark.read.option("delimiter",",").option("header",value = true).csv("C:\\Users\\akivi\\OneDrive\\Desktop\\my demos\\targetfamily.csv")

    val newRecordsDF =  sourceDF.join(targetDF, Seq("id"), "left").filter(targetDF("id") isNull).select(sourceDF("*"))

    newRecordsDF.show()

    val updatedRecords =   sourceDF.join(targetDF, Seq("id"), "inner").select(sourceDF("*"))
    updatedRecords.show()
    val resultDf = newRecordsDF.union(updatedRecords)
    resultDf.show()

    scala.io.StdIn.readLine()
  }

}



