package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object ReadXMLDriver {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder().master("local").appName("XML").getOrCreate()
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

    val df = spark.read.format("xml").option("rootTag", "dataset").option("rowTag", "record")
      .load("C:\\Users\\gsuresh\\Downloads\\spark-scala-examples-master\\Input\\dataset.xml")

    //df.show(numRows = 100,true)
    df.write.format("csv").option("header", true)
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .mode("overwrite")
      .save("C:\\Users\\gsuresh\\Downloads\\spark-scala-examples-master\\Output\\")
    //    import spark.implicits._
    //    val filteredDF = df.filter($"gender"==="Male")
    //    filteredDF.write.csv("C:\\Users\\gsuresh\\Desktop\\csv")

  }

}
