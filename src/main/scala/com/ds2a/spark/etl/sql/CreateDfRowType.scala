package com.ds2a.spark.etl.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CreateDfRowType {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val schema = StructType(Array(
      StructField("language",StringType,nullable = true) ,StructField("user-account",StringType,nullable = true)))


    val data = Seq(("java","2000"),("python","10000"),("scala","3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val rowRdd = rdd.map(f => Row(f._1,f._2))
    val dfFromRdd =spark.createDataFrame(rowRdd,schema)
    dfFromRdd.show()
    dfFromRdd.printSchema()
  }

}
