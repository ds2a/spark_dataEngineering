package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession


object CreateDf {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val columns = Seq("language","users_count")
    val data = Seq(("Java","20000"),("Python","10000"),("scala","3000"))
    val rdd = spark.sparkContext.parallelize(data)
    import spark.implicits._
    val dfFromRdd = rdd.toDF()
    dfFromRdd.printSchema()
    dfFromRdd.show()

    val dfFromRdd2 = rdd.toDF("language","users_count")
    dfFromRdd2.printSchema()
    dfFromRdd2.show()

    val dfFromRdd3 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRdd3.printSchema()
    dfFromRdd3.show()
  }
}
