package com.ds2a.spark.etl.sql.employee

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim}

object EmployeeSelfJoinDSLDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("").master("local").getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    import spark.implicits._

    val empdf = spark.read.option("header", true).option("inferSchema",true).csv("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\employee.csv")
    val empdeptjoindf1 = empdf.as("e").join(empdf.as("m"), col("e.mid") === col("m.id"), "inner")

    empdeptjoindf1.printSchema()
    empdeptjoindf1.show()


  }

}