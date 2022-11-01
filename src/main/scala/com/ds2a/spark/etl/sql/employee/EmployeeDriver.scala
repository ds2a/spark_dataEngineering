package com.ds2a.spark.etl.sql.employee

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim

object EmployeeDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("").master("local").getOrCreate()
    import spark.implicits._

    val empdf = spark.read.option("header", true).csv("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\employee.csv")
    val depddf = spark.read.option("header", true).csv("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\dept.csv")
    empdf.printSchema()
    empdf.show()
    depddf.printSchema()
    depddf.show()

    val empdeptjoindf = empdf.join(depddf, trim(empdf("did")) === depddf("did"), "inner").select($"id", empdf("did"))
    val empdeptjoindf1 = empdf.join(depddf, Seq("did"), "inner")

    empdeptjoindf1.printSchema()
    empdeptjoindf1.show()

    empdeptjoindf.printSchema()
    empdeptjoindf.show()



  }

}