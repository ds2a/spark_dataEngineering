package com.ds2a.spark.etl.sql.employee

import org.apache.spark.sql.SparkSession

object EmployeeSelfJoinSQLDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("").master("local").getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val empdf = spark.read.option("header", true).option("inferSchema", true).csv("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\input\\employee.csv")
    empdf.createOrReplaceTempView("employee")

    val empdeptjoindf1 = spark.sql(
      """ select e.name ,
                                     m.name
                                     from employee e
                                     left join employee m
                                     on e.mid = m.id """)

    empdeptjoindf1.printSchema()
    empdeptjoindf1.show()

  }
  }