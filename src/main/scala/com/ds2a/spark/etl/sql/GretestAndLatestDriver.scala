package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{greatest, least}

object GreatestAndLatestDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val list_emp = Seq(("Hema", 20, 40, 45, 67, 83),
      ("Anu", 34, 45, 67, 78, 65),
      ("Venu", 35, 13, 32, 56, 76),
      ("Swetcha", 67, 34, 56, 80, 65))
   val list_schema = Seq("Name", "term-1", "term-2", "term-3", "term-4", "term-5")
    val df_list = list_emp.toDF("Name", "term-1", "term-2", "term-3", "term-4", "term-5")
   //val df_list=spark.createDataFrame(list_emp,list_schema )
    df_list.show()

    val df_greatandlow=df_list.withColumn("Great",greatest("term-1", "term-2", "term-3", "term-4", "term-5"))
      .withColumn("Least",least("term-1", "term-2", "term-3", "term-4", "term-5"))
    df_greatandlow.show()
  }
}
