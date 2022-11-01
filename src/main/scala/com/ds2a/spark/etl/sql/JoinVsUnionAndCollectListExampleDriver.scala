package com.ds2a.spark.etl.sql

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object JoinVsUnionAndCollectListExampleDriver {

  def collect_list(str: String): Column = ???

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val data1 = Seq((1, "A"), (2, "B"), (3, "C"))
    val data2 = Seq((1, "D"), (2, "E"), (3, "F"))

    val df1 = data1.toDF("id", "Name")
    val df2 = data2.toDF("id", "Name")

    //using joins
    val concat_join = df1.join(df2, df1("id") === df2("id"), "inner")
      .select(df1("id"), concat(df1("Name"), df2("Name")).as("concat")).show()

    //using unions approch
    val concat_union = df1.union(df2).groupBy("id")
      .agg(concat_ws("", collect_list("Name")).as("concat"))
      .orderBy("id")

    concat_union.explain()
    concat_union.explain(true)
    concat_union.show()
  }
}
