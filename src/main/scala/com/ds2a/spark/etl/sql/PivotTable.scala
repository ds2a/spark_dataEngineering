package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object PivotTable {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))
    import spark.implicits._
    val df = data.toDF("products","Amount","country")
    df.show()

    val pivotDf1 = df.groupBy("products").pivot("country").sum("Amount")
    pivotDf1.show()



    val pivotDf3 = df.groupBy("products","country")
      .sum("Amount")
      .groupBy("products")
      .pivot("country")
      .sum("sum(Amount)")
      pivotDf3.show()
  }

}
