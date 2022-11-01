package com.ds2a.spark.etl.sql

import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.catalyst.expressions.codegen.CodeFormatter.format
import org.apache.spark.sql.functions.{col, format_string, to_date, to_timestamp}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object createAnilDf {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val data = Seq(Row("James", "", "Smith", "1991-04-01", "M", 3000),
      Row("Michael", "Rose", "", "2000-05-19", "M", 4000),
      Row("Robert", "", "Williams", "1978-09-05", "M", 4000),
      Row("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
      Row("Jen", "Mary", "Brown", "1980-02-17", "F", -1))
    val data1 = Seq(("James", "", "Smith", "1991-04-01", "M", 3000),
      ("Michael", "Rose", "", "2000-05-19", "M", 4000),
      ("Robert", "", "Williams", "1978-09-05", "M", 4000),
      ("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
      ("Jen", "Mary", "Brown", "1980-02-17", "F", -1))
    val schema = StructType(Array(StructField("firstname", StringType, nullable = true), StructField("middlename",
      StringType, nullable = true),
      StructField("lastname", StringType, nullable = true), StructField("dob", StringType, nullable = true),
      StructField("gender", StringType, nullable = true), StructField("salary", IntegerType, nullable = true)))
    val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
    import scala.collection.JavaConversions._
    import spark.implicits._

    val df = spark.createDataFrame(data, schema)
    df.show()

    val df1 = spark.createDataFrame(data1).toDF(columns: _*)
    df.show()
    df.printSchema()
    val df3 = df1.withColumn("dob",col("dob").cast(TimestampType))
    df3.show()
    val df2 = df1.withColumn("dob1", to_timestamp(col("dob")))
                .withColumn("dob2", to_date(col("dob")))
    df2.printSchema()
    df2.show()

    df3.printSchema()
    /*Using Rdd's
     */
  }
}
