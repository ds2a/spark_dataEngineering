package com.ds2a.spark.etl.sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DfRename {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()

    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1))
    val schema = new StructType().add("name",new StructType().add("fristname",StringType)
    .add("middlename",StringType).add("lastName",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.printSchema()
    df.withColumnRenamed("dob","DateOfBirth")
      .printSchema()
    val schema2 = new StructType().add("fname",StringType)
      .add("middlename",StringType).add("lname",StringType)

    df.select(col("name").cast(schema2),col("dob"),col("gender"),col("salary")).printSchema()
    df.withColumnRenamed("name.firstname","fname")
    .withColumnRenamed("name.middlename","mname")
    .withColumnRenamed("lastname","lname")
      .drop("name")
      .printSchema()
  }

}
