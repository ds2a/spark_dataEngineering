package com.ds2a.spark.etl.sql.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


//https://www.projectpro.io/recipes/work-with-complex-nested-json-files-using-spark-sql

object ComplexJsonParserDriver {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()


    import spark.implicits._

    val schema = new StructType()
      .add("dc_id", StringType) // data center
      .add("source", // info about the source of alarm
        MapType( // define this as a Map(Key->value)
          StringType,
          new StructType()
            .add("description", StringType)
            .add("ip", StringType)
            .add("id", LongType)
            .add("temp", LongType)
            .add("c02_level", LongType)
            .add("geo",
              new StructType()
                .add("lat", DoubleType)
                .add("long", DoubleType)
            )))


    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val df = spark.read.option("multiline", true).schema(schema).json("/FileStore/tables/complex.json")
    //  display(df) //use when working in Databricks only

    val explodedDF = df.select($"dc_id", explode($"source"))
    explodedDF.printSchema()
    explodedDF.show(false)

    val final_df = explodedDF.select($"dc_id" as "dcId",
      $"key" as "deviceType",
      col("value").getItem("ip") as "ip",
      'value.getItem("id") as 'deviceId,
      'value.getItem("c02_level") as 'c02_level,
      'value.getItem("temp") as 'temp,
      'value.getItem("geo").getItem("lat") as 'lat, //note embedded level requires yet another level of fetching.
      'value.getItem("geo").getItem("long") as 'lon)
    final_df.printSchema()
    final_df.show()

  }

}
