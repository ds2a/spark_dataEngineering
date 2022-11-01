package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object ReadJsonDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("json").getOrCreate()
    val df = spark.read.json("C:\\Users\\gsuresh\\Downloads\\MOCK_DATA.json")
    val fillDF = df.select("id", "first_name", "last_name", "gender", "ip_address", "email")
    fillDF.show()
    //fillDF.write.csv("C:\\Users\\gsuresh\\Desktop\\json_csv")
    //import spark.implicits._
    //val filteredDF = df.filter($"gender"==="Male")
    //filteredDF.show()
  }

}
