package com.ds2a.spark.etl.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{arrays_zip, explode}

object JsonRead {

  //create main method
  def main(args: Array[String]): Unit = {

    //initiate sparksession

    val spark = SparkSession.builder().master("local").getOrCreate()

    //read json file
    spark.sparkContext.setLogLevel("ERROR")
    val inputJson = spark.read.option("multiline", true)
      .json("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\src\\main\\scala\\com\\ds2a\\spark\\etl\\scala\\JsonReadData")
    //.show(false)

    //import implicities
    import spark.implicits._
    //add new column split the array type convertion and explode the
    val df = inputJson.withColumn("new",arrays_zip($"phone_number",$"phone_type"))
    // val df2 = df.withColumn("explode",explode($"new"))
      .withColumn("explode",explode($"new"))
      .select("memberid","explode.*")
      .show()

    //.select("memberid","new.*")
     //df.show(false)


    //df.groupBy("memberid").pivot("phone_type").agg(concat_ws(",",collect_list($"phone_number"))).show()
    inputJson.createOrReplaceTempView("json")
   // spark.sql(
//      """
//        |  with cte as
//        | ( select t.memberid, t.numbers.* from
//        |  (select memberid, explode(arrays_zip(phone_number, phone_type)) numbers from json) t)
//        |select max(phone_number),memberid, phone_type
//        | from cte
//        | group by memberid, phone_type
//        |
//        |""".stripMargin).show()
  }



}
