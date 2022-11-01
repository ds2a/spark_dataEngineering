import org.apache.spark.sql.functions.{col, current_date, datediff, floor, round, to_timestamp}

object jJsonPractice1 extends  App with spark {
spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
 val inputJson = spark.read
   .option("multiline","true")
  //  options(Map("header" -> "true","inferSchema" -> "true"))
    .json("C:\\Users\\sonti\\Downloads\\MOCK_DATA (6).json")
   // .show(100,false)
    //.printSchema()
  val age_cal = inputJson.withColumn("age",
      round(datediff(current_date,to_timestamp($"joining_date","dd.mm.yyyy")))
      .divide(365.25))
    .show()
}