package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, from_json}
import org.apache.spark.sql.types.{DateType,StringType, StructField, StructType}

object jsonData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val EcommerceDataDf = spark.read.option("header",value = false).option("delimiter","|").csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\jsondata.txt")
    val EcommerceDataDf1 = EcommerceDataDf.toDF("value")
    EcommerceDataDf1.show(false)
    val schema = new StructType(Array(StructField("datetime",StringType, nullable = true)
      ,StructField("userid",StringType, nullable = true)
      ,StructField("country", StringType, nullable = true)
      ,StructField("sessionid", StringType, nullable = true)
      ,StructField("http_method", StringType, nullable = true)
      ,StructField("url", StringType, nullable = true)))

    val EcommerceListDataDf = EcommerceDataDf1.withColumn("value",from_json(col("value"),schema))
    EcommerceListDataDf.show(false)
    val EcommerceAllDataDf = EcommerceListDataDf.select(col("value.*"))
    EcommerceAllDataDf.show(false)
    val EcommerceDataDayLevelDf = EcommerceAllDataDf.withColumn("date",EcommerceAllDataDf("datetime").cast(DateType))
    EcommerceDataDayLevelDf.show(false)

import spark.implicits._
    val distinctDf = EcommerceAllDataDf.distinct()
    //distinctDf.show(false)

    val distinctDfWrtuserid = EcommerceAllDataDf.select(EcommerceAllDataDf("userid")).distinct
    println("Distinct count: "+distinctDfWrtuserid.count())

    val PurchasedItemsDataDayLevelDf = EcommerceDataDayLevelDf.filter(EcommerceDataDayLevelDf("http_method") === "POST").groupBy("date").agg(count($"http_method") as "purchased")
    val  PurchasedItemsWrtPurchasedDateDf= PurchasedItemsDataDayLevelDf.withColumnRenamed("date","purchasedDate").coalesce(1)
    PurchasedItemsWrtPurchasedDateDf.show()
    PurchasedItemsWrtPurchasedDateDf.write.option("header","true").mode("overwrite").csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\EcommerceTargetDir\\PurchasedItemsWrtPurchasedDate.csv")

    val PurchasedAndNonPurchasedItemsDataDayLevelDf = EcommerceDataDayLevelDf.filter(EcommerceDataDayLevelDf("http_method") === "PUT" or EcommerceDataDayLevelDf("http_method") === "POST").groupBy("date").agg(count($"http_method") as "purchasedAndNonPurchased")
    val PurchasedAndNonPurchasedItemsWrtPurchasedAndNonPurchasedDateDf = PurchasedAndNonPurchasedItemsDataDayLevelDf.withColumnRenamed("date","purchasedAndNonPurchasedDate")

    PurchasedItemsWrtPurchasedDateDf.createTempView("purchasedTable")
    PurchasedAndNonPurchasedItemsWrtPurchasedAndNonPurchasedDateDf.createTempView("purchasedAndNonPurchasedTable")
    val purchasedStatusDf = spark.sql("""select purchasedAndNonPurchasedDate,purchasedAndNonPurchased,purchased from purchasedTable right join purchasedAndNonPurchasedTable on purchasedTable.purchasedDate = purchasedAndNonPurchasedTable.purchasedAndNonPurchasedDate""").na.fill(0)
    val ItemsStatusOnDayLevelDf =  purchasedStatusDf.withColumn("nonPurchased",col("purchasedAndNonPurchased")-col("purchased"))
    ItemsStatusOnDayLevelDf.write.option("header","true").mode("overwrite").csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\EcommerceTargetDir\\ItemsStatusOnDayLevel.csv")
    ItemsStatusOnDayLevelDf.show()

    EcommerceDataDayLevelDf.createTempView("DataTable")

    val SessionsDayLevelWrtNonPurchasedItemsDf= spark.sql("""select date,count(sessionid) as sessionsLoginAndLogoutAndWithoutPurchased from (select date,sessionid from DataTable group by date,sessionid having sum(case when http_method = 'POST' or url = '/login' or url = '/logout' then 1 else 0 end) =2) as t group by date""")
    SessionsDayLevelWrtNonPurchasedItemsDf.write.option("header","true").mode("overwrite").csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\EcommerceTargetDir\\SessionsDayLevelWrtNonPurchasedItems.csv")
    SessionsDayLevelWrtNonPurchasedItemsDf.show()
  }
}
