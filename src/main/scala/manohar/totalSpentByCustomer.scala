package manohar

import org.apache.spark.sql.SparkSession

object totalSpentByCustomer {
  def extractCustomerPricePairs(line:String):(Int,Float)={
    val fields = line.split(",")
    (fields(0).toInt,fields(2).toFloat)
  }
  def main(args: Array[String]): Unit = {
    val spark:SparkSession= SparkSession.builder().master("local[1]").appName("totalSpentByCustomer").getOrCreate()
    val input = spark.sparkContext.textFile("C:\\SparkScalaCourse\\data\\customer-orders.csv")
    val mappedInput = input.map(extractCustomerPricePairs)
    val totalByCustomer = mappedInput.reduceByKey((x,y) => x + y)
    totalByCustomer.foreach(println)
    val flipped = totalByCustomer.map(x=>(x._2,x._1))
    val totalCustomerSorted = flipped.sortByKey()
    val results = totalCustomerSorted.collect()
    results.foreach(println)
  }
}
