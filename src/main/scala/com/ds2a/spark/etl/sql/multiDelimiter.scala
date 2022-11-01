
/*package com.ds2a.spark.etl.sql

import org.apache.spark.sql.SparkSession

object multiDelimiter {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession.builder().master("local[1]")
      .appName("sparkByExamples.com")
      .getOrCreate()
    val df = spark.read.text("C:\\Users\\akivi\\OneDrive\\Desktop\\S2A Technologies HYd\\ravi.csv")
    //df.show()
    val header = df.first()[0]
    val schema = header.split('~|')
    print(schema)

  }

}
*/
