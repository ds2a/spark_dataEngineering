package com.ds2a.spark.etl.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lit}

object Steve {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CDR").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val labels = spark.read.option("header", "true").csv("C:\\Users\\anred\\Desktop\\fl\\roy-australia\\Assignment_Files\\Task_1\\spark-training-etl\\input\\tfm.csv")
    //labels.show()
    labels.printSchema()

    val byLabelingSetAndLabel = Window.partitionBy("labeling_set_id", "label")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)


    val cleanedLabels = labels.withColumn("original_cluster_size", count("vinoai_index").over(byLabelingSetAndLabel))
      .dropDuplicates("vinoai_index", "label")
      .withColumn("current_cluster_size", count("vinoai_index").over(byLabelingSetAndLabel))
      .where($"current_cluster_size" > lit(1) || $"original_cluster_size" === lit(1))

    // cleanedLabels.show()
    cleanedLabels.printSchema()

    cleanedLabels.coalesce(1).write.option("header", "true").csv("C:\\Users\\anred\\Desktop\\fl\\roy-australia\\Assignment_Files\\Task_1\\spark-training-etl\\output1\\")


  }

}
