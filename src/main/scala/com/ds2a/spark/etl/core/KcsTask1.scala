package com.ds2a.spark.etl.core
/*
Problem Statement: You have to create data or files from the given dataset (Check Data Tab to access and download the data).
• heCourses.json
• students.csv
 Based on that please accomplish the following activities.
1. Create this two files in local directory and then upload the hdfs under the spark4 directory.
2. Use the in build schema for the heCourses.json file and create a new Dataframe from this.
3. Define a new schema for the students.csv as given below column name. a. StdID b. CourseId c. RegistrationDate
4. Using the above schema create a DataFrame for the "students.csv" data.
5. Find the list of the courses using both the dataframe which is/are not yet subscribed and then save the result in the "spark4/notSubscribed.json" directory.
6. Find the total fee collected by each course category. The column name of the total fee collected field should be "TotalFeeCollected"
7. Save the result in the "spark4/TotalFee.json"
//File Contents for the heCourses.json
[
  {
    "CourseId": 1001,
    "CourseFee": 7000,
    "Subscription": "Annual",
    "CourseName": "Hadoop Professional Training",
    "Category": "BigData",
    "Website": "HadoopExam.com"
  },
  {
    "CourseId": 1002,
    "CourseFee": 7500,
    "Subscription": "Annual",
    "CourseName": "Spark Professional Training",
    "Category": "BigData",
    "Website": "HadoopExam.com"
  },
  {
    "CourseId": 1003,
    "CourseFee": 7000,
    "Subscription": "Annual",
    "CourseName": "PySpark Professional Training",
    "Category": "BigData",
    "Website": "HadoopExam.com"
  },
  {
    "CourseId": 1004,
    "CourseFee": 7000,
    "Subscription": "Annual",
    "CourseName": "Apache Hive Professional Training",
    "Category": "Analytics",
    "Website": "HadoopExam.com"
  },
  {
    "CourseId": 1005,
    "CourseFee": 10000,
    "Subscription": "Annual",
    "CourseName": "Machine Learning Professional Training",
    "Category": "Data Science",
    "Website": "HadoopExam.com"
  },
  {
    "CourseId": 1006,
    "CourseFee": 7000,
    "Subscription": "Annual",
    "CourseName": "SAS Base",
    "Category": "Analytics",
    "Website": "HadoopExam.com"
  }
]

//File Contents for the students.csv
ST1,1004,20200201
ST1,1003,20200211
ST2,1002,20200206
ST2,1001,20200204
ST3,1004,20200202
ST4,1003,20200211
ST6,1004,20200207
ST7,1005,20200202
 ST9,1003,20200206
ST9,1002,20200209
ST3,1001,20200208
ST2,1004,20200207
ST1,1005,20200201
ST2,1003,20200204
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KcsTask1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("KcsTask1").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val heCoursesDf = spark.read.option("header","true").option("multiline","true").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\hecourses.json")
    heCoursesDf.printSchema()
    heCoursesDf.show(false)
     val schema = StructType(Array(
       StructField("StdID",StringType,nullable = true),
       StructField("CourseId",IntegerType,nullable = true),
       StructField("RegistrationDate",StringType,nullable = true)
     ))

    val studentsDf = spark.read.option("header","false").schema(schema).csv("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\input\\students.csv")
    studentsDf.printSchema()
    studentsDf.show(false)

    val CourseNotSubscribedDf = heCoursesDf.join(studentsDf,heCoursesDf("CourseId") === studentsDf("CourseId"),"leftAnti")
    CourseNotSubscribedDf.write.mode("overwrite").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\KcsTask1Target\\notsubscribed.json")
    CourseNotSubscribedDf.show(false)

    val CoursesSubscribedDf =  heCoursesDf.join(studentsDf,heCoursesDf("CourseId") === studentsDf("CourseId"),"inner")
    CoursesSubscribedDf.show(false)

    CoursesSubscribedDf.createTempView("CoursesSubscribedTable")
    val TotalFeeCollectedWrtCategoryDf = spark.sql("""select Category,sum(CourseFee) as TotalFeeCollected from CoursesSubscribedTable group by Category """).coalesce(1)
    TotalFeeCollectedWrtCategoryDf.show(false)
    TotalFeeCollectedWrtCategoryDf.write.mode("overwrite").json("C:\\Users\\akivi\\IdeaProjects\\spark-etl\\target\\KcsTask1Target\\TotalFee.json")
  }

}
