package manohar

import org.apache.spark.sql.SparkSession

object wordCount {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local[1]").appName("wordCount").getOrCreate()
    val input = spark.sparkContext.textFile("C:\\SparkScalaCourse\\data\\book.txt")
    val words = input.flatMap(x=>x.split(" "))
    val words1 = input.flatMap(x=>x.split("\\W+"))
    val lowerWords1 = words1.map(x=>x.toLowerCase())
    val wordCounts = words.countByValue()
    val lowerWords = lowerWords1.countByValue()

    val wordCountsBetter = lowerWords1.map(x=>(x,1)).reduceByKey((x,y)=> x+y)
    val wordCountsSorted = wordCountsBetter.map(x=>(x._2,x._1)).sortByKey()

    wordCounts.foreach(println)
    lowerWords.foreach(println)
    for(result <- wordCountsSorted){
      val count = result._1
      val word = result._2
      println(s"$word:$count")
    }
  }
}
