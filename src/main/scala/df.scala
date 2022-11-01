import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.util.ArrayList

object df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("df").getOrCreate()

    val schema = StructType(Array(
      StructField("language", StringType, true),
      StructField("users", StringType, true)
    ))


    var l = new ArrayList[Row]()
    l.add(Row("Java", "20000"))
    l.add(Row("Java", "20000"))
    l.add(Row("Java", "20000"))

    //    val rowData = List(Row("Java", "20000"),
    //      Row("Python", "100000"),
    //      Row("Scala", "3000"))

    //    rowData

    //    val rowData= Seq(Array(("Java", "20000"),
    //      ("Python", "100000"),
    //      ("Scala", "3000")))

    var dfFromData3 = spark.createDataFrame(l, schema)

    dfFromData3.show()
  }

}
