import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExcelReadWriteDriver {

  def readExcel(spark: SparkSession, file: String): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true").
      option("treatEmptyValuesAsNulls", "false").
      option("inferSchema", "false").
      option("addColorColumns", "false").load(file)
  }


  def main(args: Array[String]): Unit = {

    // creating Spark Session instance
    val spark = SparkSession.builder().master("local").appName("reading_excel").getOrCreate()
    //reading book1.xls file data to a dataframe
    //    val df = spark.read.excel(true).load("C:\\Users\\sonti\\OneDrive\\Documents\\Book1.xlsx")

    val df = readExcel(spark, "C:\\Users\\sonti\\OneDrive\\Documents\\Book1.xlsx")

    //display 20 records of dataframe
    df.show()


  }
}