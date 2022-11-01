import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.spark.sql.SparkSession




object ExcelRead {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("ExcelRead").getOrCreate()
    val df = spark.read.excel(false).load("C:\\Users\\sonti\\OneDrive\\Documents\\Book2.xlsx")
    df.show()
  }

}
