import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object typeSafety {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local").getOrCreate()


    val ApplicationConf = ConfigFactory.load("Application.conf")
    val userName= ApplicationConf.getString("app.user_name")

    println(userName)
  }
}
