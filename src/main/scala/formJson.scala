import manohar.jsonSchema
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object formJson {
//initate main class
  def main(args: Array[String]): Unit = {

    //sparkSession intiation
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //read json file
    import spark.implicits._
    val inputJson = spark.read.options(Map("header"->"true","delimiter"->"|"))
      .csv("C:\\Users\\sonti\\IdeaProjects\\spark-etl\\src\\main\\scala\\forrmJson.csv")
      //.show(false)
     // .printSchema()
    //custom schema
val customSchema = new StructType().add("Zipcode",IntegerType)
  .add("ZipCodeType",StringType)
  .add("City",StringType)
  .add("State",StringType)
    //final json data

    val schemaFromCaseClass = Encoders.product[jsonSchema].schema

  val finaljson = inputJson.select(col("Id"),from_json(col("JsonValue"),schemaFromCaseClass).as("json_intial"))
    //.show(false)
    .select("id","json_intial.*")
    //.show(false)
   val numPart =  finaljson.rdd.getNumPartitions
    println(numPart)
//    val dfFromCSVJSON =  dfFromCSV.select(col("Id"),
//      from_json(col("JsonValue"),schema).as("jsonData"))
//      .select("Id","jsonData.*")
//    dfFromCSVJSON.printSchema()
//    dfFromCSVJSON.show(false)
//    finaljson.repartition(19).write.options(Map("header"->"true")).mode("overWrite")
//      .csv("C:\\Users\\sonti\\OneDrive\\Desktop\\writeTest")


  }

}
