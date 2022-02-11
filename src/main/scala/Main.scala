import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Main extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark:SparkSession = SparkSession.builder().appName("jsonParquet")
    .getOrCreate()
  val custom_schema = StructType(Array(
    StructField("ORIGIN_COUNTRY_NAME",StringType,true),
    StructField("DEST_COUNTRY_NAME",StringType,true),
    StructField("count",StringType,true)
  ))
  val df = spark.read.schema(custom_schema).json("./example.json")
    .write.parquet("hdfs://localhost/user/hive/kalin/")
}

