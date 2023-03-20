import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Reading_parquet extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()


  val orderdf = spark.read
    .format("parquet") //parquet is default format
    // .option("header", true)   // no concept of header in parquet
    //.option("inferSchema", true)   //self describing data
    .option("path","G:\\scalalearning\\Reading_Dataframe_in_Spark\\Data\\users-201019-002101.parquet")
    .load

  orderdf.printSchema()
  orderdf.show(false) // truncate = false to show entire data dont cut the data

}
