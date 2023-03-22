import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveAsTable extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  // reading CSv File.
  val orderdf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")
    .load

  //repartioning to testing

  println("orderdf has ", orderdf.rdd.getNumPartitions)

  spark.sql("create database if not exists retail")
  orderdf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail.Order1")

  spark.catalog.listTables("retail").show()

  //orderdf.printSchema()
  //orderdf.show()



}
