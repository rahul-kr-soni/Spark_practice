import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, _}



object Simple_Jions extends App{

  // setting the log level
  Logger.getLogger("org").setLevel(Level.ERROR)

  //setting up Spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")

  //creating Spark session
  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()


  val customerdf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\customers.csv")
    .load()

  customerdf.show()


  val orderdf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\orders.csv")
    .load()
  orderdf.show

  val joineddf =orderdf.join(customerdf,orderdf.col("order_customer_id") ===
    customerdf.col("customer_id"),"inner")

  joineddf.show

  // better way to increase readability

  val joinCondition = orderdf.col("order_customer_id") === customerdf.col("customer_id")

  val joinType = "right"   //type of joins available ="right","left","outer","inner"

  val joindf = orderdf.join(customerdf, joinCondition, joinType).sort("order_customer_id")



  joindf.show


}
