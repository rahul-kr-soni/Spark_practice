import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark_sql extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()

  // reading CSv File.
  val orderdf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")
    .load

  orderdf.createOrReplaceTempView("orders") // creating view from Dataframe
  //
  //
  // val result_df = spark.sql("Select order_status, count(*) as cnt from orders group by order_status order by cnt desc")

  val result_df=spark.sql("select order_customer_id,count(*) as order_cnt from orders" +
    " where order_status ='CLOSED' group by order_customer_id order by order_cnt desc")
    result_df.show()

  //orderdf.printSchema()
  //orderdf.show()


}
