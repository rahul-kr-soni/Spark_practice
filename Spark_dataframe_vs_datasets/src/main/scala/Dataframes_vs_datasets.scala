
//converting Dataframe to Datasets

import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}


// import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{Dataset, Row, SparkSession}


// creating case class refer line 29 to line 34

case class OrderData (order_id : Int ,order_date:String ,order_customer_id:Int,order_status:String)

object Dataframes_vs_datasets extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()

  //2 action (read,inferschema)
  val orderdf : Dataset[Row] = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")

   // orderdf.filter("order_ids < 10").show // it will throw error at run time in case of data frame

  // ====now convert dataframe to datasets=======
  // to convert we require case class

  //steps:
  // create case class --> OrdersData
  // and create dataset[OrdersData] // class type instead of row type

  //=======conversion from df to ds===================
  import spark.implicits._     //
  /*
  Here spark is variable created for the session.
  we can import this after creating Spark session.
  Implicits is require in order to convert df to ds and vice versa
   */
  val orderds = orderdf.as[OrderData]

  orderds.filter(x => x.order_id < 10).show()


  //scala.io.StdIn.readLine()
  spark.stop()
}
