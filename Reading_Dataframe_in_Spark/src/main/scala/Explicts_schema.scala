import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

object Explicts_schema extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()

// Programatically Approach     // *imp using this approach needs Spark Data Type
  val orderSchema = StructType(List(
    StructField("orderid",IntegerType),
    StructField("opderdate",TimestampType),
    StructField("Customerid",IntegerType),
    StructField("Status",StringType)
  ))




  // DDL Approach   // *imp using this approach needs Scala Data Type
  val orderSchemaDDL = "orderd Int, orderDate String, Custid Int, Status String"

// DF to DS
  case class OrderData (orderid : Int ,opderdate:Timestamp ,Customerid:Int,Status:String)

  val orderdf = spark.read
    .format("csv")
    .option("header", true)
    .schema(orderSchema)  //programatically approach
    //.schema(orderSchemaDDL)
    .option("path", "G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")
    .load

  orderdf.printSchema()
  orderdf.show()
// conversion df to sd
  import spark.implicits._
  val ds = orderdf.as[OrderData]
  ds.printSchema()
  ds.show()
}
