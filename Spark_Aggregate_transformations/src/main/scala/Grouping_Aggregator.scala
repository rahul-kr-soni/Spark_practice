import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, _}

object Grouping_Aggregator extends App{
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

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\order_data.csv")
    .load()


  // Aggregation using column object expression
  val summarydf=df.groupBy("Country","InvoiceNo")
    .agg(sum("Quantity").as("TotalQuantity"),
      sum(expr("Quantity * UnitPrice")).as("Invoicevalue"))

  summarydf.show()


  // Aggregation using column string

  val df2 = df.groupBy("Country","InvoiceNo")
    .agg(expr("sum(Quantity) as TotalQuantiy"),
      expr("sum(Quantity * UnitPrice) as invoceValue"))
  df2.show


  // Spark Sql Approach

  df.createOrReplaceTempView("sales")

  spark.sql("Select Country,InvoiceNo,sum(Quantity) as TotalQuantity,sum(Quantity * UnitPrice) as invoceValue " +
    "from sales " +
    "group by Country,InvoiceNo ").show()


}
