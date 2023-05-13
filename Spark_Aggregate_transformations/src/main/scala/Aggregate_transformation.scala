import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, _}

object Aggregate_transformation extends App {

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

  val df = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("path","G:\\scalalearning\\Database_spark\\Week12\\order_data.csv")
    .load()

  df.show(false)


  // Aggregation using column object expression
df.select(
  count("*").as("row_count"),
  sum("Quantity").as("TotalQuantity"),
  avg("UnitPrice").as("AvgPrice"),
  countDistinct("InvoiceNo").as("Count_Distinct")
).show()


  // Aggregation using column string

  df.selectExpr(
    "count(*) as Row_count",
    "sum(Quantity) as TotalQuantity",
    "avg(UnitPrice) as AvgPrice",
    "count(Distinct(InvoiceNo)) as CountDistinct"
  ).show()


  // Spark Sql Approach

  df.createOrReplaceTempView("sales")

  spark.sql(
    "select count(*) as Row_count, sum(Quantity) as TotalQuantity,avg(UnitPrice) as AvgPrice,"+
    "count(Distinct(InvoiceNo)) as CountDistinct from sales"
  ).show()




}
