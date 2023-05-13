import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Windows_Aggregations extends App{

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
    .option("header", "false")
    .option("inferSchema", "true")
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\windowdata.csv")
    .load()

  //df.show()
  val df2 = df.toDF("country","Weeknum","numinvoices","totalquantity","invoicevalue")

  df2.show()

  val mywindow = Window.partitionBy("country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)


  df2.withColumn("RunningTotal",sum("invoicevalue").over(mywindow)).show(50)


}
