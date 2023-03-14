//take orderdf  do repartion into 4 part
//filter the orderdf order_customer_id >10000
//display your log information'


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Operation_spark_df extends App {

  //Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()

//2 action (read,inferschema)
  val Orderdf = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")

  //Orderdf.show(50)
  //Orderdf.printSchema()
  // Below are transformation
 val groupdf = Orderdf
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()


  //action
  groupdf.show()

  Logger.getLogger(getClass.getName).info("Program run Succesfully")
  scala.io.StdIn.readLine()

  spark.stop()

}
