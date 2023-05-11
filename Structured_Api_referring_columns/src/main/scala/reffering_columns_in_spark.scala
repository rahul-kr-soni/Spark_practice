import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object reffering_columns_in_spark extends App {
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
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\orders.csv")
    .load

  //orderdf.show

  //column string way
  orderdf.select("order_id","order_status").show

  //column object way





  orderdf.select(column("order_id"),col("order_status")).show

  import spark.implicits._

  //$ and ' sign are scala specific to call column object

  orderdf.select($"order_date",'order_status).show()


  //colunm expression
  //orderdf.select(column("orde_id"),"concat(order_status,'_status')")    """ this line thow error because we cannot merge column object and col expr

  //way to combine add "expr before expression

  orderdf.select(column("order_id"),expr("concat(order_status,'_status')")).show(false)

  //easy way

  orderdf.selectExpr( "order_id","concat(order_status,'_status')").show(false)


}
