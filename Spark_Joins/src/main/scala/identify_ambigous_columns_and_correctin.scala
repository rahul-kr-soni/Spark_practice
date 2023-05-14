import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, _}



object identify_ambigous_columns_and_correctin extends App {

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

  //1st way to hanble ambigouty ==> renaming column
  val orderdfnew= orderdf.withColumnRenamed("order_customer_id","cust_id")

 /* orderdf.show

  val joineddf = orderdf.join(customerdf, orderdf.col("order_customer_id") ===
    customerdf.col("customer_id"), "inner")

  joineddf.show

*/
  // better way to increase readability

  val joinCondition = orderdfnew.col("cust_id") === customerdf.col("customer_id")

  val joinType = "outer" //type of joins available ="right","left","outer","inner"

  val joindf = orderdfnew.join(customerdf, joinCondition, joinType) //.sort("cust_id")


  joindf.select("order_id","cust_id","customer_fname").sort("order_id").show



  // 2nd way to handle ambigouity  ==> dropping ambigous column

  val joinCondition1 = orderdf.col("order_customer_id") === customerdf.col("customer_id")

  val joinType1 = "outer" //type of joins available ="right","left","outer","inner"

  orderdf.join(customerdf, joinCondition1, joinType1)
    .drop(orderdf.col("order_customer_id"))
    .select("order_id","customer_id","customer_fname")
    .sort("order_id")
    .withColumn("order_id",expr("coalesce (order_id,-1)")) //handling null
    .show()
}
