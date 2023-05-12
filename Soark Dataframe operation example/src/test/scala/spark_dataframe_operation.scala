import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object spark_dataframe_operation extends App {

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

  val mylist = List((1,"2013-07-25" ,11599,"CLOSED"),
  (2,"2013-07-25",256,"PENDING_PAYMENT"),
  (3,"2013-07-25",11599,"COMPLETE"),
  (4,"2013-07-25",8827,"CLOSED"))


  // low leved rdd way

  import spark.implicits._

  val rdd = spark.sparkContext.parallelize(mylist)

  rdd.toDF().show()

  // easy way

 val orderdf= spark.createDataFrame(mylist)

  orderdf.printSchema()
  orderdf.show

  // providing column name

  val df = orderdf.toDF("order_id","orderDate","Customerid","status")

  df.show()


  // if i want to add or change the content of column i should use .withcoulmn

  val newdf = df.withColumn("orderDate",unix_timestamp(col("orderDate").cast(DateType)))

  newdf.printSchema()

  newdf.show()

  //create new column

  val df2 = newdf.withColumn("newid",monotonically_increasing_id())
  df2.printSchema()
  df2.show()


  // drop duplicates

  val df3 = newdf.withColumn("newid",monotonically_increasing_id())
    .dropDuplicates("orderDate","Customerid")
  df3.show()

  // droping order id
val dropdf = newdf.withColumn("newid",monotonically_increasing_id())
  .dropDuplicates("orderDate","Customerid")
  .drop("order_id")
  .sort("orderDate")
  dropdf.show()






  //stopping spark session
  spark.stop()
}
