import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, column, expr, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Adding_columns  extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def ageCheck(Age:Int):String = {
    if (Age > 18) "Y"
  else "N"}


  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()

  // reading CSv File.
  val df = spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", true)
    .option("path", "G:\\scalalearning\\Database_spark\\Week12\\dataset1")
    .load

  df.printSchema()

  val df1 :Dataset[Row]= df.toDF("Names","Age","City")
  //df1.show()


  /*
  Column object UDF
   */
  val parseAgeFunction = udf(ageCheck(_:Int):String)

  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction" ).show()

 val df2 = df1.withColumn("Adult",parseAgeFunction(col("Age")))
 df2.show

  /*
  sql/ column string UDF  we can also use this for sql temp view or table
   */
  spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)

  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction" ).show()

  val df3 = df1.withColumn("Adult",expr("parseAgeFunction(Age)"))
  df3.show


  /*
  creating Anonymous function
   */
  spark.udf.register("parseAgeFunction", (x:Int)=> {if (x>18) "Y" else "N"})

  val df4 = df1.withColumn("Adult", expr("parseAgeFunction(Age)"))
  df4.show

  df1.createOrReplaceTempView("people")

  spark.sql("select * , parseAgeFunction(Age) from people").show
}
