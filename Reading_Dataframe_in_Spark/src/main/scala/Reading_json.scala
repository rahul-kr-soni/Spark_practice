import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Reading_json extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .getOrCreate()


  val orderdf = spark.read
    .format("json")
    // .option("header", true)   // no concept of header in json
    //.option("inferSchema", true)   //json has property to infer schema
    .option("path","G:\\scalalearning\\Reading_Dataframe_in_Spark\\Data\\players.json")
    // .option("mode","DROPMALFORMED")  // 3type of Mode : PERMISSIVE.DROPMALFORMED,FAILFAST
    .load

  orderdf.printSchema()
  orderdf.show()

}
