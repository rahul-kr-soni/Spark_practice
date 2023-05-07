import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object spark_Transformation_session_12 extends App {


  // giving structure to non structure data

  Logger.getLogger("org").setLevel(Level.ERROR)

  val myregex = """^(\S+) (\S+)\t(\S+),(\S+)""".r

  case class Orders(order_id:Int,cust_id:Int,order_status:String)
  def paser(line : String)= {

    line match {
      case myregex(order_id,date,cust_id,order_status) => Orders(order_id.toInt,cust_id.toInt,order_status)
    }

  }



  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My first Spark_Dataframe")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    //.appName("My Spark_dataframe_app_1")
    //.master("local[2]")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val lines = spark.sparkContext.textFile("G:\\scalalearning\\Database_spark\\Week12\\REGEX_ORDER_NEW.txt")

  import spark.implicits._

  val orders_ds = lines.map(paser).toDS().cache()

  orders_ds.printSchema()
  orders_ds.select("order_id").show()

  orders_ds.groupBy("order_status").count().show()


}
