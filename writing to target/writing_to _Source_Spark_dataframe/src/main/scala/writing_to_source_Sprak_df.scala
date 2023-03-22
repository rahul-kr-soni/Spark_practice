import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}


object writing_to_source_Sprak_df extends App {

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
    .option("path", "G:\\scalalearning\\Spark_Dataframe1\\Spark_Dataframe_part1\\Data\\orders-201019-002101.csv")
    .load

//repartioning to testing

  println("orderdf has " ,orderdf.rdd.getNumPartitions)

  val orderRep =orderdf.repartition(4)
  println("orderdf has ", orderRep.rdd.getNumPartitions)

  orderRep.write
    .format("avro")  //for avro we need to import avro dependencies
    .mode(SaveMode.Overwrite)
    .option("path","G:\\scalalearning\\writing_to _Source_Spark_dataframe\\output_source\\avro_output")
    .save()

  orderdf.printSchema()
  orderdf.show()
}
