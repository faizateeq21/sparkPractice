package SparkPack_1
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql._
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Spark_Revision").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("ERROR")
    println()
    println("Spark Streaming")
    println("===============")
    println()
    val structType = StructType(Array(StructField("name", StringType, nullable = true), StructField("age", IntegerType, nullable = true)))
    val df=spark.readStream.schema(structType).option("sep", ",").csv("file:///C:/Data/SparkStreamInput")
    df.writeStream.option("checkpointLocation","file:///C:/Data/SparkStreamOutput").format("console").start().awaitTermination()
    println("Spark Streaming")

    //val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()
//    val df1 = df.selectExpr("CAST(value AS STRING)")







  }

}
