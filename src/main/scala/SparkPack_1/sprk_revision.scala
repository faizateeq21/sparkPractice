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
object sprk_revision {
def main(args: Array[String]): Unit = {
  //println("Hello, world!")
  val spark = SparkSession.builder().master("local").appName("Spark_Revision").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  sc.setLogLevel("ERROR")
  println()
  println("*****************************Rev-1 Read Number list and add 2 to each element  *****************************")
  println()
  val llst = List(1,2,3,4,5,6,7,8,9,10)
  val llst2 = llst.map(x => x+2)
  println("INT llst2 is ",llst2)
  llst2.foreach(println)
  val lst = sc.parallelize(llst)
  val lst2 = lst.map(x => x+2)
  println("RDD lst2 is ",lst2.collect())
  println("RDD llst2 is ")
  lst2.foreach(println)
  println()




}
}
