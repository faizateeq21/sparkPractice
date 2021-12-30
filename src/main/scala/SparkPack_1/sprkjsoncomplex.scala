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
object sprkjsoncomplex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("sprkjsoncomplex").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("ERROR")

    println()
    println("*****************************Read Complex JSON *****************************")
    println()

    val csdf = spark.read.option("multiline","true").json("C://DATA/Intellij_Data/comjson2.json")
    csdf.printSchema()
/*
    println()
    println("*****************************Read Complex JSON *****************************")
    println()

    val petsdf = spark.read.option("multiline","true").json("C://DATA/Intellij_Data/pets.json")
    petsdf.printSchema()

    println()
    println("*****************************Flatten Complex JSON *****************************")
    println()

    val flatpdf = petsdf.withColumn("Pets",explode(col("Pets"))).select("Address.*","Mobile","Name","Pets","status")
    flatpdf.printSchema()

    println()
    println("*****************************Regenerate Complex JSON *****************************")
    println()

    val rscomdf = flatpdf.select(struct(col("Permanent address"),col("current Address")).as("Address"),col("Mobile"),
      col("Name"),col("status"),col("Pets"))
    rscomdf.printSchema()

    println()
    println("*****************************Read Complex JSON *****************************")
    println()

    val finalcdf = rscomdf.groupBy(
      col("Name"),col("Address"),
      col("Mobile"),col("status"))
     .agg(collect_list(col("Pets")).as("Pets"))
      .select(col("Name"),col("Address"),
        col("Mobile"),col("status"),
        col("Pets"))
     finalcdf.printSchema()

    println()
    println("*****************************Generate final  Complex JSON *****************************")
    println()

    val comdf = spark.read.format("json").option("multiline","true")
      .load("C://DATA/IntelliJ_Data/comjson.json")

    comdf.printSchema()
    comdf.show()

    println("*****************************Explode Array *****************************")
    //val expldf = comdf.withColumn("Students", explode($"Students"))
    val expldf = comdf.withColumn("Students", explode(col("Students")))

    expldf.printSchema()
    expldf.show()
    println( )
    println("*****************************Json Struct  *****************************")
    val datadf = spark.read.format("json").option("multiline","true")
      .load("C://DATA/IntelliJ_Data/picture_multiline.json")


    datadf.show()
    datadf.printSchema()

    println("***********************Flatttendf************************")
    val flatdf = datadf.withColumn("image_url",expr("image.url"))
      .withColumn("image_height",expr("image.height"))  .withColumn("image_width",expr("image.width"))
                        .drop("image")
      .withColumn("thumbnail_url",expr("thumbnail.url"))
      .withColumn("thumbnail_height",expr("thumbnail.height"))
      .withColumn("thumbnail_width",expr("thumbnail.width"))
      .drop("thumbnail")
    flatdf.show()
    flatdf.printSchema()


    spark.stop()
*/

  }
}
