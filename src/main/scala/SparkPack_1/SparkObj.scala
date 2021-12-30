package SparkPack_1
//import SparkPack_1.awsObj.spark
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
object SparkObj {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val conf = new SparkConf().setAppName("SparkFirst").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //val spark = SparkSession.builder.appName("SparkObj").master("local").getOrCreate()
    //val spark = SparkSession.builder.getOrCreate()

    val spark = SparkSession.builder()

      .config("fs.s3a.access.key","AKIAQJ35YRX5DB4UBB6E")

      .config("fs.s3a.secret.key","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")


      .getOrCreate()
    import spark.implicits._



    val df = spark.read.format("json")
      .load("s3a://zeyonifibucket/data/devices.json")



    df.write.format("csv").save("s3a://zeyonifibucket/aamir_dir")

    println("done")
    // val sc = spark.sparkContext
    /*
      val rdd = sc.parallelize(List(1,2,3,4,5,6 ,7,8,9,10))
      //rdd.take(10).foreach(println())
      println("rdd.count() = " + rdd.count())
      val rdd2 = rdd.map(x => x*x)
      val rdd3 = rdd2.filter(x => x%2==0)
      val rdd4 = rdd3.reduce((x,y) => x+y)
      println(rdd4)
      //val df = spark.read.format("csv").option("header","true").load("file:///C:/DATA/data1.csv")
    val df = spark.read.format("csv").option("header","false").load("file:///C:/DATA/txns")
      df.persist()
      df.show()
      df.printSchema()
      df.select("_c0").show()
      df.select($"_c0",$"_c1").show()
      df.select($"_c0",$"_c1",$"_c2").show()
  */
    val df2 = spark.read.format("csv").option("header","true").load("file:///C:/DATA/txns")
    df2.persist()
    df2.printSchema()
  val aggdf = df2.groupBy("category").agg(sum("amount").cast(DecimalType(18,2)).as("sum"),count("*").as("count")).orderBy("category")
  aggdf.show()
    aggdf.printSchema()
    val df3 = df2.withColumn("spendby",expr("case when spendby = 'credit' then 1 else 0 end"))
      .withColumn("txndate",expr("split(txndate,'-')[2]"))
      .withColumnRenamed("txndate","year")
    df3.show(50)

    //val df = spark.read.json("/home/hadoop/Desktop/Spark/SparkPack_1/SparkObj/src/main/resources/people.json")
    //df.show()
    spark.stop()
  }
}
