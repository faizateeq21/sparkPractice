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
  import org.apache.hadoop.fs._
  import java.io.File
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
  import org.apache.mesos.protobuf.compiler.PluginProtos.CodeGeneratorResponse
  import org.apache.mesos.protobuf.compiler.PluginProtos.CodeGeneratorResponse.File
  object awsObj {
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("SparkFirst").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val spark = SparkSession.builder()

        .config("fs.s3a.access.key", "AKIAQJ35YRX5DB4UBB6E")

        .config("fs.s3a.secret.key", "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")


        .getOrCreate()

      import spark.implicits._


      val df = spark.read.format("json")
        .load("s3a://zeyonifibucket/data/devices.json")

      // Code to change the name of the output File from PArt--00 to your desired name

      df.write.format("csv").mode("overwrite").save("C:/DATA/IntelliJ_Data/AWS_Data")
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val file = fs.globStatus(new Path("C:/DATA/IntelliJ_Data/AWS_Data/part*"))(0).getPath().getName()

      fs.rename(new Path("C:/DATA/IntelliJ_Data/AWS_Data/" + file), new Path("C:/DATA/IntelliJ_Data/AWS_Data/mydata.csv"))
      fs.delete(new Path("mydata.csv-temp"), true)

      df.write.format("csv").save("s3a://zeyonifibucket/vishal_dir")
    //  df.write.format("csv").save("C:/DATA/IntelliJ_Data/AWS_Data")
/*
      // Copy the actual file from Directory and Renames to custom name
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)

      val srcPath=new Path("C:/DATA/IntelliJ_Data/AWS_Data")
      val destPath= new Path("C:/DATA/IntelliJ_Data/address_merged.csv")
      val srcFile=FileUtil.listFiles(new File("c:/tmp/address"))
      val srcFile=FileUtil.listFiles(new File("C:/DATA/IntelliJ_Data/AWS_Data"))
        //filterNot(f=>f.getPath.endsWith(".csv")))
         .filterNot(f=>f.getPath.endsWith(".csv"))(0)
      //Copy the CSV file outside of Directory and rename to desired file name
      FileUtil.copy(srcFile,hdfs,destPath,true,hadoopConfig)
      //Removes CRC File that create from above statement
      hdfs.delete(new Path(".address_merged.csv.crc"),true)
      //Remove Directory created by df.write()
      hdfs.delete(srcPath,true)
*/

      /*val outputFile = "s3a://zeyonifibucket/data/devices.json"
val hadoopConf = new org.apache.hadoop.conf.Configuration()
val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(outputFile), hadoopConf)
val path = new org.apache.hadoop.fs.Path(outputFile)
val fileStatus = hdfs.getFileStatus(path)
val filePath = fileStatus.getPath()
val fileName = filePath.getName()
val newFileName = fileName.replace("Part--00", "Part--01")
val newFilePath = filePath.getParent().toString() + "/" + newFileName
val newFile = new org.apache.hadoop.fs.Path(newFilePath)
hdfs.rename(filePath, newFile)
*/

      println("done")

      spark.stop()
    }
  }