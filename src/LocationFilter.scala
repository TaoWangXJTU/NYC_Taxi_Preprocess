import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.util.sketch.BloomFilter
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


//case class pointandtime(x: Double, y: Double, datetime: String)
//case class Params(input:String=null, output:String=null, dateString:String=null, maxHour:Int=0)

object LocationFilter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Data Preprocessing")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.kryoserializer.buffer.max","1g")
      //      .config("spark.driver.maxResultSize", "10g")
      //      .config("spark.driver.memory", "100g")
      .config("spark.memory.fraction", "0.75")
      //      .config("spark.default.parallelism", "20")
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .getOrCreate()
    val sc = spark.sparkContext
    val start = System.nanoTime()
    import spark.implicits._

    val path:String="/home/iotdatalab/WFC/dataset/taxi/2009/"
    val output:String="/home/iotdatalab/WFC/dataset/filtered_taxi/2009/"
    val year: Int = 2009
    val minMonth: Int = 1
    val maxMonth: Int = 12
    val headers: Array[String] = Array("Start_Lon","Start_Lat","Trip_Pickup_DateTime")

    val ff = new FilterFunctions()
    // Data are filtered by location
    val month_limited_data = ff.locationFilterByMonth(spark, path, minMonth, maxMonth, headers)
    val outDir = s"${year}_Month${minMonth}-${maxMonth}"
    month_limited_data.repartition(1).write.csv(s"$output/${outDir}")

    val end = System.nanoTime()
    println(s"\nTime consumed: ${(end - start) / 1e9} seconds.\n")

    sc.stop()
  }
}
