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

object DensityFilter {
  def main(args: Array[String]): Unit = {
    //    val TAG: String = {
    //      val format = new SimpleDateFormat("yyMMdd-HHmmss")
    //      val time = format.format(new Date())
    //      //      s"output/${time}"
    //      s"${time}"
    //    }.

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

    val fileName:String="/home/iotdatalab/WFC/dataset/filtered_taxi/2009-Month1-12/part-00000"
    val output:String="densityFilteredData.csv"
    var step:Int = 4
    var threshold: Int = 50

    val ff = new FilterFunctions()
    // Data are filtered by grid density
    val filtered_Data = ff.densityFilter(spark, fileName, step, threshold)
    filtered_Data.repartition(1).write.csv(s"${output}/filtered_step=${step}-threshold=${threshold}")

    val end = System.nanoTime()
    println(s"\nTime consumed: ${(end - start) / 1e9} seconds.\n")

    sc.stop()
  }
}
