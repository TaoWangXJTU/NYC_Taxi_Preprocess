import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.sketch.BloomFilter

case class pointTimeCode(x: Double, y: Double, datetime: String, code: String)
class FilterFunctions {

  def sampleBySingleFile(spark: SparkSession, fileName: String, date:String, maxHour:Int) = {
    import spark.implicits._
    //2009 yellow: "Trip_Pickup_DateTime","Start_Lon","Start_Lat";
    //2010-2013 yellow: "pickup_datetime","pickup_longitude","pickup_latitude"
    //2014 yellow: " pickup_datetime"," pickup_longitude"," pickup_latitude"
    //2015+ yellow: "tpep_pickup_datetime","pickup_longitude","pickup_latitude"
    val data = spark.read.option("header", true).format("csv").load(fileName)
      .select(
        $"Start_Lon".alias("longitude").cast("Double"),
        $"Start_Lat".alias("latitude").cast("Double"),
        $"Trip_Pickup_DateTime".alias("pickup_time").cast("String"))
    val temp = data
      .filter($"longitude".gt(-74.2) && $"longitude".lt(-73.6))
      .filter($"latitude".gt(40.5) && $"latitude".lt(40.9))
    val date_limited_data = temp.map(row => (row.getDouble(0), row.getDouble(1), row.getString(2)))
      .filter(datetime => datetime._3.split(" ")(0) == date)
      .filter(datetime => datetime._3.split(" ")(1).split(":")(0).toInt < maxHour)
      .map(row => (row._1, row._2))
    date_limited_data
  }

  def locationFilterByMonth(spark: SparkSession, fileDir: String, minMonth: Int, maxMonth:Int, headers: Array[String]) = {
    import spark.implicits._
    //2009 yellow: "Trip_Pickup_DateTime","Start_Lon","Start_Lat";
    //2010-2013 yellow: "pickup_datetime","pickup_longitude","pickup_latitude";
    //2014 yellow: " pickup_datetime"," pickup_longitude"," pickup_latitude";
    //2015+ yellow: "tpep_pickup_datetime","pickup_longitude","pickup_latitude";
    val fileList = new File(fileDir).listFiles().map(_.getName())
    val monthFilterFiles = fileList
      .filter(_.endsWith(".csv"))
      .filter(name => {
        val month = name.stripSuffix(".csv").split("-")(1).toInt
        month >= minMonth && month <= maxMonth
      } )
      .map(fileDir + "/" + _)
    monthFilterFiles.foreach(println(_))
    val data = monthFilterFiles.par.map(file => {
      spark.read.option("header", true).format("csv").load(file)
    }).reduce {
      _ union _
    }.select(
//      $"pickup_longitude".alias("longitude").cast("Double"),
//      $"pickup_latitude".alias("latitude").cast("Double"),
//      $"pickup_datetime".alias("pickup_time").cast("String"))
      $"${headers(0)}".alias("longitude").cast("Double"),
      $"${headers(1)}".alias("latitude").cast("Double"),
      $"${headers(2)}".alias("pickup_time").cast("String"))

    val temp = data
      .filter($"longitude".gt(-74.2) && $"longitude".lt(-73.6))
      .filter($"latitude".gt(40.5) && $"latitude".lt(40.9))
    temp

  }

  def densityFilter(spark: SparkSession, fileName: String, step: Int, threshold: Int) = {

    import spark.implicits._
    val sc = spark.sparkContext

    val schema = StructType(Array(
      StructField("longitude", StringType, true),
      StructField("latitude", StringType, true),
      StructField("pickup_time", StringType, true)
    ))
    val temp: DataFrame = spark.read.option("header", false).schema(schema).format("csv").load(fileName)
    val temp1 = temp
      .select($"longitude".cast("Double"),
        $"latitude".cast("Double"),
        $"pickup_time".cast("String"),
        ($"longitude" / math.pow(10, -step)).alias("longitude_tmp").cast("Int"),
        ($"latitude" / math.pow(10, -step)).alias("latitude_tmp").cast("Int"))
      .map(row => pointTimeCode(row.getDouble(0), row.getDouble(1), row.getString(2), s"${row.getInt(3)}${row.getInt(4)}")).cache()
    val temp2 = temp1.groupBy("code")

    val temp3 = temp2.count().filter($"count".gt(threshold))
    val bf: BloomFilter = temp3.stat.bloomFilter($"code", temp3.count(), 0.000001f)
    val bcBF = sc.broadcast(bf)
    val temp4 = temp1.mapPartitions(iterator => {
      val bloomFilter = bcBF.value
      iterator.filter(x => {
        bloomFilter.mightContain(x.code)
      })
    }).map(row => (row.x, row.y, row.datetime))

    temp4
  }

  def sortDataByDateTime(spark: SparkSession, fileName: String) = {
    import spark.implicits._
    val sc = spark.sparkContext
    val data = sc.textFile(fileName).map(line => {
      val arr = line.split(",")
      (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(2).toString).getTime, arr(0) + "," + arr(1))
    })
    val sortedData = data
      .repartitionAndSortWithinPartitions(new HashPartitioner(1))
    sortedData
  }

}
