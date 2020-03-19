# Pre-processing of the NYC TLC Trip Record Data

The pre-processing program consists of two stages:     
- First, the data is filtered by the geographic location range where the passengers boarded the car, 
which needs ranging in (40.5°,40.9°)N and (73.6°~74.2°)W. 
- Second, in order to better visualize traffic trajectories and urban traffic routes, 
we need to correct systematic errors by deleting some data because they are not in the right location. 
For this purpose, we map the data to geographic (latitude and longitude) grids with a resolution of 0.0001° 
and filter those grids whose density need to be larger than 50 points. 

In this experiment, we chose yellow taxi records from 2009-01-01 to 2015-12-31.
And then we split all the dataset into three parts:     

a. from 2009-01-01 to 2012-02-29    
b. from 2012-03-01 to 2013-12-31    
c. from 2014-01-01 to 2015-12-31       

We filtered each part of data set with two pre-processing stages, and then merged them together into the final data set.

## How to use this program
We supply filtering methods in pre-processing. The program is implemented in Scala with Spark.
The example of pre-processing dataset of 2009 yellow taxi records is as bellow:
### stage 1
```scala
//header example:
//2009 yellow : Array("Trip_Pickup_DateTime","Start_Lon","Start_Lat");
//2010-2013 yellow: Array("pickup_datetime","pickup_longitude","pickup_latitude");
//2014 yellow: Array(" pickup_datetime"," pickup_longitude"," pickup_latitude");
//2015+ yellow: Array("tpep_pickup_datetime","pickup_longitude","pickup_latitude");

val path:String="/home/iotdatalab/WFC/dataset/taxi/2009/"
val output:String="/home/iotdatalab/WFC/dataset/filtered_taxi/2009/"
val year: Int = 2009
val minMonth: Int = 1
val maxMonth: Int = 12
val headers: Array[String] = Array("Start_Lon","Start_Lat","Trip_Pickup_DateTime")
val ff = new FilterFunctions()
val month_limited_data = ff.locationFilterByMonth(spark, path, minMonth, maxMonth, headers)
val outDir = s"${year}_Month${minMonth}-${maxMonth}"
month_limited_data.repartition(1).write.csv(s"$output/${outDir}")

```
### stage 2
```scala
val fileName:String="/home/iotdatalab/WFC/dataset/filtered_taxi/2009-Month1-12/part-00000"
val output:String="path/to/output/densityFilteredData"
var step:Int = 4
var threshold: Int = 50
val ff = new FilterFunctions()
val filtered_Data = ff.densityFilter(sparkSession, fileName, step, threshold)
filtered_Data.repartition(1).write.csv(s"${output}/filtered_step=${step}-threshold=${threshold}/")
```

### Sorting data in ascending order by datetime

```scala
val output:String="path/to/output/densityFilteredData"
val fileName:String=s"${output}/filtered_step=${step}-threshold=${threshold}/part-00000"
val ff = new FilterFunctions()
val sortedData = ff.sortDataByDateTime(sparkSession, fileName)
// every row of sortedData has (parsed-datetime, location-string)
sortedData.map(a => a._1 + "," + a._2).saveAsTextFile(s"${output}/sortedByTime_with_time/")
// if datetime is not required, you can use this:
sortedData.map(_._2).saveAsTextFile(s"${output}/sortedByTime_without_time")
```