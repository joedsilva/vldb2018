# BixiLinearSpark

In this Scala Spark worflow, we explore the [Montreal Bixi biking data set](https://www.kaggle.com/aubertsigouin/biximtl/data) for the year 2017.
We have additionally enriched this data set with the biking distance/duration available via Google map API as gmdata2017.
Our objective is to predict the "trip duration", given the distance between two stations.

---

Let us first create a function with all necessary information to connect to MonetDB through the JDBC driver and retrieve a table as a Spark dataframe.

The SparkSession object has to be created by the user when writing an application, or is already existing if one is using the Spark shell.

We can have a peek into the table tripdata2017. We can see the attributes and explore some sample data.
Further we can also use the describe() function to look at the data distribution characteristics.

```scala
def datasetLoader(spark: SparkSession, name: String) : Dataset[Row] = {
  return spark.read.format("jdbc").option("dbtable", name)
    .option("url", "jdbc:monetdb://cerberus:50000/bixi")
    .option("driver", "nl.cwi.monetdb.jdbc.MonetDriver")
    .load()
}

val tripdata2017 = datasetLoader(spark, "sys.tripdata2017")
tripdata2017.show(5)
tripdata2017.describe().show()
```

```
+---+-------------------+-------+-------------------+--------+--------+--------+
| id|            starttm|stscode|              endtm|endscode|duration|ismember|
+---+-------------------+-------+-------------------+--------+--------+--------+
|  0|2017-04-15 00:00:00|   7060|2017-04-15 00:31:00|    7060|    1841|       1|
|  1|2017-04-15 00:01:00|   6173|2017-04-15 00:10:00|    6173|     553|       1|
|  2|2017-04-15 00:01:00|   6203|2017-04-15 00:04:00|    6204|     195|       1|
|  3|2017-04-15 00:01:00|   6104|2017-04-15 00:06:00|    6114|     285|       1|
|  4|2017-04-15 00:01:00|   6174|2017-04-15 00:11:00|    6174|     569|       1|
+---+-------------------+-------+-------------------+--------+--------+--------+

+-------+------------------+-----------------+-----------------+-----------------+------------------+
|summary|                id|          stscode|         endscode|         duration|          ismember|
+-------+------------------+-----------------+-----------------+-----------------+------------------+
|  count|           4018722|          4018722|          4018722|          4018722|           4018722|
|   mean|         2009360.5| 6324.81538434358|6319.866031041709|837.4504471819648|0.7992535935553641|
| stddev|1160105.2585866952|375.8616766245094|383.2968772439043|657.7147213640399|0.4005587680592605|
|    min|                 0|             5002|                0|               61|                 0|
|    max|           4018721|            10002|            10002|             7199|                 1|
+-------+------------------+-----------------+-----------------+-----------------+------------------+
```

So we have 4 million + records in tripdata2017. Also, the station codes are labels. We may have to enrich this information. Let us take a look at the contents of stations2017.

```scala
val stations2017 = datasetLoader(spark, "sys.stations2017")
stations2017.show(5)
stations2017.describe().show()
```

```
+-----+--------------------+-----------------+------------------+---------+
|scode|               sname|        slatitude|        slongitude|sispublic|
+-----+--------------------+-----------------+------------------+---------+
| 7060|de l'Église / de ...|45.46300108733155|-73.57156895217486|        1|
| 6173|    Berri / Cherrier|45.51908844137639|-73.56950908899307|        1|
| 6203|Hutchison / Sherb...|         45.50781|         -73.57208|        1|
| 6204|   Milton / Durocher|       45.5081439|      -73.57477158|        1|
| 6104|Wolfe / René-Léve...|45.51681750463149|    -73.5541883111|        1|
+-----+--------------------+-----------------+------------------+---------+

+-------+------------------+--------------------+--------------------+--------------------+-------------------+
|summary|             scode|               sname|           slatitude|          slongitude|          sispublic|
+-------+------------------+--------------------+--------------------+--------------------+-------------------+
|  count|               546|                 546|                 546|                 546|                546|
|   mean|  6412.74358974359|                null|   45.51910910494543|   -73.5826223650175| 0.9835164835164835|
| stddev|405.39612545856306|                null|0.027918925794717744|0.027343746575568602|0.12744236583071217|
|    min|              5002|10e Avenue / Rose...|   45.43074022417498|  -73.67063373327255|                  0|
|    max|             10002|Émile-Journault /...|  45.582757156033914|  -73.49506705999374|                  1|
+-------+------------------+--------------------+--------------------+--------------------+-------------------+
```

This is good, we have the longitude and latitude associated with each station, which can be used to enrich the tripdata.

Since we have 546 stations, this gives the possibility of 546 x 546 = 298116 possible scenarios for trips. However, we need not be concerned with trips that started and ended at the same station as those are noise. Also, to weed out any further fluctuations in the input data set, we will limit ourselves to only those station combinations which has at the least 50 trips.

```scala
val freqStations = tripdata2017.filter(col("stscode") =!= col("endscode"))
  .groupBy("stscode", "endscode").agg(count("id").alias("numtrips"))
  .filter(col("numtrips") >= 50)
freqStations.show(5)
freqStations.describe().show()
```

```
+-------+--------+--------+
|stscode|endscode|numtrips|
+-------+--------+--------+
|   6411|    6206|     762|
|   6154|    6152|     133|
|   6196|    6280|      64|
|   6271|    6199|     157|
|   6127|    6321|     154|
+-------+--------+--------+

+-------+------------------+------------------+------------------+
|summary|           stscode|          endscode|          numtrips|
+-------+------------------+------------------+------------------+
|  count|             19300|             19300|             19300|
|   mean|6302.1273575129535|6291.7812953367875|116.90585492227979|
| stddev|349.88075359412693|351.21744777844117|117.25065079384676|
|    min|              5002|              5002|                50|
|    max|             10002|             10002|              2200|
+-------+------------------+------------------+------------------+
```

We can see that there are 19,300 station combinations that is of interest to us. Next stop, we need to include the longitude and latitude information of the start and end stations.

This can be done by joining with the station information.


```scala
val freqStationsCord = freqStations.join(stations2017, col("stscode") === col("scode"))
  .withColumnRenamed("slatitude", "stlat").withColumnRenamed("slongitude", "stlong")
  .drop("sispublic").drop("scode").drop("sname")
  .join(stations2017, col("endscode") === col("scode"))
  .withColumnRenamed("slatitude", "enlat").withColumnRenamed("slongitude", "enlong")
  .drop("sispublic").drop("scode").drop("sname")
freqStationsCord.show(5)
```

```
+-------+--------+--------+-----------+------------+-----------------+-----------------+
|stscode|endscode|numtrips|      stlat|      stlong|            enlat|           enlong|
+-------+--------+--------+-----------+------------+-----------------+-----------------+
|   6127|    6336|      59|  45.537877|  -73.618323|45.54279435304991|-73.6181053519249|
|   6029|    6336|      77|  45.482779|-73.58452544|45.54279435304991|-73.6181053519249|
|   6344|    6336|      53|  45.535189|  -73.617459|45.54279435304991|-73.6181053519249|
|   6910|    6336|     113|  45.537041|  -73.602026|45.54279435304991|-73.6181053519249|
|   6327|    6336|      50|45.53985704|-73.62038255|45.54279435304991|-73.6181053519249|
+-------+--------+--------+-----------+------------+-----------------+-----------------+
```

It would be easier if we can translate the coordinates to a distance metric. The geographiclib package supports this computation using Vincenty's formula. This provides us with a distance as crow flies between two coordiantes. This might be a reasonable approximation of actual distance travelled in a trip.

With a user defined function, we can generate a dataset which also includes this distance metric.


```scala
import net.sf.geographiclib.Geodesic
val geoDistance = (lat1: Double, lon1: Double, lat2: Double, lon2: Double)
  => Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2).s12
val geoDistanceUDF = udf(geoDistance)
val freqStationsDist = freqStationsCord.withColumn("vdistm",
    round(geoDistanceUDF(col("stlat"), col("stlong"), col("enlat"), col("enlong"))))
freqStationsDist.show(5)
```

```
+-------+--------+--------+-----------+------------+-----------------+-----------------+------+
|stscode|endscode|numtrips|      stlat|      stlong|            enlat|           enlong|vdistm|
+-------+--------+--------+-----------+------------+-----------------+-----------------+------+
|   6127|    6336|      59|  45.537877|  -73.618323|45.54279435304991|-73.6181053519249| 547.0|
|   6029|    6336|      77|  45.482779|-73.58452544|45.54279435304991|-73.6181053519249|7168.0|
|   6344|    6336|      53|  45.535189|  -73.617459|45.54279435304991|-73.6181053519249| 847.0|
|   6910|    6336|     113|  45.537041|  -73.602026|45.54279435304991|-73.6181053519249|1409.0|
|   6327|    6336|      50|45.53985704|-73.62038255|45.54279435304991|-73.6181053519249| 372.0|
+-------+--------+--------+-----------+------------+-----------------+-----------------+------+
```

We can next enrich our trip data set with the distance information by joining these computed distances with each trip.

```scala
val tripData = tripdata2017.join(freqStationsDist, usingColumns=Seq("stscode", "endscode"))
  .select("id", "duration", "vdistm")
tripData.show(5)
tripData.describe().show()
```

```
+------+--------+------+
|    id|duration|vdistm|
+------+--------+------+
|147500|     439|1152.0|
|196572|     322|1152.0|
|345948|     382|1152.0|
|378487|     350|1152.0|
|584852|     472|1152.0|
+------+--------+------+

+-------+------------------+-----------------+------------------+
|summary|                id|         duration|            vdistm|
+-------+------------------+-----------------+------------------+
|  count|           2256283|          2256283|           2256283|
|   mean|2003455.0317952137|630.4498819518651|1370.3965291587979|
| stddev| 1162662.504311024|533.4432445098437| 921.2912258857563|
|    min|                 2|               61|              48.0|
|    max|           4018721|             7199|            9075.0|
+-------+------------------+-----------------+------------------+
```

So we have trip duration for each trip and the distance as crow flies, between the two stations involved in the trip.

Also, we have about 2 million trips for which we have distance between stations metric. Given that there are only a few thousand unique values for distance, we might want to keep some values of distance apart for testing. For this purpose, we will first get distinct values for distance and then sort it.


```scala
val uniqueTripDist = tripData.select("vdistm").distinct.sort(asc("vdistm"))
uniqueTripDist.show(5)
uniqueTripDist.describe().show()
```

```
+------+
|vdistm|
+------+
|  48.0|
|  72.0|
|  81.0|
|  85.0|
|  89.0|
+------+

+-------+------------------+
|summary|            vdistm|
+-------+------------------+
|  count|              3638|
|   mean|  2200.99175371083|
| stddev|1391.3941795635976|
|    min|              48.0|
|    max|            9075.0|
+-------+------------------+
```

We will keep some data apart for testing. We can achieve that with a random split in Spark. We set apart 33%, accross the entire range of distance values. We use 42 as seed for the PRNG.

```scala
val splitTripDist = uniqueTripDist.randomSplit(Array(1, 2), 42)
val testTripDist = splitTripDist(0)
testTripDist.show(5)
testTripDist.describe().show()
```

```
+------+
|vdistm|
+------+
| 110.0|
| 121.0|
| 125.0|
| 132.0|
| 141.0|
+------+

+-------+------------------+
|summary|            vdistm|
+-------+------------------+
|  count|              1157|
|   mean| 2139.840103716508|
| stddev|1394.5676096327018|
|    min|             110.0|
|    max|            9031.0|
+-------+------------------+
```

Now let us get the remaining values for distances to be used for training.

```scala
val trainTripDist = splitTripDist(1)
trainTripDist.show(5)
trainTripDist.describe().show()
```

```
+------+
|vdistm|
+------+
|  48.0|
|  72.0|
|  81.0|
|  85.0|
|  89.0|
+------+

+-------+-----------------+
|summary|           vdistm|
+-------+-----------------+
|  count|             2481|
|   mean|2229.509471987102|
| stddev|1389.272878109179|
|    min|             48.0|
|    max|           9075.0|
+-------+-----------------+
```

Let us now extract the fields of interest to us for the training data, which is just the distance of each trip and it's duration

```scala
var trainData = tripData.select("vdistm", "duration")
  .join(trainTripDist, usingColumns=Seq("vdistm"))
trainData.show(5)
trainData.describe().show()
```

```
+------+--------+
|vdistm|duration|
+------+--------+
| 299.0|     159|
| 299.0|      93|
| 299.0|     106|
| 299.0|     114|
| 299.0|      98|
+------+--------+

+-------+------------------+-----------------+
|summary|            vdistm|         duration|
+-------+------------------+-----------------+
|  count|           1546581|          1546581|
|   mean|1399.3084655766495|646.0634076068437|
| stddev| 936.4663159077667|545.4760271814907|
|    min|              48.0|               61|
|    max|            9075.0|             7199|
+-------+------------------+-----------------+
```

As the values are huge, we should normalize the data attributes. First get the max values for these attributes.

```scala
val maxdist = uniqueTripDist.agg(max(col("vdistm"))).head().getDouble(0)
println(maxdist)
val maxduration = tripData.agg(max(col("duration"))).head().getInt(0)
println(maxduration)
```

```
9075.0
7199
```

Now let us normalize the training data.

```scala
trainData = trainData.select(col("vdistm") / maxdist as "vdistm", col("duration") / maxduration as "duration")
trainData.show(5)
trainData.describe().show()
```

```
+-------------------+--------------------+
|             vdistm|            duration|
+-------------------+--------------------+
|0.03294765840220386|0.022086400889012363|
|0.03294765840220386|0.012918460897346854|
|0.03294765840220386|0.014724267259341575|
|0.03294765840220386|0.015835532712876788|
|0.03294765840220386|0.013613001805806362|
+-------------------+--------------------+

+-------+--------------------+-------------------+
|summary|              vdistm|           duration|
+-------+--------------------+-------------------+
|  count|             1546581|            1546581|
|   mean|  0.1541937703114761| 0.0897434932083405|
| stddev|  0.1031918805408007|0.07577108309230315|
|    min|0.005289256198347108|  0.008473399083206|
|    max|                 1.0|                1.0|
+-------+--------------------+-------------------+
```

Our linear regression equation is of the form:
`dur = a + b * dist`

We will re-organize the training data set to fit this format and also setup our initial parameters for a and b.

```scala
val trainDataSet = trainData.select("vdistm").withColumn("x0", lit(1)).select("x0", "vdistm")
trainDataSet.show(5)
val trainDataSetDuration = trainData.select("duration")
trainDataSetDuration.show(5)
var params = Seq(1.0).toDF("a").withColumn("b", lit(1.0))
params.show(1)
```

```
+---+-------------------+
| x0|             vdistm|
+---+-------------------+
|  1|0.03294765840220386|
|  1|0.03294765840220386|
|  1|0.03294765840220386|
|  1|0.03294765840220386|
|  1|0.03294765840220386|
+---+-------------------+

+--------------------+
|            duration|
+--------------------+
|0.022086400889012363|
|0.012918460897346854|
|0.014724267259341575|
|0.015835532712876788|
|0.013613001805806362|
+--------------------+

+---+---+
|  a|  b|
+---+---+
|1.0|1.0|
+---+---+
```

Let us try to run a prediction using these parameters.
We have first to convert our dataframes into matrices to perform some multiplication and use the transpose operator.

We use the distributed matrices available in Spark. They are still based on the RDD API though. Once the Spark ML lib is fully updated and based on the new Dataframe API, the conversion below should become faster.

```scala
def dataframeToMatrix(df: Dataset[Row]) : BlockMatrix = {
  val assembler = new VectorAssembler().setInputCols(df.columns).setOutputCol("vector")
  val df2 = assembler.transform(df)
  return new IndexedRowMatrix(df2.select("vector").rdd.map{
    case Row(v: Vector) => Vectors.fromML(v)
  }.zipWithIndex.map { case (v, i) => IndexedRow(i, v) }).toBlockMatrix()
}

val trainDataSetMat = dataframeToMatrix(trainDataSet)
var paramsMat = dataframeToMatrix(params)
var pred = trainDataSetMat.multiply(paramsMat.transpose)
```

We need to compute the squared error for the predictions. Since we will be reusing them, we might as well store it as a function.

```scala
def squaredErr(actual: BlockMatrix, predicted: BlockMatrix) : Double = {
  var s: Double = 0
  val it = actual.subtract(predicted).toLocalMatrix().rowIter
  while (it.hasNext) {
    s += scala.math.pow(it.next.apply(0), 2)
  }
  return s / (2 * actual.numRows())
}
```

Let us see what is the error for the first iteration.

```scala
val trainDataSetDurationMat = dataframeToMatrix(trainDataSetDuration)
var sqerr = squaredErr(trainDataSetDurationMat, pred)
println(sqerr)
```

```
0.5705374176397783
```

We need to perform a gradient descent based on the squared errors. We will write another function to perform this.

```scala
def gradDesc(actual: BlockMatrix, predicted: BlockMatrix,
             indata: BlockMatrix) : Seq[Double] = {
  val m = predicted.subtract(actual).transpose.multiply(indata).toLocalMatrix()
  val n = actual.numRows()
  return Seq(m.apply(0, 0) / n, m.apply(0, 1) / n)
}
```

Let us update our params using gradient descent using the error we got. We also need to use a learning rate, alpha (arbitrarily chosen).

```scala
val alpha = 0.1

val update = gradDesc(trainDataSetDurationMat, pred, trainDataSetMat)
params = params.select(col("a") - alpha * update(0) as "a",
  col("b") - alpha * update(1) as "b")
params.show(1)
```

```
+------------------+------------------+
|                 a|                 b|
+------------------+------------------+
|0.8935549722896863|0.9829404525035315|
+------------------+------------------+
```

Now let us try to use the updated params to train the model again and see if the error is decreasing.

```scala
paramsMat = dataframeToMatrix(params)
pred = trainDataSetMat.multiply(paramsMat.transpose)
sqerr = squaredErr(trainDataSetDurationMat, pred)
println(sqerr)
```

```
0.46027197905370804
```

Before we proceed, may be we should check if google maps API's distance metric gives a better learning rate. Let us see what fields we can use from Google.

```scala
val gmdata2017 = datasetLoader(spark, "sys.gmdata2017")
gmdata2017.show(5)
gmdata2017.describe().show()
```

```
+-------+--------+------+---------+
|stscode|endscode|gdistm|gduration|
+-------+--------+------+---------+
|   6406|    6052|  3568|      596|
|   6050|    6406|  3821|      704|
|   6148|    6173|  1078|      293|
|   6110|    6114|  1319|      337|
|   6123|    6114|   725|      177|
+-------+--------+------+---------+

+-------+-----------------+-----------------+------------------+------------------+
|summary|          stscode|         endscode|            gdistm|         gduration|
+-------+-----------------+-----------------+------------------+------------------+
|  count|            19516|            19516|             19516|             19516|
|   mean|6302.622361139578|6291.736728837876| 2079.293092846895|  516.190869030539|
| stddev|350.3058187956947|350.4692481410197|1345.6292794069195|299.19197733856276|
|    min|             5002|             5002|                18|                 4|
|    max|            10002|            10002|             14530|              3083|
+-------+-----------------+-----------------+------------------+------------------+
```

We can build a new data set for the trips between frequently used station combination that includes google's distance.

```scala
val gtripData = gmdata2017.join(tripdata2017, usingColumns=Seq("stscode", "endscode"))
  .join(freqStationsCord, usingColumns=Seq("stscode", "endscode"))
  .select("id", "duration", "gdistm", "gduration")
  .withColumn("gdistm", col("gdistm").cast("double"))
gtripData.show(5)
gtripData.describe().show()
```

```
+------+--------+------+---------+
|    id|duration|gdistm|gduration|
+------+--------+------+---------+
|147500|     439|1543.0|      355|
|196572|     322|1543.0|      355|
|345948|     382|1543.0|      355|
|378487|     350|1543.0|      355|
|584852|     472|1543.0|      355|
+------+--------+------+---------+

+-------+------------------+-----------------+------------------+-----------------+
|summary|                id|         duration|            gdistm|        gduration|
+-------+------------------+-----------------+------------------+-----------------+
|  count|           2256283|          2256283|           2256283|          2256283|
|   mean|2003455.0317952137|630.4498819518651|1834.7316307395836|454.6804899917253|
| stddev| 1162662.504311024|533.4432445098437|1236.2586594809306|273.3622012352504|
|    min|                 2|               61|              48.0|               11|
|    max|           4018721|             7199|           14530.0|             3083|
+-------+------------------+-----------------+------------------+-----------------+
```

Google also provides its estimated duration for the trip. We will have to see in the end if our trained model is able to predict the trip duration better than google's estimate. So we will also save Google's estimate for the trip duration for that comparison.

Next up, we need to format this dataset the same way we did the first one.


```scala
val guniqueTripDist = gtripData.select("gdistm").distinct.sort(asc("gdistm"))
val gsplitTripDist = guniqueTripDist.randomSplit(Array(1, 2), 42)
val gtestTripDist = gsplitTripDist(0)
val gtrainTripDist = gsplitTripDist(1)
var gtrainData = gtripData.select("gdistm", "duration")
  .join(gtrainTripDist, usingColumns=Seq("gdistm"))

val gmaxdist = guniqueTripDist.agg(max(col("gdistm"))).head().getDouble(0)
println(gmaxdist)
val gmaxduration = gtripData.agg(max(col("duration"))).head().getInt(0)
println(gmaxduration)
gtrainData = gtrainData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
```

```
14530.0
7199
```

Let us see how the error rate is progressing for the new dataset.

```scala
val gtrainDataSet = gtrainData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
val gtrainDataSetDuration = gtrainData.select("duration")
var gparams = Seq(1.0).toDF("a").withColumn("b", lit(1.0))

val gtrainDataSetMat = dataframeToMatrix(gtrainDataSet)
var gparamsMat = dataframeToMatrix(gparams)
var gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
val gtrainDataSetDurationMat = dataframeToMatrix(gtrainDataSetDuration)
var gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
println(gsqerr)
val gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
  col("b") - alpha * gupdate(1) as "b")
gparamsMat = dataframeToMatrix(gparams)
gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
println(gsqerr)
```

```
0.5429223807597637
0.43862179637912924
```

It looks like using Google maps' distance is giving us a slight advantage. That makes sense, since Vincenty's formula computes distances as a crow flies, where as Google maps' distance metric is based on the actual road network distances. Better data gives better prediction results !

We are done with the feature selection and feature engineering phase for now.

Next we will proceed to train our linear regression model using the training data set.

Meanwhile, we will also let it printout the error rate at frequent intervals so that we know it is decreasing.

Since gtrainDataSetMat and gtrainDataSetDurationMat are never modified and are used at each iteration, let us put them in cache to speed up the computations.

```scala
gtrainDataSetMat.cache()
gtrainDataSetDurationMat.cache()

for (i <- 0 to 999) {
  val gparamsMat = dataframeToMatrix(gparams)
  gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
  val gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
  gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
    col("b") - alpha * gupdate(1) as "b")
  if ((i+1)%100 == 0) {
    println(s"Error rate after ${i+1} iterations is ${squaredErr(gtrainDataSetDurationMat, gpred)}")
  }
}

gparams.show(1)
gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
println(gsqerr)
```

```
Error rate after 100 iterations is 0.00232543404206751
Error rate after 200 iterations is 0.0022674829107455045
Error rate after 300 iterations is 0.0022171460273500703
Error rate after 400 iterations is 0.0021734227498817196
Error rate after 500 iterations is 0.0021354441373096704
Error rate after 600 iterations is 0.0021024554160513575
Error rate after 700 iterations is 0.002073800979868345
Error rate after 800 iterations is 0.0020489113605793876
Error rate after 900 iterations is 0.0020272919106481533
Error rate after 1000 iterations is 0.0020085129727347314

+--------------------+------------------+
|                   a|                 b|
+--------------------+------------------+
|0.001674344318843045|0.6777788115855243|
+--------------------+------------------+

0.0020085129727347314
```

Let us see how our model performs in predictions against the test data set we had kept apart.

```scala
val gtestData = gtripData.select("gdistm", "duration")
  .join(gtestTripDist, usingColumns=Seq("gdistm"))
val gtestDataNormalized = gtestData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
val gtestDataSet = gtestData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
val gtestDataSetDuration = gtestData.select("duration")

val gtestDataSetDurationMat = dataframeToMatrix(gtestDataSetDuration)
val gtestDataSetMat = dataframeToMatrix(gtestDataSet)
gparamsMat = dataframeToMatrix(gparams)
val gtestpred = gtestDataSetMat.multiply(gparamsMat.transpose)

val gdurationMat = dataframeToMatrix(Seq(gmaxduration).toDF("duration"))
val gtestsqerr1 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gtestpred.multiply(gdurationMat))
println(gtestsqerr1)
```

```
107263.30226709295
```

We would also like to check how the duration provided by Google maps' API hold up to the test data set.

```scala
val gdurationDataMat = dataframeToMatrix(gtripData.join(gtestTripDist, usingColumns=Seq("gdistm")).select("gduration"))
val gtestsqerr2 = squaredErr(gtestDataSetDurationMat.multiply(gdurationMat), gdurationDataMat)
println(gtestsqerr2)
```

```
118049.31241827628
```

So yes, our model is able to do a better job.
