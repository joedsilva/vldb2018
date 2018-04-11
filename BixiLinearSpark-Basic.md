# BixiLinearSpark-Basic

In this Scala Spark worflow, we explore the [Montreal Bixi biking data set](https://www.kaggle.com/aubertsigouin/biximtl/data) for the year 2017.
We have additionally enriched this data set with the biking distance/duration available via Google map API as gmdata2017.
Our objective is to predict the "trip duration", given the distance between two stations.

This is a "basic" workflow where the user directly builds the training dataset with minimal to no exploration, an unrealistic, but best case scenario.

---

Let us first create a function with all necessary information to connect to MonetDB through the JDBC driver and retrieve a table as a Spark dataframe.

The SparkSession object has to be created by the user when writing an application, or is already existing if one is using the Spark shell.

```scala
def datasetLoader(spark: SparkSession, name: String) : Dataset[Row] = {
  return spark.read.format("jdbc").option("dbtable", name)
    .option("url", "jdbc:monetdb://localhost:50000/bixi")
    .option("driver", "nl.cwi.monetdb.jdbc.MonetDriver")
    .load()
}

val tripdata2017 = datasetLoader(spark, "sys.tripdata2017")
val stations2017 = datasetLoader(spark, "sys.stations2017")
```

We will not be concerned with trips that started and ended at the same station as those are noise. Also, to weed out any further fluctuations in the input data set, we will limit ourselves to only those station combinations which has at the least 50 trips.

```scala
val freqStations = tripdata2017.filter(col("stscode") =!= col("endscode"))
  .groupBy("stscode", "endscode").agg(count("id").alias("numtrips"))
  .filter(col("numtrips") >= 50)
```

Next we will enrich the trip data set by using the distance information provided by the Google maps' API.

Google also provides its estimated duration for the trip. We will have to see in the end if our trained model is able to predict the trip duration better than google's estimate. So we will also save Google's estimate for the trip duration for that comparison.

```scala
val gmdata2017 = datasetLoader("sys.gmdata2017")
val gtripData = gmdata2017.join(tripdata2017, usingColumns=Seq("stscode", "endscode"))
  .join(freqStations, usingColumns=Seq("stscode", "endscode"))
  .select("id", "duration", "gdistm", "gduration")
  .withColumn("gdistm", col("gdistm").cast("double"))
```

As there are multiple trips between the same stations, many trips will have the same distance. So we want to keep some values of distance apart for testing. For this purpose, we will first get distinct values for distance and then sort it.

```scala
val guniqueTripDist = gtripData.select("gdistm").distinct.sort(asc("gdistm"))
```

We will keep a third of these distances apart for testing and the rest, we will use for training.

```scala
val gsplitTripDist = guniqueTripDist.randomSplit(Array(1, 2), 42)
val gtestTripDist = gsplitTripDist(0)
val gtrainTripDist = gsplitTripDist(1)
```

We will next extract the training data set and normalize its features.

```scala
var gtrainData = gtripData.select("gdistm", "duration")
  .join(gtrainTripDist, usingColumns=Seq("gdistm"))

val gmaxdist = guniqueTripDist.agg(max(col("gdistm"))).head().getDouble(0)
val gmaxduration = gtripData.agg(max(col("duration"))).head().getInt(0)
gtrainData = gtrainData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
```

Our linear regression equation is of the form:
`dur = a + b * dist`

We will re-organize the training data set to fit this format and also setup our initial parameters for a and b.

```scala
val gtrainDataSet = gtrainData.select("gdistm").withColumn("x0", lit(1)).select("x0", "gdistm")
val gtrainDataSetDuration = gtrainData.select("duration")
var gparams = Seq(1.0).toDF("a").withColumn("b", lit(1.0))
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

val gtrainDataSetMat = dataframeToMatrix(gtrainDataSet)
var gparamsMat = dataframeToMatrix(gparams)
var gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
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
val gtrainDataSetDurationMat = dataframeToMatrix(gtrainDataSetDuration)
var gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
println(gsqerr)
```

```
0.5429223807597637
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

var gupdate = gradDesc(gtrainDataSetDurationMat, gpred, gtrainDataSetMat)
gparams = gparams.select(col("a") - alpha * gupdate(0) as "a",
  col("b") - alpha * gupdate(1) as "b")
gparams.show(1)
```

```
+------------------+------------------+
|                 a|                 b|
+------------------+------------------+
|0.8960660439052575|0.9863525802432211|
+------------------+------------------+
```

Now let us try to use the updated params to train the model again and see if the error is decreasing.

```scala
gparamsMat = dataframeToMatrix(gparams)
gpred = gtrainDataSetMat.multiply(gparamsMat.transpose)
gsqerr = squaredErr(gtrainDataSetDurationMat, gpred)
println(gsqerr)
```

```
0.43862179637912924
```

This is good our error rate is decreasing with iteration. Hopefully this will help us construct the right parameters.

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
var gtestData = gtripData.select("gdistm", "duration")
  .join(gtestTripDist, usingColumns=Seq("gdistm"))
gtestData = gtestData.select(col("gdistm") / gmaxdist as "gdistm", col("duration") / gmaxduration as "duration")
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
