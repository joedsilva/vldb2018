This is the database UDF (stored procedure) version of the linear regression workflow on the Bixi dataset.

The user uses both the regular database client to execute SQL statements as well as writes and executes databse UDFs when Python is required to perform linear alegbraic computations. As a result the workflow is an interleaved mix of SQLs and database UDFs.

This often results in the user having to redo some of the computation and transformation already performed in a previous invocation of the UDF as NumPy objects do not survive outside the UDF's life span.

The restrictive nature of UDF also results in having to duplicate some of the source code between different UDFs, such as those required to compute the cost function (squared error) and perform gradient descent.

Let us see what tables we have in the database
```
\dt
```

```
TABLE  bixi.gmdata2017
TABLE  bixi.stations2017
TABLE  bixi.tripdata2017
```

Let us take a peek into tripdata2017.
```SQL
SELECT * FROM tripdata2017 LIMIT 5;
```

```
+------+----------------------+---------+----------------------+----------+----------+----------+
| id   | starttm              | stscode | endtm                | endscode | duration | ismember |
+======+======================+=========+======================+==========+==========+==========+
|    0 | 2017-04-15 00:00:00  |    7060 | 2017-04-15 00:31:00  |     7060 |     1841 |        1 |
|    1 | 2017-04-15 00:01:00  |    6173 | 2017-04-15 00:10:00  |     6173 |      553 |        1 |
|    2 | 2017-04-15 00:01:00  |    6203 | 2017-04-15 00:04:00  |     6204 |      195 |        1 |
|    3 | 2017-04-15 00:01:00  |    6104 | 2017-04-15 00:06:00  |     6114 |      285 |        1 |
|    4 | 2017-04-15 00:01:00  |    6174 | 2017-04-15 00:11:00  |     6174 |      569 |        1 |
+------+----------------------+---------+----------------------+----------+----------+----------+
5 tuples
```

We will look at the statistical distribution of certain fields of interest from the tripdata

```SQL
SELECT
  COUNT(id) AS count_id
 ,COUNT(DISTINCT id) AS countd_id
 ,SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS countn_id
 ,MAX(id) AS max_id
 ,MIN(id) AS min_id
 ,AVG(id) AS avg_id
 ,SYS.MEDIAN(id) AS median_id
 ,SYS.QUANTILE(id, 0.25) AS q25_id
 ,SYS.QUANTILE(id, 0.50) AS q50_id
 ,SYS.QUANTILE(id, 0.75) AS q75_id
 ,SYS.STDDEV_POP(id) AS std_id
 ,COUNT(duration) AS count_duration
 ,COUNT(DISTINCT duration) AS countd_duration
 ,SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) AS countn_duration
 ,MAX(duration) AS max_duration
 ,MIN(duration) AS min_duration
 ,AVG(duration) AS avg_duration
 ,SYS.MEDIAN(duration) AS median_duration
 ,SYS.QUANTILE(duration, 0.25) AS q25_duration
 ,SYS.QUANTILE(duration, 0.50) AS q50_duration
 ,SYS.QUANTILE(duration, 0.75) AS q75_duration
 ,SYS.STDDEV_POP(duration) AS std_duration
FROM tripdata2017;
```

```
+----------+-----------+-----------+---------+--------+--------------------------+-----------+---------+---------+---------+--------------------------+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+
| count_id | countd_id | countn_id | max_id  | min_id | avg_id                   | median_id | q25_id  | q50_id  | q75_id  | std_id                   | count_duration | countd_duration | countn_duration | max_duration | min_duration | avg_duration             | median_duration | q25_duration | q50_duration | q75_duration | std_duration             |
+==========+===========+===========+=========+========+==========================+===========+=========+=========+=========+==========================+================+=================+=================+==============+==============+==========================+=================+==============+==============+==============+==========================+
|  4018721 |   4018721 |         0 | 4018721 |      0 |       2009360.2118134103 |   2009360 | 1004680 | 2009360 | 3014040 |       1160105.1147372632 |        4018721 |            7115 |               0 |         7199 |           61 |        837.4505261748701 |             670 |          382 |          670 |         1121 |        657.7147023007943 |
+----------+-----------+-----------+---------+--------+--------------------------+-----------+---------+---------+---------+--------------------------+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+
1 tuple
```

So we have 4 million + records in tripdata2017. 
Also, the station codes are labels. We may have to enrich this information.
Let us take a look at the contents of stations2017.
```SQL
SELECT * FROM stations2017 LIMIT 5;
```
```
+-------+---------------------------+--------------------------+--------------------------+-----------+
| scode | sname                     | slatitude                | slongitude               | sispublic |
+=======+===========================+==========================+==========================+===========+
|  7060 | "de l'Église / de Verdun" |        45.46300108733155 |       -73.57156895217486 |         1 |
|  6173 | "Berri / Cherrier"        |        45.51908844137639 |       -73.56950908899307 |         1 |
|  6203 | "Hutchison / Sherbrooke"  |                 45.50781 |                -73.57208 |         1 |
|  6204 | "Milton / Durocher"       |               45.5081439 |             -73.57477158 |         1 |
|  6104 | "Wolfe / René-Lévesque"   |        45.51681750463149 |           -73.5541883111 |         1 |
+-------+---------------------------+--------------------------+--------------------------+-----------+
5 tuples
```

```SQL
SELECT
  COUNT(scode) AS count_scode
 ,COUNT(DISTINCT scode) AS countd_scode
FROM stations2017;
```

```
+-------------+--------------+
| count_scode | countd_scode |
+=============+==============+
|         546 |          546 |
+-------------+--------------+
1 tuple
```

This is good, we have the longitude and latitude associated with each station, which can be used to enrich the tripdata.

Since we have 546 stations, this gives the possibility of 546 x 546 = 298116 possible scenarios for trips.
However, we need not be concerned with trips that started and ended at the same station as those are noise.
Also, to weed out any further fluctuations in the input data set, we will limit ourselves to only those station combinations which has at the least 50 trips.


```SQL
CREATE LOCAL TEMPORARY TABLE freqstations AS
  SELECT stscode, endscode, COUNT(*) as numtrips
  FROM tripdata2017
  WHERE stscode <> endscode
  GROUP BY stscode, endscode
  HAVING COUNT(*) >= 50
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT * FROM freqstations LIMIT 5;
```
```
+---------+----------+----------+
| stscode | endscode | numtrips |
+=========+==========+==========+
|    6203 |     6204 |      101 |
|    6104 |     6114 |      308 |
|    6719 |     6354 |       91 |
|    6175 |     6118 |       81 |
|    6280 |     6160 |       50 |
+---------+----------+----------+
5 tuples
```

```SQL
SELECT COUNT(*) AS count_numtrips FROM freqstations;
```

```
+----------------+
| count_numtrips |
+================+
|          19300 |
+----------------+
```


We can see that there are 19,300 station combinations that is of interest to us.
Next stop, we need to include the longitude and latitude information of the start and end stations.

```SQL
CREATE LOCAL TEMPORARY TABLE freqstationscord AS
  SELECT fs.*, sst.slatitude AS stlat, sst.slongitude AS stlong
        ,est.slatitude AS enlat, est.slongitude AS enlong
  FROM freqstations fs, stations2017 sst, stations2017 est
  WHERE fs.stscode = sst.scode AND fs.endscode = est.scode
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT * FROM freqstationscord LIMIT 5;
```

```
+---------+----------+----------+--------------------------+--------------------------+--------------------------+--------------------------+
| stscode | endscode | numtrips | stlat                    | stlong                   | enlat                    | enlong                   |
+=========+==========+==========+==========================+==========================+==========================+==========================+
|    6203 |     6204 |      101 |                 45.50781 |                -73.57208 |               45.5081439 |             -73.57477158 |
|    6104 |     6114 |      308 |        45.51681750463149 |           -73.5541883111 |                 45.52353 |                -73.55199 |
|    6719 |     6354 |       91 |        45.46072936353252 |       -73.63407254219055 |                45.471743 |               -73.613924 |
|    6175 |     6118 |       81 |        45.52054115838411 |        -73.5677509009838 |        45.52504753729988 |       -73.56003552675247 |
|    6280 |     6160 |       50 |       45.524504989237535 |       -73.59414249658585 |         45.5329774553515 |       -73.58122229576111 |
+---------+----------+----------+--------------------------+--------------------------+--------------------------+--------------------------+
```


It would be easier if we can translate the coordinates to a distance metric.
 Python's geopy module supports this computation using Vincenty's formula.
 This provides us with a distance as crow flies between two coordiantes.
 This might be a reasonable approximation of actual distance travelled in a trip.
 We can use a _scalar UDF_ to accomplish this.

```Python
CREATE FUNCTION computevdist(stlat FLOAT, stlong FLOAT, enlat FLOAT, enlong FLOAT) 
RETURNS INTEGER
LANGUAGE PYTHON
{
    import numpy as np;
    import geopy.distance;  # We will use this module to compute distance.
    vdistm = np.empty(len(stlat), dtype=int);  # add a new empty column to hold distance.
    
    # populate the distance metric using longitude/latitude of coordinates.
    for i in range(0, len(stlat)):
        vdistm[i] = int(geopy.distance.distance((stlat[i], stlong[i]), (enlat[i], enlong[i])).meters);
    return vdistm;
};
```

```SQL
CREATE LOCAL TEMPORARY TABLE freqstationsdist AS
  SELECT stscode, endscode, numtrips, computevdist(stlat, stlong, enlat, enlong) AS vdistm
  FROM freqstationscord
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT * FROM freqstationsdist LIMIT 5;
```

```
+---------+----------+----------+--------+
| stscode | endscode | numtrips | vdistm |
+=========+==========+==========+========+
|    6203 |     6204 |      101 |    213 |
|    6104 |     6114 |      308 |    765 |
|    6719 |     6354 |       91 |   1995 |
|    6175 |     6118 |       81 |    783 |
|    6280 |     6160 |       50 |   1380 |
+---------+----------+----------+--------+
5 tuples
```

We can next enrich our trip data set with the distance information by joining these computed distances with each trip.

```SQL
CREATE LOCAL TEMPORARY TABLE tripdata AS
  SELECT id, duration, vdistm
  FROM tripdata2017 td, freqstationsdist fs
  WHERE td.stscode = fs.stscode
    AND td.endscode = fs.endscode
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT * FROM tripdata LIMIT 5;
```

```
+------+----------+--------+
| id   | duration | vdistm |
+======+==========+========+
|    2 |      195 |    213 |
|    3 |      285 |    765 |
|    5 |      620 |   1995 |
|   12 |      395 |    783 |
|   13 |     1085 |   1380 |
+------+----------+--------+
5 tuples
```

```SQL
SELECT
  COUNT(duration) AS count_duration
 ,COUNT(DISTINCT duration) AS countd_duration
 ,SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) AS countn_duration
 ,MAX(duration) AS max_duration
 ,MIN(duration) AS min_duration
 ,AVG(duration) AS avg_duration
 ,SYS.MEDIAN(duration) AS median_duration
 ,SYS.QUANTILE(duration, 0.25) AS q25_duration
 ,SYS.QUANTILE(duration, 0.50) AS q50_duration
 ,SYS.QUANTILE(duration, 0.75) AS q75_duration
 ,SYS.STDDEV_POP(duration) AS std_duration
 ,COUNT(vdistm) AS count_vdistm
 ,COUNT(DISTINCT vdistm) AS countd_vdistm
 ,SUM(CASE WHEN vdistm IS NULL THEN 1 ELSE 0 END) AS countn_vdistm
 ,MAX(vdistm) AS max_vdistm
 ,MIN(vdistm) AS min_vdistm
 ,AVG(vdistm) AS avg_vdistm
 ,SYS.MEDIAN(vdistm) AS median_vdistm
 ,SYS.QUANTILE(vdistm, 0.25) AS q25_vdistm
 ,SYS.QUANTILE(vdistm, 0.50) AS q50_vdistm
 ,SYS.QUANTILE(vdistm, 0.75) AS q75_vdistm
 ,SYS.STDDEV_POP(vdistm) AS std_vdistm
FROM tripdata
;
```

```
+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+
| count_duration | countd_duration | countn_duration | max_duration | min_duration | avg_duration             | median_duration | q25_duration | q50_duration | q75_duration | std_duration             | count_vdistm | countd_vdistm | countn_vdistm | max_vdistm | min_vdistm | avg_vdistm               | median_vdistm | q25_vdistm | q50_vdistm | q75_vdistm | std_vdistm               |
+================+=================+=================+==============+==============+==========================+=================+==============+==============+==============+==========================+==============+===============+===============+============+============+==========================+===============+============+============+============+==========================+
|        2256283 |            6663 |               0 |         7199 |           61 |        630.4498819518651 |             482 |          298 |          482 |          793 |        533.4431262970096 |      2256283 |          3652 |             0 |       9074 |         47 |       1369.8934561843528 |          1117 |        711 |       1117 |       1755 |        921.2930658164754 |
+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+
1 tuple
```

So we have trip duration for each trip and the distance as crow flies, between the two stations involved in the trip.

Also, we have about 2 million trips for which we have distance between stations metric.
Given that there are only a few thousand unique values for distance, we might want to keep some values of distance apart for testing.
For this purpose, we will first get distinct values for distance and then create an ordering over it.

```SQL
CREATE LOCAL TEMPORARY TABLE uniquetripdist AS
  SELECT vdistm, ROW_NUMBER() OVER(ORDER BY vdistm) AS rowidx
  FROM (SELECT DISTINCT vdistm FROM tripdata)x
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT
  COUNT(vdistm) AS count_vdistm
 ,COUNT(DISTINCT vdistm) AS countd_vdistm
 ,SUM(CASE WHEN vdistm IS NULL THEN 1 ELSE 0 END) AS countn_vdistm
 ,MAX(vdistm) AS max_vdistm
 ,MIN(vdistm) AS min_vdistm
 ,AVG(vdistm) AS avg_vdistm
 ,SYS.MEDIAN(vdistm) AS median_vdistm
 ,SYS.QUANTILE(vdistm, 0.25) AS q25_vdistm
 ,SYS.QUANTILE(vdistm, 0.50) AS q50_vdistm
 ,SYS.QUANTILE(vdistm, 0.75) AS q75_vdistm
 ,SYS.STDDEV_POP(vdistm) AS std_vdistm
FROM uniquetripdist
;
```

```
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+
| count_vdistm | countd_vdistm | countn_vdistm | max_vdistm | min_vdistm | avg_vdistm               | median_vdistm | q25_vdistm | q50_vdistm | q75_vdistm | std_vdistm               |
+==============+===============+===============+============+============+==========================+===============+============+============+============+==========================+
|         3652 |          3652 |             0 |       9074 |         47 |       2203.7724534501644 |          2013 |       1096 |       2013 |       3068 |        1386.047833359503 |
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+
1 tuple
```

```SQL
SELECT * FROM uniquetripdist WHERE rowidx <= 5;
```
```
+--------+--------+
| vdistm | rowidx |
+========+========+
|     47 |      1 |
|     71 |      2 |
|     81 |      3 |
|     85 |      4 |
|     88 |      5 |
+--------+--------+
5 tuples
```
```SQL
SELECT * FROM uniquetripdist WHERE rowidx > 3652-5;
```
```
5 tuples
+--------+--------+
| vdistm | rowidx |
+========+========+
|   8529 |   3648 |
|   8752 |   3649 |
|   8860 |   3650 |
|   9031 |   3651 |
|   9074 |   3652 |
+--------+--------+
5 tuples
```

We will try to train a model based on this data set and see if it looks promising.
This will requires a UDF.

```Python
CREATE FUNCTION BixiLinear()
RETURNS TABLE(sqerr1 FLOAT, sqerr2 FLOAT) LANGUAGE PYTHON
{
  import numpy as np;

  # Our linear regression equation is of the form.
  # dur = a + b*dist
  # We will normalize and extract the training data set to train this model.
  # For this purpose we will hardcode the max duration and max distance
  #   values we observed in the earlier output.
  # We are also keeping apart 1/3rd of the distance metrics for testing.
  # Rest we will use to build the training data.
  maxduration = 7199; maxdist = 9074;
  trainDataSet_ = _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * vdistm/{} AS vdistm ' \
                                      ',CAST(1.0 AS FLOAT) * duration/{} AS duration ' \
                                ' FROM ( SELECT vdistm, duration ' \
                                       ' FROM tripdata ' \
                                       ' WHERE vdistm IN ( SELECT vdistm ' \
                                                         ' FROM uniquetripdist ' \
                                                         ' WHERE NOT (rowidx%3 = 1) ) ' \
                                       ')x ;'.format(maxdist, maxduration));

  trainDataSet  = np.stack( (trainDataSet_['bias'], trainDataSet_['vdistm']) );
  trainDataSetDuration = trainDataSet_['duration'];
  params = np.ones((2, 1));

  #Let us do a prediction on our training dataset.
  pred = params.T @ trainDataSet;

  # We need to compute the squared error for the predictions.
  def squaredErr(actual, predicted):
    return ((predicted - actual) ** 2).sum() / (2 * (actual.shape[0]));

  # Let us see what is the error for the first iteration.
  sqerr = squaredErr(trainDataSetDuration, pred);

  # We need to perform a gradient descent based on the squared errors.
  # We will write another function to perform this.
  def gradDesc(actual, predicted, indata):
    return indata @ ((predicted - actual).T) / actual.shape[0];

  # Let us update our params using gradient descent using the error we got.
  # We also need to use a learning rate, alpha (arbitrarily chosen).
  alpha = 0.1;
  params = params - alpha * gradDesc(trainDataSetDuration, pred, trainDataSet);

  # Now let us try to use the updated params to train the model again.
  pred = params.T @ trainDataSet;
  sqerr2 = squaredErr(trainDataSetDuration, pred);

  return {'sqerr1':sqerr, 'sqerr2':sqerr2 };
};
```

We will execute this UDF to see if this approach has any promise.

```SQL
SELECT * FROM BixiLinear();
```

```
+--------------------------+--------------------------+
| sqerr1                   | sqerr2                   |
+==========================+==========================+
|       0.5694865536695626 |       0.4594973598186898 |
+--------------------------+--------------------------+
1 tuple
```

So the error rates are decreasing, so it might be a possible solution.
But before we proceed, may be we should check if Google maps API's distance metric gives a better learning rate. Let us see what fields we can use from Google.

```SQL
SELECT * FROM gmdata2017 LIMIT 5;
```

```
+---------+----------+--------+-----------+
| stscode | endscode | gdistm | gduration |
+=========+==========+========+===========+
|    6406 |     6052 |   3568 |       596 |
|    6050 |     6406 |   3821 |       704 |
|    6148 |     6173 |   1078 |       293 |
|    6110 |     6114 |   1319 |       337 |
|    6123 |     6114 |    725 |       177 |
+---------+----------+--------+-----------+
5 tuples
```

```SQL
SELECT
  COUNT(gdistm) AS count_gdistm
 ,COUNT(DISTINCT gdistm) AS countd_gdistm
 ,SUM(CASE WHEN gdistm IS NULL THEN 1 ELSE 0 END) AS countn_gdistm
 ,MAX(gdistm) AS max_gdistm
 ,MIN(gdistm) AS min_gdistm
 ,AVG(gdistm) AS avg_gdistm
 ,SYS.MEDIAN(gdistm) AS median_gdistm
 ,SYS.QUANTILE(gdistm, 0.25) AS q25_gdistm
 ,SYS.QUANTILE(gdistm, 0.50) AS q50_gdistm
 ,SYS.QUANTILE(gdistm, 0.75) AS q75_gdistm
 ,SYS.STDDEV_POP(gdistm) AS std_gdistm
 ,COUNT(gduration) AS count_gduration
 ,COUNT(DISTINCT gduration) AS countd_gduration
 ,SUM(CASE WHEN gduration IS NULL THEN 1 ELSE 0 END) AS countn_gduration
 ,MAX(gduration) AS max_gduration
 ,MIN(gduration) AS min_gduration
 ,AVG(gduration) AS avg_gduration
 ,SYS.MEDIAN(gduration) AS median_gduration
 ,SYS.QUANTILE(gduration, 0.25) AS q25_gduration
 ,SYS.QUANTILE(gduration, 0.50) AS q50_gduration
 ,SYS.QUANTILE(gduration, 0.75) AS q75_gduration
 ,SYS.STDDEV_POP(gduration) AS std_gduration
FROM gmdata2017
;
```

```
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+-----------------+------------------+------------------+---------------+---------------+--------------------------+------------------+---------------+---------------+---------------+--------------------------+
| count_gdistm | countd_gdistm | countn_gdistm | max_gdistm | min_gdistm | avg_gdistm               | median_gdistm | q25_gdistm | q50_gdistm | q75_gdistm | std_gdistm               | count_gduration | countd_gduration | countn_gduration | max_gduration | min_gduration | avg_gduration            | median_gduration | q25_gduration | q50_gduration | q75_gduration | std_gduration            |
+==============+===============+===============+============+============+==========================+===============+============+============+============+==========================+=================+==================+==================+===============+===============+==========================+==================+===============+===============+===============+==========================+
|        19516 |          4903 |             0 |      14530 |         18 |        2079.293092846895 |          1766 |       1118 |       1766 |       2711 |       1345.5948039376285 |           19516 |             1540 |                0 |          3083 |             4 |         516.190869030539 |              459 |           300 |           459 |           671 |        299.1843119406819 |
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+-----------------+------------------+------------------+---------------+---------------+--------------------------+------------------+---------------+---------------+---------------+--------------------------+
1 tuple
```

We can build a new data set for the trips between frequently used station combination that includes google's distance.

```SQL
CREATE LOCAL TEMPORARY TABLE gtripdata AS
  SELECT id, duration, gdistm, gduration
  FROM tripdata2017 td, freqstations fs, gmdata2017 gm
  WHERE td.stscode = fs.stscode
    AND td.endscode = fs.endscode
    AND td.stscode = gm.stscode
    AND td.endscode = gm.endscode
ON COMMIT PRESERVE ROWS
;
```

```SQL
SELECT * FROM gtripdata LIMIT 5;
```

```
+------+----------+--------+-----------+
| id   | duration | gdistm | gduration |
+======+==========+========+===========+
|    2 |      195 |    288 |       218 |
|    3 |      285 |   1007 |       296 |
|    5 |      620 |   2587 |       538 |
|   12 |      395 |   1615 |       322 |
|   13 |     1085 |   1710 |       352 |
+------+----------+--------+-----------+
5 tuples
```

```SQL
SELECT
  COUNT(gdistm) AS count_gdistm
 ,COUNT(DISTINCT gdistm) AS countd_gdistm
 ,SUM(CASE WHEN gdistm IS NULL THEN 1 ELSE 0 END) AS countn_gdistm
 ,MAX(gdistm) AS max_gdistm
 ,MIN(gdistm) AS min_gdistm
 ,AVG(gdistm) AS avg_gdistm
 ,SYS.MEDIAN(gdistm) AS median_gdistm
 ,SYS.QUANTILE(gdistm, 0.25) AS q25_gdistm
 ,SYS.QUANTILE(gdistm, 0.50) AS q50_gdistm
 ,SYS.QUANTILE(gdistm, 0.75) AS q75_gdistm
 ,SYS.STDDEV_POP(gdistm) AS std_gdistm
 ,COUNT(duration) AS count_duration
 ,COUNT(DISTINCT duration) AS countd_duration
 ,SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) AS countn_duration
 ,MAX(duration) AS max_duration
 ,MIN(duration) AS min_duration
 ,AVG(duration) AS avg_duration
 ,SYS.MEDIAN(duration) AS median_duration
 ,SYS.QUANTILE(duration, 0.25) AS q25_duration
 ,SYS.QUANTILE(duration, 0.50) AS q50_duration
 ,SYS.QUANTILE(duration, 0.75) AS q75_duration
 ,SYS.STDDEV_POP(duration) AS std_duration
 ,COUNT(gduration) AS count_gduration
 ,COUNT(DISTINCT gduration) AS countd_gduration
 ,SUM(CASE WHEN gduration IS NULL THEN 1 ELSE 0 END) AS countn_gduration
 ,MAX(gduration) AS max_gduration
 ,MIN(gduration) AS min_gduration
 ,AVG(gduration) AS avg_gduration
 ,SYS.MEDIAN(gduration) AS median_gduration
 ,SYS.QUANTILE(gduration, 0.25) AS q25_gduration
 ,SYS.QUANTILE(gduration, 0.50) AS q50_gduration
 ,SYS.QUANTILE(gduration, 0.75) AS q75_gduration
 ,SYS.STDDEV_POP(gduration) AS std_gduration
FROM gtripdata
;
```

```
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+-----------------+------------------+------------------+---------------+---------------+--------------------------+------------------+---------------+---------------+---------------+--------------------------+
| count_gdistm | countd_gdistm | countn_gdistm | max_gdistm | min_gdistm | avg_gdistm               | median_gdistm | q25_gdistm | q50_gdistm | q75_gdistm | std_gdistm               | count_duration | countd_duration | countn_duration | max_duration | min_duration | avg_duration             | median_duration | q25_duration | q50_duration | q75_duration | std_duration             | count_gduration | countd_gduration | countn_gduration | max_gduration | min_gduration | avg_gduration            | median_gduration | q25_gduration | q50_gduration | q75_gduration | std_gduration            |
+==============+===============+===============+============+============+==========================+===============+============+============+============+==========================+================+=================+=================+==============+==============+==========================+=================+==============+==============+==============+==========================+=================+==================+==================+===============+===============+==========================+==================+===============+===============+===============+==========================+
|      2256283 |          4880 |             0 |      14530 |         48 |       1834.7316307395836 |          1497 |        960 |       1497 |       2359 |       1236.2583855217713 |        2256283 |            6663 |               0 |         7199 |           61 |        630.4498819518651 |             482 |          298 |          482 |          793 |        533.4431262970096 |         2256283 |             1532 |                0 |          3083 |            11 |        454.6804899917253 |              394 |           257 |           394 |           591 |       273.36214065724846 |
+--------------+---------------+---------------+------------+------------+--------------------------+---------------+------------+------------+------------+--------------------------+----------------+-----------------+-----------------+--------------+--------------+--------------------------+-----------------+--------------+--------------+--------------+--------------------------+-----------------+------------------+------------------+---------------+---------------+--------------------------+------------------+---------------+---------------+---------------+--------------------------+
1 tuple
```

Google also provides its estimated duration for the trip.
We will have to see in the end if our trained model is able to predict the trip duration better than google's estimate.
So we will also save Google's estimate for the trip duration for that comparison.

Next up, we need to format this dataset the same way we did the first one.

```SQL
CREATE LOCAL TEMPORARY TABLE guniquetripdist AS
  SELECT gdistm, ROW_NUMBER() OVER(ORDER BY gdistm) AS rowidx
  FROM (SELECT DISTINCT gdistm FROM gtripdata)x
ON COMMIT PRESERVE ROWS
;
```


We will try to train a model based on this data set and see if it looks promising.
This will also be done via a UDF.

```Python
CREATE FUNCTION BixiLinearG()
RETURNS TABLE(sqerr1 FLOAT, sqerr2 FLOAT) LANGUAGE PYTHON
{
  import numpy as np;

  # We will normalize and extract the training data set to train this model.
  # For this purpose we will hardcode the max duration and max distance values
  #   that we observed in the earlier output.
  # We are also keeping apart 1/3rd of the distance metrics for testing.
  # Rest we will use to build the training data.
  gmaxduration = 7199; gmaxdist = 14530;
  gtrainDataSet_ = _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * gdistm/{} AS gdistm ' \
                                       ' ,CAST(1.0 AS FLOAT) * duration/{} AS duration ' \
                                 ' FROM ( SELECT gdistm, duration ' \ 
                                        ' FROM gtripdata ' \
                                        ' WHERE gdistm IN ( SELECT gdistm ' \
                                                          ' FROM guniquetripdist ' \
                                                          ' WHERE NOT (rowidx%3 = 1) ) ' \
                                        ')x ;'.format(gmaxdist, gmaxduration));

  gtrainDataSet  = np.stack( (gtrainDataSet_['bias'], gtrainDataSet_['gdistm']) );
  gtrainDataSetDuration = gtrainDataSet_['duration'];
  gparams = np.ones((2, 1));

  #Let us do a prediction on our training dataset.
  gpred = gparams.T @ gtrainDataSet;

  # We need to compute the squared error for the predictions.
  def squaredErr(actual, predicted):
    return ((predicted - actual) ** 2).sum() / (2 * (actual.shape[0]));

  # Let us see what is the error for the first iteration.
  gsqerr = squaredErr(gtrainDataSetDuration, gpred);

  # We need to perform a gradient descent based on the squared errors.
  def gradDesc(actual, predicted, indata):
    return indata @ ((predicted - actual).T) / actual.shape[0];

  # Let us update our params using gradient descent using the error we got.
  alpha = 0.1;
  gparams = gparams - alpha * gradDesc(gtrainDataSetDuration, gpred, gtrainDataSet);

  # Now let us try to use the updated params to train the model again.
  gpred = gparams.T @ gtrainDataSet;
  gsqerr2 = squaredErr(gtrainDataSetDuration, gpred);

  return {'sqerr1':gsqerr, 'sqerr2':gsqerr2 };
};
```

Let us execute this UDF and see how it looks.

```SQL
SELECT * FROM BixiLinearG();
```

```
+--------------------------+--------------------------+
| sqerr1                   | sqerr2                   |
+==========================+==========================+
|       0.5419675497480462 |       0.4379084758530772 |
+--------------------------+--------------------------+
1 tuple
```

It looks like using Google maps' distance is giving us a slight advantage.
That makes sense, since Vincenty's formula computes distances as a crow flies,
where as Google maps' distance metric is based on the actual road network distances.
Better data gives better prediction results !

We are done with the feature selection and feature engineering phase for now.

Next we will proceed to train our linear regression model using the training data set.

We we will modify the original UDF a bit to add the iteration logic for linear regression.

```SQL
DROP FUNCTION BixiLinearG;
```

```Python
CREATE FUNCTION BixiLinearG()
RETURNS TABLE
(
  sqerr1 FLOAT
, sqerr2 FLOAT
) LANGUAGE PYTHON
{
  import numpy as np;

  # We will normalize and extract the training data set to train this model.
  # For this purpose we will hardcode the max duration and max distance values
  #    that we observed in the earlier output.
  gmaxduration = 7199; gmaxdist = 14530;
  # We are also keeping apart 1/3rd of the distance metrics for testing.
  # Rest we will use to build the training data.
  gtrainDataSet_ = _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * gdistm/{} AS gdistm ' \ 
                                       ',CAST(1.0 AS FLOAT) * duration/{} AS duration ' \
                                 ' FROM ( SELECT gdistm, duration ' \ 
                                        ' FROM gtripdata ' \
                                        ' WHERE gdistm IN ( SELECT gdistm ' \
                                                          ' FROM guniquetripdist ' \
                                                          ' WHERE NOT (rowidx%3 = 1) ) ' \
                                        ')x ;'.format(gmaxdist, gmaxduration));

  gtrainDataSet  = np.stack( (gtrainDataSet_['bias'], gtrainDataSet_['gdistm']) );
  gtrainDataSetDuration = gtrainDataSet_['duration'];
  gparams = np.ones((2, 1));

  # We need to compute the squared error for the predictions.
  def squaredErr(actual, predicted):
    return ((predicted - actual) ** 2).sum() / (2 * (actual.shape[0]));

  # We need to perform a gradient descent based on the squared errors.
  def gradDesc(actual, predicted, indata):
    return indata @ ((predicted - actual).T) / actual.shape[0];

  alpha = 0.1;
  for i in range(0, 1000):
    gpred = gparams.T @ gtrainDataSet;
    gparams = gparams - alpha * gradDesc(gtrainDataSetDuration, gpred, gtrainDataSet);

  gsqerr = squaredErr(gtrainDataSetDuration, gpred);

  # Let us see how our model performs in predictions against the test data set we had kept apart.
  gtestDataSet_ =  _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * gdistm/{} AS gdistm ' \
                                       ',CAST(1.0 AS FLOAT) * duration/{} AS duration , gduration ' \ 
                                 ' FROM ( SELECT gdistm, duration, gduration ' \ 
                                        ' FROM gtripData ' \ 
                                        ' WHERE gdistm IN ( SELECT gdistm ' \
                                                          ' FROM guniqueTripDist ' \
                                                          ' WHERE (rowidx%3 = 1) ) ' \
                                       ')x ;'.format(gmaxdist, gmaxduration));
                                       
  gtestDataSet  = np.stack( (gtestDataSet_['bias'], gtestDataSet_['gdistm']) );
  gtestDataSetDuration = gtestDataSet_['duration'];

  gtestpred = gparams.T @ gtestDataSet;
  gtestsqerr1 = squaredErr(gtestDataSetDuration * gmaxduration, gtestpred * gmaxduration);

  # We would also like to check how the duration provided 
  #   by Google maps API hold up to the test data set.
  gtestsqerr2 = squaredErr(gtestDataSetDuration * gmaxduration, gtestDataSet_['gduration']);

  return {'sqerr1':gtestsqerr1, 'sqerr2':gtestsqerr2};
};
```

```SQL
SELECT * FROM BixiLinearG();
```

```
+--------------------------+--------------------------+
| sqerr1                   | sqerr2                   |
+==========================+==========================+
|        99225.05321538352 |       111763.37983591038 |
+--------------------------+--------------------------+
1 tuple

```
It looks like our model did well.

Clean ups.
```SQL
DROP FUNCTION computevdist;
DROP FUNCTION BixiLinear;
DROP FUNCTION BixiLinearG;
```
