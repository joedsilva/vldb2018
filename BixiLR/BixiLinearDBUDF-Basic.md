This is the database UDF (stored procedure) version of the linear regression workflow on the Bixi dataset. 

This is a "basic" workflow where the user (an _oracle_) directly builds the training dataset with minimal to no exploration,
an unrealistic, but best case scenario.

Since the entire workflow logic is written as one whole function, the user has no way interact with any of the intermediate objects or produce any intermittent outputs.

Any computational errors / inadequacies will come into light only after the entire workflow is executed. At which point the user will have to edit the function and re-run it completely, as no intermediate NumPy objects survive outside of the UDF scope.

```Python
CREATE FUNCTION BixiLinearUDF()
RETURNS TABLE
(
  modelsqerr FLOAT
, gapisqerr FLOAT
)
LANGUAGE PYTHON
{
  import numpy as np;

  # This is the dataset of interest to us. Stations involved in the most frequent trips,
  #  enriched with Google maps API data.
  _conn.execute(' CREATE LOCAL TEMPORARY TABLE gtripData AS ' \ 
                ' SELECT t.duration, g.gdistm, g.gduration ' \
                ' FROM ( SELECT stscode, endscode ' \ 
                       ' FROM bixi.tripdata2017 ' \
                       ' WHERE stscode<>endscode ' \
                       ' GROUP BY stscode, endscode ' \
                       ' HAVING COUNT(*) >= 50 ' \
                       ' )s, tripdata2017 t, gmdata2017 g ' \
                ' WHERE t.stscode  = s.stscode ' \
                  ' AND t.endscode = s.endscode ' \
                  ' AND t.stscode  = g.stscode ' \
                  ' AND t.endscode = g.endscode ' \
                ' ON COMMIT PRESERVE ROWS ;');

  # As there are multiple trips between the same stations, many trips will have the same distance.
  # So we want to keep some values of distance apart for testing.
  # For this purpose, we will first get distinct values for distance
  #   and then create an ordering over it.
  _conn.execute(' CREATE LOCAL TEMPORARY TABLE guniqueTripDist AS ' \
                ' SELECT gdistm, ROW_NUMBER() OVER(ORDER BY gdistm) AS rowidx ' \
                ' FROM ( SELECT DISTINCT gdistm FROM gtripData )x ' \
                ' ON COMMIT PRESERVE ROWS ;');

  # We need to normalize distance and duration fields.
  # For this purpose we need to first find the maximum value for them.
  _result = _conn.execute('SELECT MAX(gdistm) AS gmaxdist FROM guniqueTripDist;');
  gmaxdist = _result['gmaxdist'][0];
  _result = _conn.execute('SELECT MAX(duration) AS gmaxduration FROM gtripData;');
  gmaxduration = _result['gmaxduration'][0];

  # Our linear regression equation is of the form.
  # dur = a + b*dist
  # We will normalize and extract the training data set to train this model.
  # We will keep apart 1/3rd of data for testing and use only 2/3rd of the data for training.
  gtrainDataSet_ = _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * gdistm/{} AS gdistm ' \
                                        ',CAST(1.0 AS FLOAT) * duration/{} AS duration ' \
                                 ' FROM ( SELECT gdistm, duration ' \
                                        ' FROM gtripData ' \ 
                                        ' WHERE gdistm IN ( SELECT gdistm ' \ 
                                                          ' FROM guniqueTripDist ' \ 
                                                          ' WHERE NOT (rowidx%3 = 1) ) ' \
                                        ')x;'.format(gmaxdist, gmaxduration));
  gtrainDataSet  = np.stack( (gtrainDataSet_['bias'], gtrainDataSet_['gdistm']) );
  gtrainDataSetDuration = gtrainDataSet_['duration'];
  gparams = np.ones((2, 1));


  #Let us do a prediction on our training dataset.
  gpred = gparams.T @ gtrainDataSet;

  # We need to compute the squared error for the predictions.
  # Since we will be reusing them, we might as well store it as a function.
  def squaredErr(actual, predicted):
    return ((predicted - actual) ** 2).sum() / (2 * (actual.shape[0]));

  # Let us see what is the error for the first iteration.
  gsqerr = squaredErr(gtrainDataSetDuration, gpred);

  # We need to perform a gradient descent based on the squared errors.
  # We will write another function to perform this.
  def gradDesc(actual, predicted, indata):
    return indata @ ((predicted - actual).T) / actual.shape[0];

  # Let us update our params using gradient descent using the error we got.
  # We also need to use a learning rate, alpha (arbitrarily chosen).
  alpha = 0.1;
  gparams = gparams - alpha * gradDesc(gtrainDataSetDuration, gpred, gtrainDataSet);

  # Now let us try to use the updated params to train the model again.
  gpred = gparams.T @ gtrainDataSet;
  gsqerr = squaredErr(gtrainDataSetDuration, gpred);
  
  ## Ideally, a user would like to view the value of gsqerr to be satisfied with the error rate.
  ## But in UDFs, we cannot produce intermediate outputs.

  # We are done with the feature selection and feature engineering phase for now.
  # Next we will proceed to train our linear regression model using the training data set.
  for i in range(0, 1000):
    gpred = gparams.T @ gtrainDataSet;
    gparams = gparams - alpha * gradDesc(gtrainDataSetDuration, gpred, gtrainDataSet);
    if ((i + 1) % 100 == 0):
        gsqerr = squaredErr(gtrainDataSetDuration, gpred);
        ## User cannot take a peak into the error rate to see how fast they are decreasing.

  gsqerr = squaredErr(gtrainDataSetDuration, gpred);

  # Let us see how our model performs in predictions against the test data set we had kept apart.
  gtestDataSet_ =  _conn.execute(' SELECT 1 AS bias, CAST(1.0 AS FLOAT) * gdistm/{} AS gdistm ' \
                                        ',CAST(1.0 AS FLOAT) * duration/{} AS duration, gduration ' \ 
                                 ' FROM ( SELECT gdistm, duration, gduration ' \
                                        ' FROM gtripData ' \
                                        ' WHERE gdistm IN ( SELECT gdistm ' \ 
                                                          ' FROM guniqueTripDist ' \ 
                                                          ' WHERE (rowidx%3 = 1) )' \
                                        ')x ;'.format(gmaxdist, gmaxduration));
  gtestDataSet  = np.stack( (gtestDataSet_['bias'], gtestDataSet_['gdistm']) );
  gtestDataSetDuration = gtestDataSet_['duration'];

  gtestpred = gparams.T @ gtestDataSet;
  gtestsqerr1 = squaredErr(gtestDataSetDuration * gmaxduration, gtestpred * gmaxduration);

  # We would also like to check how the duration provided 
  #  by Google maps API hold up to the test data set.
  gtestsqerr2 = squaredErr(gtestDataSetDuration * gmaxduration, gtestDataSet_['gduration']);

  #Send the errors back to the user.
  #Anything else that the user wants to see will also have to be included here or
  #  saved explicitly in the database.
  # Otherwise they are lost once the UDF terminates.
  return {'modelsqerr':gtestsqerr1, 'gapisqerr':gtestsqerr2};
};
```

We can now execute the entire workflow as a single UDF in the database.
```SQL
SELECT * FROM BixiLinearUDF();
```

```
+--------------------------+--------------------------+
| modelsqerr               | gapisqerr                |
+==========================+==========================+
|        99215.98424571124 |       111763.37983591038 |
+--------------------------+--------------------------+
```

It looks like our model did well.

Cleanup....

```SQL
DROP FUNCTION BixiLinearUDF();
```
