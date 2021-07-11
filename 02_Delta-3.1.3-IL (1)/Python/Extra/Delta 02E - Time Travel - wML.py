# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Use Databricks Delta Time Travel and MLflow to Analyze Power Plant Data
# MAGIC Databricks&reg; Delta Time Travel allows you to work with older snapshots of data.
# MAGIC 
# MAGIC MLflow allows you to organize and keep track of your Machine Learning experiments.
# MAGIC 
# MAGIC The two work seamlessly as the following demonstration notebook shows.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC 0. Stream power plant data to a Databricks Delta table
# MAGIC 0. Train a model on a current version of our data
# MAGIC 0. Post some results to MLflow
# MAGIC 0. Rewind to an older version of the data
# MAGIC 0. Re-train our model on an older version of the data
# MAGIC 0. Evaluate the (rewound) data 
# MAGIC 0. Make predictions on the streaming data
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers, Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Additional Libraries:
# MAGIC   - **`mlflow==1.5.0`**
# MAGIC * Familiarity with Spark-ML is helpful, but not required
# MAGIC * Familiarity with MLflow is helpful, but not required
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL
# MAGIC   - Structured Streaming
# MAGIC   - Delta
# MAGIC 
# MAGIC ## Library Requirements
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Additional libraries must be attached to your cluster for this lesson to work.
# MAGIC 
# MAGIC   
# MAGIC We will use the PyPI library **`mlflow==1.5.0`**.
# MAGIC * This is used for logging ML experiments
# MAGIC 
# MAGIC For more information on how to create and/or install PyPI libraries see:
# MAGIC * <a href="https://www.databricks.training/step-by-step/creating-pypi-libraries" target="_blank">Creating a Workspce Library</a>
# MAGIC * <a href="https://www.databricks.training/step-by-step/installing-libraries-from-pypi" target="_blank">Installing a Cluster Library</a> (recomended)
# MAGIC    
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC A powerplant dataset found in
# MAGIC `/mnt/training/power-plant/streamed.parquet`.
# MAGIC 
# MAGIC The schema definition is:
# MAGIC 
# MAGIC - AT = Atmospheric Temperature [1.81-37.11]°C
# MAGIC - V = Exhaust Vaccum Speed [25.36-81.56] cm Hg
# MAGIC - AP = Atmospheric Pressure in [992.89-1033.30] milibar
# MAGIC - RH = Relative Humidity [0-100]%
# MAGIC - PE = Power Output [420.26-495.76] MW
# MAGIC 
# MAGIC PE is the label or target. This is the value we are trying to predict given the measurements.
# MAGIC 
# MAGIC *Reference [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
# MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

#  Make sure the libraries are attached:
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Time Travel
# MAGIC 
# MAGIC The Databricks Delta log has a list of what files are valid for each read / write operation.
# MAGIC 
# MAGIC By referencing this list, a request can be made for the data at a specific point in time. 
# MAGIC 
# MAGIC This is similar to the concept of code Revision histories.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slow Stream of Files
# MAGIC 
# MAGIC Our stream source is a repository of many small files.
# MAGIC 
# MAGIC And to help us manage our streams better, we will make use of **`untilStreamIsReady()`**, **`stopAllStreams()`** and define the following, **`myStreamName`**:

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)
dataPath = "/mnt/training/power-plant/streamed.parquet"
myStreamName = "time_travel_pi"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType

dataSchema = StructType([
  StructField("AT", DoubleType(), True),
  StructField("V", DoubleType(), True),
  StructField("AP", DoubleType(), True),
  StructField("RH", DoubleType(), True),
  StructField("PE", DoubleType(), True)
])

initialDF = (spark
  .readStream                        # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)   # Force processing of only 1 file per trigger 
  .schema(dataSchema)                # Required for all streaming DataFrames
  .parquet(dataPath) 
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to a Databricks Delta Table
# MAGIC 
# MAGIC Use this to create `powerTable`.

# COMMAND ----------

from pyspark.sql.types import TimestampType

writePath      = workingDir + "/output.parquet"    # A subdirectory for our output
checkpointPath = workingDir + "/output.checkpoint" # A subdirectory for our checkpoint & W-A logs

powerTable = "powerTable"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Introducing Time Travel
# MAGIC 
# MAGIC Databricks Delta time travel allows you to query an older snapshot of a table.
# MAGIC 
# MAGIC Here, we introduce a new option to Databricks Delta.
# MAGIC 
# MAGIC `.option("timestampAsOf", now)` 
# MAGIC 
# MAGIC Where `now` is the current timestamp, that must be a STRING that can be cast to a Timestamp.
# MAGIC 
# MAGIC There is an alternate notation as well 
# MAGIC 
# MAGIC `.option("versionAsOf", version)`
# MAGIC 
# MAGIC More details are described in the <a href="https://docs.databricks.com/delta/delta-batch.html#deltatimetravel" target="_blank">official documentation</a>.

# COMMAND ----------

import datetime
now = datetime.datetime.now()

streamingQuery = (initialDF                     # Start with our "streaming" DataFrame
  .writeStream                                  # Get the DataStreamWriter
  .trigger(processingTime="3 seconds")          # Configure for a 3-second micro-batch
  .queryName(myStreamName)                      # Specify Query Name
  .format("delta")                              # Specify the sink type, a Parquet file
  .option("timestampAsOf", now)                 # Timestamp the stream in the form of string that can be converted to TimeStamp
  .outputMode("append")                         # Write only new data to the "file"
  .option("checkpointLocation", checkpointPath) # Specify the location of checkpoint files & W-A logs
  .table(powerTable)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell below is to keep the stream running in case we do a RunAll

# COMMAND ----------

untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame out of the Delta stream so we can get a scatterplot.
# MAGIC 
# MAGIC This will be a "snapshot" of the data at an instant in time, so, a static table.

# COMMAND ----------

staticPowerDF = spark.table(powerTable)

# COMMAND ----------

display( spark.sql("SELECT count(*) FROM {}".format(powerTable)) )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use Scatter Plot show intution
# MAGIC 
# MAGIC Let's plot `PE` versus other fields to see if there are any relationships.
# MAGIC 
# MAGIC You can toggle between fields by adjusting Plot Options.
# MAGIC 
# MAGIC Couple observations
# MAGIC * It looks like there is strong linear correlation between Atmospheric Temperature and Power Output
# MAGIC * Maybe a bit of correlation between Atmospheric Pressure and Power Output
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Values:</b> `AT` and `PE`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Scatter plot</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/plot-options-scatter.png"/></div><br/> 

# COMMAND ----------

display(staticPowerDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Train LR Model on Static DataFrame
# MAGIC 
# MAGIC 0. Split `staticPowerDF` into training and test set
# MAGIC 0. Use all features: AT, AP, RH and V
# MAGIC 0. Reshape training set
# MAGIC 0. Do linear regression
# MAGIC 0. Predict power output (PE)
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Data is changing underneath us

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Split DataFrame into test/train sets
(trainDF, testDF) = staticPowerDF.randomSplit([0.80, 0.20], seed=42)

# Set which columns are features
assembler = VectorAssembler(inputCols=["AT", "AP", "RH", "V"], outputCol="features")

# Reshape the train set
trainVecDF = assembler.transform(trainDF)

# Set which column is the label
lr = LinearRegression(labelCol="PE", featuresCol="features")

# Fit training data
lrModel = lr.fit(trainVecDF)

# Append predicted PE column, rename it
trainPredictionsDF = (lrModel
  .transform(trainVecDF)
  .withColumnRenamed("prediction", "predictedPE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use MLFlow 
# MAGIC 
# MAGIC MLflow is an open source platform for managing the end-to-end machine learning lifecycle. 
# MAGIC 
# MAGIC In this notebook, we use MLflow to track experiments to record and compare parameters and results.
# MAGIC 
# MAGIC More details are in the <a href="https://www.mlflow.org/docs/latest/index.html" target="_blank">official documentation</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post results to MLflow
# MAGIC 
# MAGIC In this notebook, we would like to keep track of the Root Mean Squared Error (RMSE).
# MAGIC 
# MAGIC This line actually does the work of posting the RMSE to MLflow.
# MAGIC 
# MAGIC **`logMetric("RMSE", rmse)`**
# MAGIC 
# MAGIC If you rerun the below cell multiple times, you will see new runs are posted to MLflow, with different RMSE!

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import os

with mlflow.start_run(run_name="Time-Travel-1") as run:
  
  # Evaluate our result
  eval = RegressionEvaluator(labelCol="PE", predictionCol="predictedPE", metricName="rmse")
  rmse = eval.evaluate(trainPredictionsDF)

  mlflow.log_metric("RMSE", rmse)
  
  experimentID = run.info.experiment_id
  

displayHTML("""<div>RMSE: {}</div>
               <div>&nbsp;</div>
               <div>Click "Runs" in the upper-right hand corner of the screen to view the results.""".format(rmse))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question to Ponder:
# MAGIC 
# MAGIC Why is `RMSE` changing under our feet? We are working with "static" DataFrames..

# COMMAND ----------

# MAGIC %md
# MAGIC Let's wind back to a version of our table we had several hours ago & fit our data to that version.
# MAGIC 
# MAGIC Maybe some pattern we were looking at became apparent for the first time a few hours ago.
# MAGIC 
# MAGIC This query shows the timestamps of the Delta writes as they were happening.

# COMMAND ----------

display(spark.sql("SELECT timestamp FROM (DESCRIBE HISTORY {}) ORDER BY timestamp".format(powerTable)))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's rewind back to almost the beginning (where we had just a handful of rows), let's say the 5th write.
# MAGIC 
# MAGIC Maybe we started noticing a pattern at this point.

# COMMAND ----------

# Pick out 5th write
oldTimestamp = spark.sql("SELECT timestamp FROM (DESCRIBE HISTORY {}) ORDER BY timestamp".format(powerTable)).take(5)[-1].timestamp

# Re-build the DataFrame as it was in the 5th write
rewoundDF = spark.sql("SELECT * FROM {} TIMESTAMP AS OF '{}'".format(powerTable, oldTimestamp))

# COMMAND ----------

# MAGIC %md
# MAGIC We had this many (few) rows back then.

# COMMAND ----------

rewoundDF.count()

# COMMAND ----------

display(rewoundDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Model Based on Data from a Few Hours Ago
# MAGIC 
# MAGIC * Use `rewoundDF`
# MAGIC * Write to MLflow
# MAGIC 
# MAGIC Notice the only change from what we did earlier is the use of `rewoundDF`

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Split DataFrame into test/train sets
(trainDF, testDF) = staticPowerDF.randomSplit([0.80, 0.20], seed=42)

# Set which columns are features
assembler = VectorAssembler(inputCols=["AT", "AP", "RH", "V"], outputCol="features")

# Reshape the train set
trainVecDF = assembler.transform(trainDF)

# Set which column is the label
lr = LinearRegression(labelCol="PE", featuresCol="features")

# Fit training data
lrModel = lr.fit(trainVecDF)

# Append predicted PE column, rename it
trainPredictionsDF = (lrModel
  .transform(trainVecDF)
  .withColumnRenamed("prediction", "predictedPE")
)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
import mlflow

with mlflow.start_run(run_name="Time-Travel-2") as run:
  
  # Evaluate our result
  eval = RegressionEvaluator(labelCol="PE", predictionCol="predictedPE", metricName="rmse")
  rmse = eval.evaluate(trainPredictionsDF)

  # Log the results with MLFlow
  mlflow.log_metric("RMSE", rmse)
  
# Display some results below
displayHTML("""<div>RMSE: {}</div>
               <div>&nbsp;</div>
               <div>Click "Runs" in the upper-right hand corner of the screen to view the results.""".format(rmse))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Using Test Set
# MAGIC 
# MAGIC 0. Reshape data via `assembler.transform()`
# MAGIC 0. Apply linear regression model 
# MAGIC 0. Record metrics in MLflow

# COMMAND ----------

from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline(stages=[assembler, lr])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrModel = lrPipeline.fit(testDF)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RegressionMetrics
import mlflow

with mlflow.start_run(run_name="Time-Travel-3") as run:
  justPredictionAndLabelDF = lrModel.transform(testDF).select("prediction", "PE")
  metrics = RegressionMetrics(justPredictionAndLabelDF.rdd.map(lambda r: (r.prediction, r.PE)))
  rmse = metrics.rootMeanSquaredError

  # Log the results with MLFlow
  mlflow.log_metric("RMSE", rmse)
  
# Display some results below
displayHTML("""<div>RMSE: {}</div>
               <div>&nbsp;</div>
               <div>Click "Runs" in the upper-right hand corner of the screen to view the results.""".format(rmse))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Final Model
# MAGIC 
# MAGIC The stats from the test set are pretty good so we've done a decent job coming up with the model.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression, LinearRegressionModel
lrPipeline = Pipeline()

weights = lrModel.stages[1].coefficients
intercept = lrModel.stages[1].intercept

# COMMAND ----------

print("The equation that describes the relationship between AT, AP, RH and PE is:\nPE = {} - {} * AT + {} * AP - {} * RH - {} * V"  
      .format(intercept, abs(weights[0]), weights[1], abs(weights[2]), abs(weights[3])))

# COMMAND ----------

# MAGIC %md
# MAGIC We are pretty happy with the model we developed.
# MAGIC 
# MAGIC Let's save the model.

# COMMAND ----------

fileName = workingDir + "/model"
lrModel.write().overwrite().save(fileName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make real-time predictions using the data from the stream.
# MAGIC 
# MAGIC Let's apply the model we saved to the rest of the streaming data!

# COMMAND ----------

from pyspark.ml import PipelineModel
lrPredModel = PipelineModel.load(fileName)

# COMMAND ----------

# MAGIC %md
# MAGIC Time to make some predictions!!

# COMMAND ----------

stream = (lrPredModel
          .transform(initialDF)
          .withColumnRenamed("prediction", "PredictedPE"))

display(stream.select("AT", "AP", "V", "RH", "PE", "PredictedPE"))

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to stop all streams

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
