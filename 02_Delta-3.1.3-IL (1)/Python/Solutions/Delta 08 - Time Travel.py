# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Databricks Delta Time Travel to View an Older Snapshot of Data
# MAGIC Databricks&reg; Delta Time Travel allows you to work with older snapshots of data.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC 0. Stream power plant data to a Databricks Delta table
# MAGIC 0. Look at summary statistics of current data set
# MAGIC 0. Rewind to an older version of the data
# MAGIC 0. Look at summary statistics of older data set
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers, Data Scientists
# MAGIC * Secondary Audience: Data Analysts
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL
# MAGIC   - Structured Streaming
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

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Time Travel
# MAGIC 
# MAGIC The Databricks Delta log has a list of what files are valid for each read / write operation.
# MAGIC 
# MAGIC By referencing this list, a request can be made for the data at a specific point in time. 
# MAGIC 
# MAGIC This is similar to the concept of code Revision histories.
# MAGIC 
# MAGIC Examples of Time Travel use cases are:
# MAGIC * Re-creating analyses, reports, or outputs (for example, the output of a machine learning model). 
# MAGIC   * This could be useful for debugging or auditing, especially in regulated industries.
# MAGIC * Writing complex temporal queries.
# MAGIC * Fixing mistakes in your data.
# MAGIC * Providing snapshot isolation for a set of queries for fast changing tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slow Stream of Files
# MAGIC 
# MAGIC Our stream source is a repository of many small files.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType
spark.conf.set("spark.sql.shuffle.partitions", 8)

dataPath = "/mnt/training/power-plant/streamed.parquet"

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
# MAGIC And to help us manage our streams better, we will make use of **`untilStreamIsReady()`**, **`stopAllStreams()`** and define the following, **`myStreamName`**:

# COMMAND ----------

myStreamName = "lesson08_pi"

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
  .queryName(myStreamName)                       # Specify Query Name
  .format("delta")                              # Specify the sink type, a Parquet file
  .option("timestampAsOf", now)                 # Timestamp the stream in the form of string that can be converted to TimeStamp
  .outputMode("append")                         # Write only new data to the "file"
  .option("checkpointLocation", checkpointPath) # Specify the location of checkpoint files & W-A logs
  .table(powerTable)
)

# COMMAND ----------

# Wait until the stream is ready before proceeding
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Retention Period and Table Properties
# MAGIC 
# MAGIC You configure retention periods using `ALTER TABLE` syntax with the following table properties:
# MAGIC 
# MAGIC * `delta.logRetentionDuration "interval interval-string" `
# MAGIC   * Configure how long you can go back in time. Default is interval 30 days.
# MAGIC 
# MAGIC * `delta.deletedFileRetentionDuration = "interval interval-string" `
# MAGIC   * Configure how long stale data files are kept around before being deleted with VACUUM. Default is interval 1 week.
# MAGIC   
# MAGIC * `interval-string` is in the form `30 days` or `1 week`
# MAGIC 
# MAGIC For full access to 30 days of historical data, set `delta.deletedFileRetentionDuration = "interval 30 days" ` on your table. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using a large number of days may cause your storage costs to go way up.

# COMMAND ----------

spark.sql(f"""ALTER TABLE {powerTable} SET TBLPROPERTIES (delta.deletedFileRetentionDuration="interval 10 days") """)
tblPropDF = spark.sql(f"SHOW TBLPROPERTIES {powerTable}")
display(tblPropDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Run this cell multiple times to show that the data is changing.

# COMMAND ----------

countDF = spark.sql(f"SELECT count(*) FROM {powerTable}")
display(countDF)

# COMMAND ----------

historyDF = spark.sql(f"SELECT timestamp FROM (DESCRIBE HISTORY {powerTable}) ORDER BY timestamp")
display(historyDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Let's rewind back to almost the beginning (where we had just a handful of rows), let's say the 2nd write.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/second-write.png" style="height: 250px"/></div><br/>

# COMMAND ----------

# List timestamps of when table writes occurred
historyDF = spark.sql(f"SELECT timestamp FROM (DESCRIBE HISTORY {powerTable}) ORDER BY timestamp")

# Pick out 2nd write
oldTimestamp = historyDF.take(2)[-1].timestamp

# Re-build the DataFrame as it was in the 2nd write
rewoundDF = spark.sql(f"SELECT * FROM {powerTable} TIMESTAMP AS OF '{oldTimestamp}'")

# COMMAND ----------

# MAGIC %md
# MAGIC We had this many (few) rows back then.

# COMMAND ----------

rewoundDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all remaining streams.

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> All done!</h2>
# MAGIC 
# MAGIC Thank you for your participation!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
