# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Gain Actionable Insights from a Data Lake, Satisfy GDPR
# MAGIC 
# MAGIC In this capstone project, use Databricks Delta to manage a data lake consisting of a lot of historical data plus incoming streaming data.
# MAGIC 
# MAGIC A video gaming company stores historical data in a data lake, which is growing exponentially. 
# MAGIC 
# MAGIC The data isn't sorted in any particular way (actually, it's quite a mess).
# MAGIC 
# MAGIC It is proving to be _very_ difficult to query and manage this data because there is so much of it.
# MAGIC 
# MAGIC To further complicate issues, a regulatory agency has decreed you be able to identify and delete all data associated with a specific user (i.e. GDPR). 
# MAGIC 
# MAGIC In other words, you must delete data associated with a specific `deviceId`.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Additional Audiences: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL
# MAGIC   - Structured Streaming
# MAGIC   - Delta
# MAGIC 
# MAGIC ## Instructions
# MAGIC 0. Read in streaming data into Databricks Delta bronze tables
# MAGIC 0. Create Databricks Delta silver table
# MAGIC 0. Compute aggregate statistics about data i.e. create gold table
# MAGIC 0. Identify events associated with specific `deviceId` 
# MAGIC 0. Do data cleanup using Databricks Delta advanced features

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
# MAGIC Set up relevant paths.

# COMMAND ----------

inputPath = "/mnt/training/gaming_data/mobile_streaming_events_b"

outputPathBronze =     workingDir + "/gaming/bronze.delta"
checkpointPathBronze = workingDir + "/gaming/bronze.checkpoint"

outputPathSilver = workingDir + "/gaming/silver.delta"
outputPathGold   = workingDir + "/gaming/gold.delta"

# COMMAND ----------

myStreamName = "capstone_pi"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source
# MAGIC 
# MAGIC The input source is a folder containing files of around 100,000 bytes each and is set up to stream slowly.
# MAGIC 
# MAGIC Run this code to read streaming data in.

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType

eventSchema = ( StructType()
  .add('eventName', StringType()) 
  .add('eventParams', StructType() 
    .add('game_keyword', StringType()) 
    .add('app_name', StringType()) 
    .add('scoreAdjustment', IntegerType()) 
    .add('platform', StringType()) 
    .add('app_version', StringType()) 
    .add('device_id', StringType()) 
    .add('client_event_time', TimestampType()) 
    .add('amount', DoubleType()) 
  )     
)

gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option('streamName','mobilestreaming_demo') 
  .option("maxFilesPerTrigger", 1)
  .json(inputPath) 
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Write Stream
# MAGIC 
# MAGIC The instructions here are to:
# MAGIC 
# MAGIC * Write the stream from `gamingEventDF` to the Databricks Delta data lake in path defined by `outputPathBronze`.
# MAGIC * Convert `client_event_time` to a date format and rename to `eventDate`
# MAGIC * Filter out null `eventDate` 

# COMMAND ----------

# TODO

from pyspark.sql.functions import col, to_date

eventsStream = (gamingEventDF
  .filter(FILL_IN)
  .withColumn(FILL_IN)

  FILL_IN

  .option('checkpointLocation', checkpointPathBronze) 
  .queryName(myStreamName)
  .start(outputPathBronze)
)

# COMMAND ----------

# Wait until the stream is initialized...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC ...then create table `mobile_events_delta_bronze`.

# COMMAND ----------

# TODO
spark.sql("""
   CREATE TABLE IF NOT EXISTS mobile_events_delta_bronze
   FILL_IN
  """)

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  rawTableExists = (spark.table("mobile_events_delta_bronze") is not None)
except:
  rawTableExists = False
  
firstRow = spark.sql("SELECT * FROM mobile_events_delta_bronze").take(1)

dbTest("Delta-08-rawTableExists", True, rawTableExists)  
dbTest("Delta-08-rowsExist", False, not firstRow) 

print(spark.catalog.currentDatabase())
print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3a: Read Existing Data into a Delta table.
# MAGIC 
# MAGIC Create `device_id_type_table` from data in `/mnt/training/gaming_data/dimensionData`.
# MAGIC 
# MAGIC This table associates `deviceId` with `deviceType` = `{android, ios}`.

# COMMAND ----------

# TODO
tablePath = "/mnt/training/gaming_data/dimensionData"

spark.sql(f"""
   CREATE TABLE IF NOT EXISTS device_id_type_table 
   FILL_IN
  """)

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("device_id_type_table") is not None)
except:
  tableExists = False
  
dbTest("Delta-08-tableExists", True, tableExists)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3b: Create a silver table
# MAGIC 
# MAGIC Create table `mobile_events_delta_silver` by joining `device_id_type_table` with `mobile_events_delta_bronze` on `deviceId`.
# MAGIC * Your fields should be `eventName`, `deviceId`, `eventTime`, `eventDate` and `deviceType`.
# MAGIC * Write to `outputPathSilver`

# COMMAND ----------

# TODO
import time
sqlCmd = """
   CREATE TABLE IF NOT EXISTS mobile_events_delta_silver
   FILL_IN
  """

spark.sql(sqlCmd)

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DateType
schema = spark.table("mobile_events_delta_silver").schema

firstRowQuery = spark.sql("SELECT * FROM mobile_events_delta_silver limit 1").collect()[0][0]

dbTest("test-1", True, "eventName,StringType" in str(schema))
dbTest("test-2", True, "deviceId,StringType" in str(schema))
dbTest("test-3", True, "eventTime,TimestampType" in str(schema))
dbTest("test-4", True, "eventDate,DateType" in str(schema))
dbTest("test-5", True, "deviceType,StringType" in str(schema))

dbTest("Delta-08-queryRowsExist", False, not firstRowQuery) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4a: Create a Delta gold table out of silver table
# MAGIC 
# MAGIC The company executives want to look at the number of active users by week.
# MAGIC 
# MAGIC Count number of events in the by week.

# COMMAND ----------

# TODO
spark.sql("""
   CREATE TABLE IF NOT EXISTS mobile_events_delta_gold 
   FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
schema = str(spark.table("mobile_events_delta_gold").schema)

dbTest("test-1", True, "WAU,LongType" in str(schema))
dbTest("test-2", True, "week,IntegerType" in str(schema))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b: Visualization
# MAGIC 
# MAGIC The company executives are visual people: they like pretty charts.
# MAGIC 
# MAGIC Create a bar chart out of `mobile_events_delta_gold` where the horizontal axis is month and the vertical axis is WAU.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `week`
# MAGIC * <b>Values:</b> `WAU`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/eLearning/Delta/plot-options-bar.png"/>

# COMMAND ----------

# TODO
platinumDF = spark.sql("FILL_IN")

# COMMAND ----------

# TODO

display(FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Isolate a specific `deviceId`
# MAGIC 
# MAGIC Identify all the events associated with a specific user, rougly proxied by the first `deviceId` we encounter in our query. 
# MAGIC 
# MAGIC Use the `mobile_events_delta_silver` table.
# MAGIC 
# MAGIC The `deviceId` you come up with should be a string.

# COMMAND ----------

# TODO 
deviceId = str(spark.sql("FILL_IN").collect()[0][0])

# COMMAND ----------

# TEST - Run this cell to test your solution.
deviceIdexists = len(deviceId) > 0

dbTest("Delta-L8-lenDeviceId", True, deviceIdexists)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 6: ZORDER 
# MAGIC 
# MAGIC Implicitly re-order by `deviceId`. 
# MAGIC 
# MAGIC The data pertaining to this `deviceId` is spread out all over the data lake. (It's definitely _not_ co-located!).
# MAGIC 
# MAGIC Pass in the `deviceId` variable you defined above.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> `ZORDER` may take a few minutes.

# COMMAND ----------

spark.sql("OPTIMIZE mobile_events_delta_silver ZORDER BY (deviceId)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Delete Specific `deviceId`
# MAGIC 
# MAGIC 0. Delete rows with that particular `deviceId` from `mobile_events_delta_silver`.
# MAGIC 0. Make sure that `deviceId` is no longer in the table!

# COMMAND ----------

# TODO
spark.sql("FILL_IN".format(deviceId))
noDeviceId = spark.sql(("FILL_IN").format(deviceId)).collect()

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-08-noDeviceId", True , not noDeviceId)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 8: Vacuum
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use a retention of 0 hours in production, as this may affect queries that are currently in flight. 
# MAGIC By default this value is 7 days. 
# MAGIC 
# MAGIC We use 0 hours here for purposes of demonstration only.
# MAGIC 
# MAGIC Recall, we use `VACUUM` to reduce the number of files in each partition directory to 1.

# COMMAND ----------

# Disable the safety check for a retention of 0 hours
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# TODO
spark.sql("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure all directories have 1 file.

# COMMAND ----------

# TEST - Run this cell to test your solution.
from functools import reduce

myList = filter(lambda p: "_delta_log" not in p.path, dbutils.fs.ls(outputPathSilver))   # Pick up list of subdirectories except for _delta_log
myMap = map(lambda p: computeFileStats(p.path)[0], myList)                               # Apply computeFileStats to each path object in list
myFilter = filter(lambda p: p > 0, list(myMap))
numFilesOne = reduce(lambda a, b: a and b and 1, myFilter)                                  # Do bitwise & on every element with eachother and the value 1

dbTest("Delta-08-numFilesOne", 1, numFilesOne)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to stop all your streams

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
