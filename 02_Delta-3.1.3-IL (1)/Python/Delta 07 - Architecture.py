# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Architecture
# MAGIC Databricks&reg; Delta simplifies data pipelines and eliminates the need for the traditional Lambda architecture.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Get streaming Wikipedia data into a data lake via Kafka broker
# MAGIC * Write streaming data into a <b>raw</b> table
# MAGIC * Clean up bronze data and generate normalized <b>query</b> tables
# MAGIC * Create <b>summary</b> data of key business metrics
# MAGIC * Create plots/dashboards of business metrics
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analyst and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC * Read Wikipedia edits in real time, with a multitude of different languages. 
# MAGIC * Aggregate the anonymous edits by country, over a window, to see who's editing the English Wikipedia over time. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
# MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Lambda Architecture
# MAGIC 
# MAGIC The Lambda architecture is a big data processing architecture that combines both batch and real-time processing methods.
# MAGIC It features an append-only immutable data source that serves as system of record. Timestamped events are appended to 
# MAGIC existing events (nothing is overwritten). Data is implicitly ordered by time of arrival. 
# MAGIC 
# MAGIC Notice how there are really two pipelines here, one batch and one streaming, hence the name <i>lambda</i> architecture.
# MAGIC 
# MAGIC It is very difficult to combine processing of batch and real-time data as is evidenced by the diagram below.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/lambda.png" style="height: 400px"/></div><br/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks Delta Architecture
# MAGIC 
# MAGIC The Databricks Delta Architecture is a vast improvement upon the traditional Lambda architecture.
# MAGIC 
# MAGIC Text files, RDBMS data and streaming data is all collected into a <b>raw</b> table (also known as "bronze" tables at Databricks).
# MAGIC 
# MAGIC A Raw table is then parsed into <b>query</b> tables (also known as "silver" tables at Databricks). They may be joined with dimension tables.
# MAGIC 
# MAGIC <b>Summary</b> tables (also known as "gold" tables at Databricks) are business level aggregates often used for reporting and dashboarding. 
# MAGIC This would include aggregations such as daily active website users.
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The word **table** is used very loosely here: really, it means **data**.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/delta.png" style="height: 350px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Delta Architecture
# MAGIC 
# MAGIC We use terminology 
# MAGIC * "bronze" (instead of "raw"), 
# MAGIC * "silver" (instead of "query"), 
# MAGIC * "gold" (instead of "summary"), 
# MAGIC * "platinum" (another level of refinement)
# MAGIC 
# MAGIC This is not standard in the industry.

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

bronzePath           = workingDir + "/wikipedia/bronze.delta"
bronzeCheckpointPath = workingDir + "/wikipedia/bronze.checkpoint"

silverPath           = workingDir + "/wikipedia/silver.delta"
silverCheckpointPath = workingDir + "/wikipedia/silver.checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC And to help us manage our streams better, we will make use of **`untilStreamIsReady()`**, **`stopAllStreams()`** and define the following, **`bronzeStreamName`**, **`silverStreamName`** and **`goldStreamName`**:

# COMMAND ----------

bronzeStreamName = "bronze_stream_pi"
silverStreamName = "silver_stream_pi"
goldStreamName = "gold_stream_pi"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to RAW table (aka "bronze table")
# MAGIC 
# MAGIC <b>Raw data</b> is unaltered data that is collected into a data lake, either via bulk upload or through streaming sources.
# MAGIC 
# MAGIC The following function reads the Wikipedia IRC channels that has been dumped into our Kafka server.
# MAGIC 
# MAGIC The Kafka server acts as a sort of "firehose" and dumps raw data into our data lake.
# MAGIC 
# MAGIC Below, the first step is to set up schema. The fields we use further down in the notebook are commented.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

schema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
  ]), True),
  StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
  StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
  StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
  StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Next, stream into bronze Databricks Delta directory.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we are invoking the `.start(path)` method. 
# MAGIC 
# MAGIC This is so that the data is streamed into the path we want (and not a default directory).

# COMMAND ----------

from pyspark.sql.functions import from_json, col
(spark.readStream
  .format("kafka")  
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")  # Oregon
  #.option("kafka.bootstrap.servers", "server2.databricks.training:9092") # Singapore
  .option("subscribe", "en")
  .load()
  .withColumn("json", from_json(col("value").cast("string"), schema))
  .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", bronzeCheckpointPath)
  .outputMode("append")
  .queryName(bronzeStreamName)
  .start(bronzePath)
)

# COMMAND ----------

# Wait until the stream is done initializing...
untilStreamIsReady(bronzeStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look the first row of the raw table without explicitly creating a table.

# COMMAND ----------

bronzeDF = spark.sql(f"SELECT * FROM delta.`{bronzePath}` limit 1")
display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create QUERY tables (aka "silver tables")
# MAGIC 
# MAGIC Notice how `WikipediaEditsRaw` has JSON encoding. For example `{"city":null,"country":null,"countryCode2":null,"c..`
# MAGIC 
# MAGIC In order to be able parse the data in human-readable form, create query tables out of the raw data using columns<br>
# MAGIC `wikipedia`, `isAnonymous`, `namespace`, `page`, `pageURL`, `geocoding`, `timestamp` and `user`.
# MAGIC 
# MAGIC Stream into a Databricks Delta query directory.

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

(spark.readStream
  .format("delta")
  .load(bronzePath)
  .select(col("wikipedia"),
          col("isAnonymous"),
          col("namespace"),
          col("page"),
          col("pageURL"),
          col("geocoding"),
          unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").alias("timestamp"),
          col("user"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", silverCheckpointPath)
  .outputMode("append")
  .queryName(silverStreamName)
  .start(silverPath)
)

# COMMAND ----------

# Wait until the stream is done initializing...
untilStreamIsReady(silverStreamName)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Take a peek at the streaming query view without explicitly creating tables.
# MAGIC 
# MAGIC Notice how the fields are more meaningful than the fields in the bronze data set.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice that we are explicitly creating a DataFrame. This is so we can pass it to the `display` function.

# COMMAND ----------

silverDF = spark.sql("SELECT * FROM delta.`{}` limit 3".format(silverPath))
display(silverDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SUMMARY (aka "gold") level data 
# MAGIC 
# MAGIC Summary queries can take a long time.
# MAGIC 
# MAGIC Instead of running the below query off the data under `silverPath`, let's create a summary query.
# MAGIC 
# MAGIC We are interested in a breakdown of which countries that are producing anonymous edits.

# COMMAND ----------

from pyspark.sql.functions import col, desc, count

goldDF = (spark.readStream
  .format("delta")
  .load(silverPath)
  .withColumn("countryCode", col("geocoding.countryCode3"))
  .filter(col("namespace") == "article")
  .filter(col("countryCode") != "null")
  .filter(col("isAnonymous") == True)
  .groupBy(col("countryCode"))
  .count() 
  .withColumnRenamed("count", "total")
  .orderBy(col("total").desc())
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Creating Visualizations (aka "platinum" level) 
# MAGIC 
# MAGIC #### Mapping Anonymous Editors' Locations
# MAGIC 
# MAGIC Use that geocoding information to figure out the countries associated with the editors.
# MAGIC 
# MAGIC When you run the query, the default is a (live) html table.
# MAGIC 
# MAGIC In order to create a slick world map visualization of the data, you'll need to click on the item below.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `countryCode`
# MAGIC * <b>Values:</b> `total`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>World Map</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/plot-options-world-map.png"/></div><br/> 
# MAGIC 
# MAGIC By invoking a `display` action on a DataFrame created from a `readStream` transformation, we can generate a LIVE visualization!
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Visualizations (aka "platinum" level) 
# MAGIC 
# MAGIC LIVE means you can see the colors change if you watch the plot.

# COMMAND ----------

display(goldDF, streamName = goldStreamName)

# COMMAND ----------

# Wait until the stream is done initializing...
untilStreamIsReady(goldStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC When you are all done, make sure to stop all the streams.

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
# MAGIC ## Summary
# MAGIC In this lesson we:
# MAGIC * Learned about the Databricks Delta (reference) architecture.
# MAGIC * Used the Databricks Delta architecture to craft bronze, silver, gold and platinum queries.
# MAGIC * Produced beautiful visualizations of key business metrics.
# MAGIC * Did not have to explicitly create tables along the way.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="http://lambda-architecture.net/#" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Reading structured streams from Kafka</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
# MAGIC * <a href="https://docs.databricks.com/delta/delta-intro.html#case-study-multi-hop-pipelines#" target="_blank">Multi Hop Pipelines</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Time Travel]($./Delta 08 - Time Travel ).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
