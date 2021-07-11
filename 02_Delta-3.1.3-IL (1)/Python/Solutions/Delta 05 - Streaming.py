# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Databricks Delta Streaming
# MAGIC Databricks&reg; Delta allows you to work with streaming data.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Read and write streaming data into a data lake
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analyst sand Data Scientists
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
# MAGIC Data from 
# MAGIC `/mnt/training/definitive-guide/data/activity-data`
# MAGIC contains smartphone accelerometer samples from all devices and users. 
# MAGIC 
# MAGIC The file consists of the following columns:
# MAGIC 
# MAGIC | Field               | Description                  |
# MAGIC |---------------------|------------------------------|
# MAGIC | **`Arrival_Time`**  | when record came in          |
# MAGIC | **`Creation_Time`** | when record was created      |
# MAGIC | **`Index`**         | unique identifier of event   |
# MAGIC | **`x`**             | acceleration in x-dir        |
# MAGIC | **`y`**             | acceleration in y-dir        |
# MAGIC | **`z`**             | acceleration in z-dir        |
# MAGIC | **`User`**          | unique user identifier       |
# MAGIC | **`Model`**         | i.e Nexus4                   |
# MAGIC | **`Device`**        | type of Model                |
# MAGIC | **`gt`**            | ground truth                 |
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The last column, **`gt`**, "ground truth" means
# MAGIC * What the person was ACTUALLY doing when the measurement was taken, in this case, walking or going up stairs, etc..
# MAGIC * Wikipedia: <a href="https://en.wikipedia.org/wiki/Ground_truth" target="_blank">Ground Truth</a>

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

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Streaming Concepts
# MAGIC 
# MAGIC <b>Stream processing</b> is where you continuously incorporate new data into a data lake and compute results.
# MAGIC 
# MAGIC The data is coming in faster than it can be consumed.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/firehose.jpeg" style="height: 200px"/></div><br/>
# MAGIC 
# MAGIC Treat a <b>stream</b> of data as a table to which data is continously appended. 
# MAGIC 
# MAGIC In this course we are assuming Databricks Structured Streaming, which uses the DataFrame API. 
# MAGIC 
# MAGIC There are other kinds of streaming systems.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/></div><br/>
# MAGIC 
# MAGIC Examples are bank card transactions, Internet of Things (IoT) device data, and video game play events. 
# MAGIC 
# MAGIC Data coming from a stream is typically not ordered in any way.
# MAGIC 
# MAGIC A streaming system consists of 
# MAGIC * <b>Input source</b> such as Kafka, Azure Event Hub, files on a distributed system or TCP-IP sockets
# MAGIC * <b>Sinks</b> such as Kafka, Azure Event Hub, various file formats, **`foreach`** sinks, console sinks or memory sinks
# MAGIC 
# MAGIC ### Streaming and Databricks Delta
# MAGIC 
# MAGIC In streaming, the problems of traditional data pipelines are exacerbated. 
# MAGIC 
# MAGIC Specifically, with frequent meta data refreshes, table repairs and accumulation of small files on a secondly- or minutely-basis!
# MAGIC 
# MAGIC Many small files result because data (may be) streamed in at low volumes with short triggers.
# MAGIC 
# MAGIC Databricks Delta is uniquely designed to address these needs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### READ Stream using Databricks Delta
# MAGIC 
# MAGIC The **`readStream`** method is similar to a transformation that outputs a DataFrame with specific schema specified by **`.schema()`**. 
# MAGIC 
# MAGIC Each line of the streaming data becomes a row in the DataFrame once **`writeStream`** is invoked.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In this lesson, we limit flow of stream to one file per trigger with **`option("maxFilesPerTrigger", 1)`** so that you do not exceed file quotas you may have on your end. The default value is 1000.
# MAGIC 
# MAGIC Notice that nothing happens until you engage an action, i.e. a **`writeStream`** operation a few cells down.
# MAGIC 
# MAGIC Do some data normalization as well:
# MAGIC * Convert **`Arrival_Time`** to **`timestamp`** format.
# MAGIC * Rename **`Index`** to **`User_ID`**.

# COMMAND ----------

static = spark.read.json(dataPath)
dataSchema = static.schema

deltaStreamWithTimestampDF = (spark
  .readStream
  .option("maxFilesPerTrigger", 1)
  .schema(dataSchema)
  .json(dataPath)
  .withColumnRenamed('Index', 'User_ID')
  .selectExpr("*","cast(cast(Arrival_Time as double)/1000 as timestamp) as event_time")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### WRITE Stream using Databricks Delta
# MAGIC 
# MAGIC #### General Notation
# MAGIC Use this format to write a streaming job to a Databricks Delta table.
# MAGIC 
# MAGIC > **`(myDF`** <br>
# MAGIC   **`.writeStream`** <br>
# MAGIC   **`.format("delta")`** <br>
# MAGIC   **`.option("checkpointLocation", somePath)`** <br>
# MAGIC   **`.outputMode("append")`** <br>
# MAGIC   **`.table("my_table")`** or **`.start(path)`** <br>
# MAGIC **`)`**
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you use the **`.table()`** notation, it will write output to a default location. 
# MAGIC * This would be in parquet files under **`/user/hive/warehouse/default.db/my_table`**
# MAGIC 
# MAGIC In this course, we want everyone to write data to their own directory; so, instead, we use the **`.start()`** notation.
# MAGIC 
# MAGIC #### Output Modes
# MAGIC Notice, besides the "obvious" parameters, specify **`outputMode`**, which can take on these values
# MAGIC * **`append`**: add only new records to output sink
# MAGIC * **`complete`**: rewrite full output - applicable to aggregations operations
# MAGIC * **`update`**: update changed records in place
# MAGIC 
# MAGIC #### Checkpointing
# MAGIC 
# MAGIC When defining a Delta streaming query, one of the options that you need to specify is the location of a checkpoint directory.
# MAGIC 
# MAGIC **`.writeStream.format("delta").option("checkpointLocation", <path-to-checkpoint-directory>) ...`**
# MAGIC 
# MAGIC This is actually a structured streaming feature. It stores the current state of your streaming job.
# MAGIC 
# MAGIC Should your streaming job stop for some reason and you restart it, it will continue from where it left off.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If you do not have a checkpoint directory, when the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Also note that every streaming job should have its own checkpoint directory: no sharing.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Let's Do Some Streaming
# MAGIC 
# MAGIC In the cell below, we write streaming query to a Databricks Delta table. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify a schema: it is inferred from the data! 

# COMMAND ----------

# MAGIC %md
# MAGIC And to help us manage our streams better, we will make use of **`untilStreamIsReady()`**, **`stopAllStreams()`** and define the following, **`streamA`** and **`streamB`**:

# COMMAND ----------

streamA = "streamA_pi"
streamB = "streamB_pi"

# COMMAND ----------

writePath =      workingDir + "/output.delta"
checkpointPath = workingDir + "/output.checkpoint"

deltaStreamingQuery = (deltaStreamWithTimestampDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .queryName(streamA)
  .start(writePath)
)

# COMMAND ----------

# Wait until stream is done initializing...
untilStreamIsReady(streamA)

# COMMAND ----------

# MAGIC %md
# MAGIC See list of active streams.

# COMMAND ----------

for s in spark.streams.active:
  print("{}: {}".format(s.name, s.id))

# COMMAND ----------

# MAGIC %md
# MAGIC # LAB

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1: Table-to-Table Stream
# MAGIC 
# MAGIC Here we read a stream of data from from **`writePath`** and write another stream to **`activityPath`**.
# MAGIC 
# MAGIC The data consists of a grouped count of **`gt`** events.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the stream identified by **`deltaStreamingQuery`** is still running!
# MAGIC 
# MAGIC To perform an aggregate operation, what kind of **`outputMode`** should you use?

# COMMAND ----------

# ANSWER
activityPath   = workingDir + "/activityCount.delta"
checkpointPath = workingDir + "/activityCount.checkpoint"

activityCountsQuery = (spark.readStream
  .format("delta")
  .load(str(writePath))   
  .groupBy("gt")
  .count()
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  .queryName(streamB)
  .start(activityPath)
)

# COMMAND ----------

# Wait until stream is done initializing...
untilStreamIsReady(streamB)

# COMMAND ----------

# MAGIC %md
# MAGIC See list of active streams.

# COMMAND ----------

for s in spark.streams.active:
  print("{}: {}".format(s.name, s.id))

# COMMAND ----------

# TEST - Run this cell to test your solution.
activityQueryTruth = spark.streams.get(activityCountsQuery.id).isActive

dbTest("Delta-05-activityCountsQuery", True, activityQueryTruth)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 2
# MAGIC 
# MAGIC Plot the occurrence of all events grouped by **`gt`**.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Series Groupings:</b> **`gt`**
# MAGIC * <b>Values:</b> **`count`**
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/ch5-plot-options.png" style="height: 300px;"/></div><br/>
# MAGIC 
# MAGIC ### Quick Recap
# MAGIC * Stream #1 writes to one location A
# MAGIC * Stream #2 reads from location A, augments and writes to location B
# MAGIC * The following query does a batch query against location B
# MAGIC * Rerun periodically to see updates

# COMMAND ----------

batchDF = spark.read.format("delta").load(activityPath)
display(batchDF)

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
# MAGIC 
# MAGIC In this lesson, we:
# MAGIC * Treated a stream of data as a table to which data is continously appended. 
# MAGIC * Learned we could write Databricks Delta output to a directory or directly to a table.
# MAGIC * Used Databricks Delta's **`complete`** mode with aggregation queries.
# MAGIC * Saw that **`display`** will produce LIVE graphs if we feed it a streaming DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Optimization]($./Delta 06 - Optimization).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html#as-a-sink" target="_blank">Delta Streaming Write Notation</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tagatha Das. This is an excellent video describing how Structured Streaming works.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
