// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Delta Optimizations and Best Practices
// MAGIC 
// MAGIC Databricks&reg; Delta has nifty optimizations to speed up your queries.
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Optimize a Databricks Delta data pipeline backed by online shopping data
// MAGIC * Learn about best practices to apply to data pipelines
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers 
// MAGIC * Secondary Audience: Data Analysts and Data Scientists
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: **Chrome**
// MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
// MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
// MAGIC   - ETL Part 1
// MAGIC   - Spark-SQL
// MAGIC 
// MAGIC ## Datasets Used
// MAGIC * Online retail datasets from
// MAGIC `/mnt/training/online_retail`

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
// MAGIC 
// MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
// MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC Set up relevant paths.

// COMMAND ----------

val deltaDataPath = workingDir + "/customer-data-delta/"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## SMALL FILE PROBLEM
// MAGIC 
// MAGIC Historical and new data is often written in very small files and directories. 
// MAGIC 
// MAGIC This data may be spread across a data center or even across the world (that is, not co-located).
// MAGIC 
// MAGIC The result is that a query on this data may be very slow due to
// MAGIC * network latency 
// MAGIC * volume of file metatadata 
// MAGIC 
// MAGIC The solution is to compact many small files into one larger file.
// MAGIC Databricks Delta has a mechanism for compacting small files.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC 
// MAGIC Use Azure Data Explorer to see many small files.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Data Explorer is available ONLY on Azure (not in Databricks)
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Delta/azure-small-file.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px"/></div>

// COMMAND ----------

// MAGIC %md
// MAGIC ### OPTIMIZE
// MAGIC Databricks Delta supports the `OPTIMIZE` operation, which performs file compaction.
// MAGIC 
// MAGIC Small files are compacted together into new larger files up to 1GB.
// MAGIC 
// MAGIC `OPTIMIZE` does not do any kind of file clean up, so, at this point the number of files increases!
// MAGIC 
// MAGIC The 1GB size was determined by the Databricks optimization team as a trade-off between query speed and run-time performance when running Optimize.
// MAGIC 
// MAGIC `OPTIMIZE` is not run automatically because you must collect many small files first.
// MAGIC 
// MAGIC * Run `OPTIMIZE` more often if you want better end-user query performance 
// MAGIC * Since `OPTIMIZE` is a time consuming step, run it less often if you want to optimize cost of compute hours
// MAGIC * To start with, run `OPTIMIZE` on a daily basis (preferably at night when spot prices are low), and determine the right frequency for your particular business case
// MAGIC * In the end, the frequency at which you run `OPTIMIZE` is a business decision
// MAGIC 
// MAGIC The easiest way to see what `OPTIMIZE` does is to perform a simple `count(*)` query before and after and compare the timing!

// COMMAND ----------

// MAGIC %md
// MAGIC ### Repopulate Data Set
// MAGIC 
// MAGIC You may have deleted the files created in previous lessons.
// MAGIC 
// MAGIC We re-create them for you.

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, from_unixtime, to_date}
val jsonSchema = "action string, time long"
val streamingEventPath = "/mnt/training/structured-streaming/events/"
val deltaIotPath  = workingDir + "/iot-pipeline/"

val rawDataDF = spark
  .read 
  .schema(jsonSchema)
  .json(streamingEventPath) 
  .withColumn("date", to_date(from_unixtime($"time".cast("Long"),"yyyy-MM-dd")))
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
  .repartition(200)
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("date")
  .save(deltaIotPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Take a look at a subdirectory of `deltaIotPath`.
// MAGIC 
// MAGIC Notice, hundreds of files like `../delta/iot-pipeline/date=xxxx-xx-xx/part-xxxx.snappy.parquet`.

// COMMAND ----------

try {
  dbutils.fs.ls(dbutils.fs.ls(deltaIotPath)(1).path).mkString("\n")
} catch {
  case e: Exception => println("There are no files in deltaIotPath")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Pick a `deviceId` then run the `SELECT` query. 
// MAGIC 
// MAGIC Notice it is very slow, due to the large number of small files.

// COMMAND ----------

val devID = spark.sql("SELECT deviceId FROM delta.`%s` limit 1".format(deltaIotPath)).first()(0).asInstanceOf[Int]
val iotDF = spark.sql("SELECT * FROM delta.`%s` where deviceId=%s".format(deltaIotPath, devID))
display(iotDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Partition Pruning, Data Skipping and ZORDER
// MAGIC 
// MAGIC Databricks Delta uses multiple mechanisms to speed up queries.
// MAGIC 
// MAGIC <b>Partition Pruning</b> is a performance optimization that speeds up queries by limiting the amount of data read.
// MAGIC 
// MAGIC If the WHERE clause filters on a table partitioned column, then only table partitions (sub-directories) that may have matching records are read.  
// MAGIC 
// MAGIC For example, we have a data set that is partitioned by `date`. 
// MAGIC 
// MAGIC A query using `WHERE date > 2018-06-01` would not access data that resides in partitions that correspond to dates prior to `2018-06-01`.  
// MAGIC 
// MAGIC <b>Data Skipping</b> is a performance optimization that aims at speeding up queries that contain filters (WHERE clauses). 
// MAGIC 
// MAGIC As new data is inserted into a Databricks Delta table, file-level min/max statistics are collected for all columns (including nested ones) of supported types. Then, when there’s a lookup query against the table, Databricks Delta first consults these statistics in order to determine which files can safely be skipped.
// MAGIC 
// MAGIC <b>ZOrdering</b> is a technique to colocate related information in the same set of files. 
// MAGIC 
// MAGIC ZOrdering maps multidimensional data to one dimension while preserving locality of the data points. 
// MAGIC 
// MAGIC Given a column that you want to perform ZORDER on, say `OrderColumn`, Delta
// MAGIC * Takes existing parquet files within a partition.
// MAGIC * Maps the rows within the parquet files according to `OrderColumn` using <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">this algorithm</a>.
// MAGIC * In the case of only one column, the mapping above becomes a linear sort.
// MAGIC * Rewrites the sorted data into new parquet files.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You cannot use the table partition column also as a ZORDER column.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### ZORDER example
// MAGIC In the image below, table `Students` has 4 columns: 
// MAGIC * `gender` with 2 distinct values
// MAGIC * `Pass-Fail` with 2 distinct values
// MAGIC * `Class` with 4 distinct values  
// MAGIC * `Student` with many distinct values 
// MAGIC 
// MAGIC Suppose you wish to perform the following query:
// MAGIC 
// MAGIC ```SELECT Name FROM Students WHERE gender = 'M' AND Pass_Fail = 'P' AND Class = 'Junior'```
// MAGIC 
// MAGIC ```ORDER BY Gender, Pass_Fail```
// MAGIC 
// MAGIC The most effective way of performing that search is to order the data starting with the largest set, which is `Gender` in this case. 
// MAGIC 
// MAGIC If you're searching for `gender = 'M'`, then you don't even have to look at students with `gender = 'F'`. 
// MAGIC 
// MAGIC Note that this technique is only beneficial if all `gender = 'M'` values are co-located.
// MAGIC 
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/zorder.png" style="height: 300px"/></div><br/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### ZORDER usage
// MAGIC 
// MAGIC With Databricks Delta the notation is:
// MAGIC 
// MAGIC > `OPTIMIZE Students`<br>
// MAGIC `ZORDER BY Gender, Pass_Fail`
// MAGIC 
// MAGIC This will ensure all the data backing `Gender = 'M' ` is colocated, then data associated with `Pass_Fail = 'P' ` is colocated.
// MAGIC 
// MAGIC See References below for more details on the algorithms behind ZORDER.
// MAGIC 
// MAGIC Using ZORDER, you can order by multiple columns as a comma separated list; however, the effectiveness of locality drops.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In streaming, where incoming events are inherently ordered (more or less) by event time, use `ZORDER` to sort by a different column, say 'userID'.

// COMMAND ----------

spark.sql("""OPTIMIZE delta.`%s` 
             ZORDER by (deviceID)""".format(deltaIotPath))

// COMMAND ----------

// MAGIC %md
// MAGIC The performance of the following query should now be much faster than it was before. 

// COMMAND ----------

val deviceDF = spark.sql("SELECT * FROM delta.`%s` WHERE deviceId=%s".format(deltaIotPath, devID))
display(deviceDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## VACUUM
// MAGIC 
// MAGIC To save on storage costs you should occasionally clean up invalid files using the `VACUUM` command. 
// MAGIC 
// MAGIC Invalid files are small files compacted into a larger file with the `OPTIMIZE` command.
// MAGIC 
// MAGIC The  syntax of the `VACUUM` command is 
// MAGIC >`VACUUM name-of-table RETAIN number-of HOURS;`
// MAGIC 
// MAGIC The `number-of` parameter is the <b>retention interval</b>, specified in hours.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Databricks does not recommend you set a retention interval shorter than seven days because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table.
// MAGIC 
// MAGIC The scenario here is:
// MAGIC 0. User A starts a query off uncompacted files, then
// MAGIC 0. User B invokes a `VACUUM` command, which deletes the uncompacted files
// MAGIC 0. User A's query fails because the underlying files have disappeared
// MAGIC 
// MAGIC Invalid files can also result from updates/upserts/deletions.
// MAGIC 
// MAGIC More details are provided here: <a href="https://docs.databricks.com/delta/optimizations.html#garbage-collection" target="_blank"> Garbage Collection</a>.
// MAGIC 
// MAGIC Count the number of files before we vacuum.

// COMMAND ----------

try {
  dbutils.fs.ls(dbutils.fs.ls(deltaIotPath)(1).path).length
} catch {
  case e: Exception => println("There are no files in deltaIotPath")
}

// COMMAND ----------

// MAGIC %md
// MAGIC In the example below we set off an immediate `VACUUM` operation with an override of the retention check so that all files are cleaned up immediately.
// MAGIC 
// MAGIC You would not do not do this in production!
// MAGIC 
// MAGIC If you do not set the override of the retention check, you get a helpful message.
// MAGIC 
// MAGIC ```
// MAGIC requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have
// MAGIC writers that are currently writing to this table, there is a risk that you may corrupt the
// MAGIC state of your Delta table.
// MAGIC 
// MAGIC If you are certain that there are no operations being performed on this table, such as
// MAGIC insert/upsert/delete/optimize, then you may turn off this check by setting:
// MAGIC spark.databricks.delta.retentionDurationCheck.enabled = false
// MAGIC ```

// COMMAND ----------

try {
  spark.sql(""" VACUUM delta.`%s` RETAIN 0 HOURS """.format(deltaIotPath))
} catch {
  case e:Exception => println(e)
}

// COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)

spark.sql(" VACUUM delta.`%s` RETAIN 0 HOURS ".format(deltaIotPath))

// COMMAND ----------

// MAGIC %md
// MAGIC Notice how the directory looks vastly cleaned up!

// COMMAND ----------

try {
  dbutils.fs.ls(dbutils.fs.ls(deltaIotPath)(1).path).length
} catch {
  case e: Exception => println("There are no files in deltaIotPath")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # LAB

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Step 1: Repopulate Data
// MAGIC 
// MAGIC If you've deleted the data under `deltaDataPath = workingDir + "/customer-data-delta/"` in previous lessons, that is okay.
// MAGIC 
// MAGIC If the path no longer exists or has been cleaned out, repopulate data, otherwise, do nothing.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We are chaining the `read` and `write` operations in one statement.

// COMMAND ----------

val inputPath = "/mnt/training/online_retail/data-001/data.csv"
val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"
val deltaDataPath = workingDir + "/customer-data-delta/"

spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(deltaDataPath) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 2: Time an Unoptimized Query by StockCode
// MAGIC 
// MAGIC Let's apply some of these optimizations to the `customer_data_delta` table.
// MAGIC 
// MAGIC Our data is partitioned by `Country`.
// MAGIC 
// MAGIC We want to query the data for `StockCode` equal to `22301`. 
// MAGIC 
// MAGIC We expect this query to be slow because we have to examine ALL OF the underlying data in `customer_data_delta` to find the desired `StockCode`. 
// MAGIC 
// MAGIC The data could be found on servers all over the world! That is, data can be coming from replications in other zones.
// MAGIC 
// MAGIC First, let's time the above query: you will need to form a DataFrame to pass to `preZorderQuery`.
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC  In Scala, we have to write our own little timing function `def timeIt` as below.
// MAGIC 
// MAGIC Note that  `timeIt` takes a function as input, so you need to define `myQuery` as a function.

// COMMAND ----------

// TODO
val deltaDataPath = workingDir + "/customer-data-delta/"

def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

def myQuery = spark.sql("FILL_IN".format(deltaDataPath)).collect()

val preTime = timeIt(myQuery)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 3: OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Let's apply some of Databricks Delta's optimizations to `customer_data_delta`.
// MAGIC 
// MAGIC Our data is partitioned by `Country`.
// MAGIC 
// MAGIC Compact the files and re-order by `StockCode`.

// COMMAND ----------

// TODO
spark.sql("FILL_IN".format(deltaDataPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 4: Time an Optimized Query by StockCode
// MAGIC 
// MAGIC Let's time the above query again: use the same methodology as for the pre-optimized query.
// MAGIC 
// MAGIC We expect `postTime` to be smaller than `preTime`.
// MAGIC 
// MAGIC Recall, you defined `myQuery` previously.

// COMMAND ----------

// TODO
val postTime = timeIt(FILL_IN)

// COMMAND ----------

// TEST - Run this cell to test your solution.
println("Pre ZORDER time is %s s".format(preTime))
println("Post ZORDER time is %s s".format(postTime))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 5: Apply VACUUM
// MAGIC 
// MAGIC Make sure you set the retention period to 0 to perform the operation immediately.
// MAGIC 
// MAGIC There should be only 1 file in each `Country` partition.

// COMMAND ----------

// TODO
spark.sql("FILL_IN".format(deltaDataPath))

// COMMAND ----------

// TEST - Run this cell to test your solution.
var numFilesOne = 0
try {
  numFilesOne = dbutils.fs.ls(deltaDataPath)
    .filter(p => !p.path.contains("_delta_log"))
    .map(p => (computeFileStats(p.path)._1).toInt)
    .reduce(_ & _ & 1)
} catch {
  case e: Exception => numFilesOne = -99
}

dbTest("Delta-08-numFilesOne", 1, numFilesOne)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
// MAGIC 
// MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Cleanup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Summary
// MAGIC In this lesson, we showed you how to use Databricks Delta's advanced optimization strategies:
// MAGIC * ZORDER uses an algorithm to rewrite parquet files so that related data is co-located
// MAGIC * OPTIMIZE compacts small files into larger files of around 1GB (and solves the small file problem)
// MAGIC * VACUUM deletes the smaller files that were used to form the larger files by OPTIMIZE

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC Start the next lesson, [Architecture]($./Delta 07 - Architecture ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/delta/optimizations.html#" target="_blank">Optimizing Performance and Cost</a>
// MAGIC * <a href="http://parquet.apache.org/documentation/latest/" target="_blank">Parquet Metadata</a>
// MAGIC * <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">Z-Order Curve</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
