# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations - Append
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Append new records to a Databricks Delta table
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from
# MAGIC * `/mnt/training/online_retail` in the demo part and
# MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

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
# MAGIC ## Refresh Base Data Set

# COMMAND ----------

inputPath = "/mnt/training/online_retail/data-001/data.csv"
inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"
parquetDataPath  = workingDir + "/customer-data/"

(spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("Country")
  .save(parquetDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create table out of base data set

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS {}.customer_data 
    USING parquet 
    OPTIONS (path = '{}')
  """.format(databaseName, parquetDataPath))

spark.sql("MSCK REPAIR TABLE {}.customer_data".format(databaseName))

# COMMAND ----------

# MAGIC %md
# MAGIC The original count of records is:

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM {}.customer_data".format(databaseName)
origCount = spark.sql(sqlCmd).first()[0]

print(origCount)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Some New Data

# COMMAND ----------

inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"
miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Do a simple count of number of new items to be added to production data.

# COMMAND ----------

newDataDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPEND Using Non-Databricks Delta pipeline
# MAGIC 
# MAGIC Append the new data to `parquetDataPath`.

# COMMAND ----------

(newDataDF
  .write
  .format("parquet")
  .partitionBy("Country")
  .mode("append")
  .save(parquetDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's count the rows in `customer_data`.
# MAGIC 
# MAGIC We expect to see `36` additional rows, but we do not.
# MAGIC 
# MAGIC Why not?
# MAGIC 
# MAGIC You will get the same count of old vs new records because the metastore doesn't know about the addition of new records yet.

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM {}.customer_data".format(databaseName)
newCount = spark.sql(sqlCmd).first()[0]
print("The old count of records is {}".format(origCount))
print("The new count of records is {}".format(newCount))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema-on-Read Problem Revisited
# MAGIC 
# MAGIC We've added new data the metastore doesn't know about.
# MAGIC 
# MAGIC * It knows there is a `Sweden` partition, 
# MAGIC   - but it doesn't know about the 19 new records for `Sweden` that have come in.
# MAGIC * It does not know about the new `Sierra-Leone` partition, 
# MAGIC  - nor the 17 new records for `Sierra-Leone` that have come in.
# MAGIC 
# MAGIC Here are the the original table partitions:

# COMMAND ----------

sqlCmd = "SHOW PARTITIONS {}.customer_data".format(databaseName)

originalSet = spark.sql(sqlCmd).collect()

for x in originalSet: 
  print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC Here are the partitions the new data belong to:

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS {}.mini_customer_data".format(databaseName))
newDataDF.write.partitionBy("Country").saveAsTable("{}.mini_customer_data".format(databaseName))

sqlCmd = "SHOW PARTITIONS {}.mini_customer_data ".format(databaseName)

newSet = set(spark.sql(sqlCmd).collect())

for x in newSet: 
  print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC In order to get correct counts of records, we need to make these new partitions and new data known to the metadata.
# MAGIC 
# MAGIC To do this, we apply `MSCK REPAIR TABLE`.

# COMMAND ----------

sqlCmd = "MSCK REPAIR TABLE {}.customer_data".format(databaseName)
spark.sql(sqlCmd)

# COMMAND ----------

# MAGIC %md
# MAGIC Count the number of records:
# MAGIC * The count should be correct now.
# MAGIC * That is, 65499 + 36 = 65535

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM {}.customer_data".format(databaseName)
print(spark.sql(sqlCmd).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh Base Data Set, Write to Databricks Delta

# COMMAND ----------

deltaDataPath  = workingDir + "/customer-data-delta/"

(spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(deltaDataPath) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPEND Using Databricks Delta Pipeline
# MAGIC 
# MAGIC Next, repeat the process by writing to Databricks Delta format. 
# MAGIC 
# MAGIC In the next cell, load the new data in Databricks Delta format and save to `../delta/customer-data-delta/`.

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(deltaDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a simple `count` query to verify the number of records and notice it is correct and does not first require a table repair.
# MAGIC 
# MAGIC Should have 36 more entries from before.

# COMMAND ----------

sqlCmd = "SELECT count(*) FROM delta.`{}` ".format(deltaDataPath)
print(spark.sql(sqlCmd).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## More Options?
# MAGIC 
# MAGIC Additional Databricks Delta Reader and Writer options are included in the [Extra folder]($./Extra/Delta 01E - RW-Options).

# COMMAND ----------

# MAGIC %md
# MAGIC # LAB

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1
# MAGIC 
# MAGIC 0. Apply the schema provided under the variable `jsonSchema`
# MAGIC 0. Read the JSON data under `streamingEventPath` into a DataFrame
# MAGIC 0. Add a `date` column using `to_date(from_unixtime(col("time"),"yyyy-MM-dd"))`
# MAGIC 0. Add a `deviceId` column consisting of random numbers from 0 to 99 using this expression `expr("cast(rand(5) * 100 as int)")`
# MAGIC 0. Use the `repartition` method to split the data into 200 partitions
# MAGIC 
# MAGIC Refer to  <a href="http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#" target="_blank">Pyspark function documentation</a>.

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr, col, from_unixtime, to_date
jsonSchema = "action string, time long"
streamingEventPath = "/mnt/training/structured-streaming/events/"

rawDataDF = (spark
 .read 
  FILL_IN
 .repartition(200)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schema = str(rawDataDF.schema)
dbTest("assert-1", True, "action,StringType" in schema)
dbTest("assert-2", True, "time,LongType" in schema)
dbTest("assert-3", True, "date,DateType" in schema)
dbTest("assert-4", True, "deviceId,IntegerType" in schema)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2
# MAGIC 
# MAGIC Write out the raw data.
# MAGIC * Use `overwrite` mode
# MAGIC * Use format `delta`
# MAGIC * Partition by `date`
# MAGIC * Save to `deltaIotPath`

# COMMAND ----------

# TODO
deltaIotPath = workingDir + "/iot-pipeline/"

(rawDataDF
 .write
  FILL_IN
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
spark.sql("""
  CREATE TABLE IF NOT EXISTS {}.iot_data_delta
  USING DELTA
  LOCATION '{}' """.format(databaseName, deltaIotPath))

try:
  tableExists = (spark.table("{}.iot_data_delta".format(databaseName)).count() > 0)
except:
  tableExists = False
  
dbTest("Delta-02-backfillTableExists", True, tableExists)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3
# MAGIC 
# MAGIC Create a new DataFrame with columns `action`, `time`, `date` and `deviceId`. The columns contain the following data:
# MAGIC 
# MAGIC * `action` contains the value `Open`
# MAGIC * `time` contains the Unix time cast into a long integer `cast(1529091520 as bigint)`
# MAGIC * `date` contains `cast('2018-06-01' as date)`
# MAGIC * `deviceId` contains a random number from 0 to 499 given by `expr("cast(rand(5) * 500 as int)")`

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr

newDF = (spark.range(10000) 
  .repartition(200)
  .selectExpr("'Open' as action", FILL_IN) 
  .FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
total = newDF.count()

dbTest("Delta-03-newDF-count", 10000, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4
# MAGIC 
# MAGIC Append new data to `deltaIotPath`
# MAGIC 
# MAGIC * Use `append` mode
# MAGIC * Use format `delta`
# MAGIC * Partition by `date`
# MAGIC * Save to `deltaIotPath`

# COMMAND ----------

# TODO
(newDF
 .write
FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
numFiles = spark.sql("SELECT count(*) as total FROM delta.`{}` ".format(deltaIotPath)).first()[0]

dbTest("Delta-03-numFiles", 110000 , numFiles)

print("Tests passed!")

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
# MAGIC In this Lesson we:
# MAGIC * Encountered the schema-on-read problem when appending new data in a traditional data lake pipeline.
# MAGIC * Learned how to append new data to existing Databricks Delta data (that mitigates the above problem).
# MAGIC * Showed how to look at the set of partitions in the data set.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Upsert]($./Delta 04 - Upsert).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Delta Table Batch Read and Writes</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
