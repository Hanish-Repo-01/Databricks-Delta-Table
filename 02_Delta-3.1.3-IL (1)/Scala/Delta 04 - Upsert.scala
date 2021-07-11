// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Delta Batch Operations - Upsert
// MAGIC 
// MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Use Databricks Delta to UPSERT data into existing Databricks Delta tables
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
// MAGIC We will use online retail datasets from
// MAGIC * `/mnt/training/online_retail` in the demo part and
// MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

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

val deltaMiniDataPath = f"${workingDir}/customer-data-mini"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## UPSERT 
// MAGIC 
// MAGIC The term is derived from the two terms "**UP**date" and "in**SERT**". 
// MAGIC 
// MAGIC It means to insert a row, or, if the row already exists, to update the row atomically.
// MAGIC 
// MAGIC In the Delta API, this operation is called **MERGE INTO**.  

// COMMAND ----------

// MAGIC %md
// MAGIC Alter the data by changing the values in one of the columns for a specific `CustomerID`.
// MAGIC 
// MAGIC Let's load the CSV file `/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv`.

// COMMAND ----------

val miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"
val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

val miniDataDF = spark.read      
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)                            

// COMMAND ----------

// MAGIC %md
// MAGIC ## UPSERT Using Non-Databricks Delta Pipeline
// MAGIC 
// MAGIC This feature is not supported in non-Delta pipelines.
// MAGIC 
// MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is not an atomic operation. It is literally TWO operations. 
// MAGIC 
// MAGIC Running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## UPSERT Using Databricks Delta Pipeline
// MAGIC 
// MAGIC Using Databricks Delta, however, we can do UPSERTS.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In this Lesson, we will explicitly create tables as SQL notation works better with UPSERT.

// COMMAND ----------

miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save(deltaMiniDataPath) 

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_data_delta_mini
    USING DELTA 
    LOCATION "${deltaMiniDataPath}" 
  """) 

// COMMAND ----------

// MAGIC %md
// MAGIC List all rows with `CustomerID=20993`.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

// COMMAND ----------

// MAGIC %md
// MAGIC Form a new DataFrame where `StockCode` is `99999` for `CustomerID=20993`.
// MAGIC 
// MAGIC Create a table `customer_data_delta_to_upsert` that contains this data.

// COMMAND ----------

import org.apache.spark.sql.functions.lit
val customerSpecificDF = miniDataDF
  .filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))

customerSpecificDF.write.saveAsTable("customer_data_delta_to_upsert")

// COMMAND ----------

// MAGIC %md
// MAGIC Upsert the new data into `customer_data_delta_mini`.
// MAGIC 
// MAGIC Upsert is done using the `MERGE INTO` syntax.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO customer_data_delta_mini
// MAGIC USING customer_data_delta_to_upsert
// MAGIC ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC     customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
// MAGIC   VALUES (
// MAGIC     customer_data_delta_to_upsert.InvoiceNo,
// MAGIC     customer_data_delta_to_upsert.StockCode, 
// MAGIC     customer_data_delta_to_upsert.Description, 
// MAGIC     customer_data_delta_to_upsert.Quantity, 
// MAGIC     customer_data_delta_to_upsert.InvoiceDate, 
// MAGIC     customer_data_delta_to_upsert.UnitPrice, 
// MAGIC     customer_data_delta_to_upsert.CustomerID, 
// MAGIC     customer_data_delta_to_upsert.Country)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta_mini`.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

// COMMAND ----------

// MAGIC %md
// MAGIC # LAB

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Step 1
// MAGIC 
// MAGIC Write base data to `deltaIotPath`.
// MAGIC 
// MAGIC We do this for you, so just run the cell below.

// COMMAND ----------

import org.apache.spark.sql.functions.{expr, from_unixtime, to_date}
val jsonSchema = "action string, time long"
val streamingEventPath = "/mnt/training/structured-streaming/events/"
val deltaIotPath = f"${workingDir}/iot-pipeline"

spark.read 
  .schema(jsonSchema)
  .json(streamingEventPath) 
  .withColumn("date", to_date(from_unixtime($"time".cast("Long"),"yyyy-MM-dd")))
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
  .write
  .mode("overwrite")
  .format("delta")
  .save(deltaIotPath)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Step 2
// MAGIC 
// MAGIC Create a DataFrame out of the the data sitting in `deltaIotPath`.

// COMMAND ----------

// TODO
val deltaIotPath = f"${workingDir}/iot-pipeline"

val newDataSql = "FILL_IN".format(deltaIotPath)

val newDataDF = spark.sql(newDataSql)

// COMMAND ----------

// TEST - Run this cell to test your solution.
val schema = newDataDF.schema.mkString(",")

dbTest("assert-1", true, schema.contains("action,StringType"))
dbTest("assert-2", true, schema.contains("time,LongType"))
dbTest("assert-3", true, schema.contains("date,DateType"))
dbTest("assert-4", true, schema.contains("deviceId,IntegerType"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 3
// MAGIC Create a DataFrame that has one record with the **`action`** set to **`Close`**.
// MAGIC 
// MAGIC The end result of this should be a DataFrame with one record in it for a pre-exisitng **`deviceId`**.
// MAGIC 
// MAGIC ### Step 3.A
// MAGIC * Return the first row you see that has **`action`** set to **`Open`**.
// MAGIC * Extract the value from the **`deviceId`** column and then assign it to **`devId`**
// MAGIC 
// MAGIC ### Step 3.B
// MAGIC * Create a new DataFrame, **`newDeviceIdDF`**, from **`newDataDF`**, that contains only the matching **`deviceId`** from step 3.A
// MAGIC * Change **`action`** to **`Close`** with the **`lit()`** function
// MAGIC * Restrict **`newDeviceIdDF`** to 1 and only 1 record with the **`limit()`** operation
// MAGIC * We will use the associated **`devId`** in the cells that follow
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC We will then use this for our upsert operation later.

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions.lit

Step 3.A
val devId = newDataDF         // Start with our existing dataset
  .FILL_IN                    // Reduce the dataset to only records where "action"=="Open"
  .FILL_IN                    // Take the first record, and from that, the column "deviceId"

Step 3.B
val newDeviceIdDF = newDataDF // Start with our existing dataset
  .FILL_IN                    // Reduce the dataset to the matching device ID
  .FILL_IN                    // Replace the action column with the literal value "Close"

display(newDeviceIdDF)

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val actionCount = newDeviceIdDF.filter($"Action" === "Close").count()

dbTest("Delta-L4-actionCount", 1, actionCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 4
// MAGIC 
// MAGIC Write to a new Databricks Delta table named **`iot_data_delta_to_upsert`** that contains just our data to be upserted.
// MAGIC 0. Start with the DataFrame **`newDeviceIdDF`**
// MAGIC 0. From the DataFrame, get the DataFrameWriter
// MAGIC 0. Use the correct "save" operation, passing to it the table name, 

// COMMAND ----------

// TODO
val tableName = "iot_data_delta_to_upsert"

newDeviceIdDF
  .FILL_IN
  .FILL_IN(FILL_IN)

// COMMAND ----------

// TEST - Run this cell to test your solution.
val count = spark.read.table("iot_data_delta_to_upsert").count()

dbTest("Delta-04-demoIotTableHasRow", true, count > 0)  

println("Tests passed!")
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 5
// MAGIC 
// MAGIC Create a Databricks Delta table named `demo_iot_data_delta` that contains just the data from `deltaIotPath`.

// COMMAND ----------

// TODO
sqlCmd = f"""
  CREATE FILL_IN
  USING FILL_IN
  FILL_IN $"{deltaIotPath}"
"""

spark.sql(sqlCmd)

// COMMAND ----------

// TEST - Run this cell to test your solution.
var tableExists = try {
  spark.read.table("demo_iot_data_delta").count() > 0
} catch {
  case e: Exception => false
}

dbTest("Delta-04-demoTableExists", true, tableExists)  

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 6
// MAGIC 
// MAGIC Insert the data `iot_data_delta_to_upsert` into `demo_iot_data_delta`.
// MAGIC 
// MAGIC You can adapt the SQL syntax for the upsert from our demo example above.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC 
// MAGIC MERGE FILL_IN
// MAGIC USING FILL_IN
// MAGIC ON FILL_IN
// MAGIC WHEN MATCHED THEN FILL_IN
// MAGIC WHEN NOT MATCHED FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
val devId = newDeviceIdDF.select("deviceId").first().getAs[Int]("deviceId")

val sqlCmd1 = f"SELECT count(*) as total FROM demo_iot_data_delta WHERE deviceId = ${devId} AND action = 'Open' "
val countOpen = spark.sql(sqlCmd1).first().getAs[Long]("total")

val sqlCmd2 = f"SELECT count(*) as total FROM demo_iot_data_delta WHERE deviceId = ${devId} AND action = 'Close' "
val countClose = spark.sql(sqlCmd2).first().getAs[Long]("total")

dbTest("Delta-L4-count", true, countOpen == 0 && countClose > 0)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 7
// MAGIC 
// MAGIC Count the number of items in `demo_iot_data_delta` where 
// MAGIC * `deviceId` is obtained from this query `newDeviceIdDF.select("deviceId").first()(0)` .
// MAGIC * `action` is `Close`.

// COMMAND ----------

// TODO

val sqlCmd = f"""
  SELECT FILL_IN 
  FROM FILL_IN 
  WHERE FILL_IN = ${devId} AND 
        FILL_IN 
"""

val count = spark.sql(sqlCmd).first()(0).asInstanceOf[Long]

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-L4-demoiot-count", true, count > 0)

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
// MAGIC In this Lesson we:
// MAGIC * Learned that is not possible to do UPSERTS in the traditional pre-Databricks Delta lake.
// MAGIC   - UPSERT is essentially two operations in one step 
// MAGIC   - UPdate and inSERT
// MAGIC * `MERGE INTO` is the SQL expression we use to do UPSERTs.
// MAGIC * Used Databricks Delta to UPSERT data into existing Databricks Delta tables.
// MAGIC * Ended up creating tables explicitly because it is easier to work with SQL syntax.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC Start the next lesson, [Streaming]($./Delta 05 - Streaming).

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
