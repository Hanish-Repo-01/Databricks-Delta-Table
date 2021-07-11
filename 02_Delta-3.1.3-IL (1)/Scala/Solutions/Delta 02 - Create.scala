// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Delta Batch Operations - Create Table
// MAGIC 
// MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Work with a traditional data pipeline using online shopping data
// MAGIC * Identify problems with the traditional data pipeline
// MAGIC * Use Databricks Delta features to mitigate those problems
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
// MAGIC We will use online retail datasets from `/mnt/training/online_retail` 

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
// MAGIC ### Getting Started
// MAGIC 
// MAGIC You will notice that throughout this course, there is a lot of context switching between PySpark, Scala and SQL.
// MAGIC 
// MAGIC This is because:
// MAGIC * `read` and `write` operations are performed on DataFrames using PySpark or Scala
// MAGIC * table creates and queries are performed directly off Databricks Delta tables using SQL
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %md
// MAGIC Set up relevant paths.

// COMMAND ----------

val inputPath = "/mnt/training/online_retail/data-001/data.csv"

val parquetDataPath  = workingDir + "/customer-data/"
val deltaDataPath    = workingDir + "/customer-data-delta/"

// COMMAND ----------

// MAGIC %md
// MAGIC ###  READ CSV Data
// MAGIC 
// MAGIC Read the data into a DataFrame. We supply the schema.
// MAGIC 
// MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause.
// MAGIC 
// MAGIC More information on table partitioning is contained in the links at the bottom of this notebook.

// COMMAND ----------

val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

val rawDF = (spark.read 
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath) 
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###  WRITE to Parquet and Databricks Delta
// MAGIC 
// MAGIC Use `overwrite` mode so that it is not a problem to re-write data in case you end up running the cell again.

// COMMAND ----------

// write using Parquet format
rawDF.write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("Country")
  .save(parquetDataPath)

// COMMAND ----------

// write using Databricks Delta format
rawDF.write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(deltaDataPath) 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### CREATE Statement Using Non-Databricks Delta Pipeline
// MAGIC 
// MAGIC Create a table called `customer_data` using `parquet` out of the above data.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we do not need to specify the schema and partition info!

// COMMAND ----------

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS customer_data 
  USING parquet 
  OPTIONS (path = "%s")
""".format(parquetDataPath))

// COMMAND ----------

// MAGIC %md
// MAGIC Perform a simple `count` query to verify the number of records.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*) FROM customer_data

// COMMAND ----------

// MAGIC %md
// MAGIC ### Why 0 records? 
// MAGIC 
// MAGIC It's the concept of
// MAGIC <b>schema on read</b> where data is applied to a plan or schema as it is pulled out of a stored location, rather than as it goes into a stored location.
// MAGIC 
// MAGIC In the traditional data lake architecture (including our pre-Databricks Delta), 
// MAGIC  * The data backing the table **`customer_data`** is located in **`parquetDataPath`** (which you can see below).
// MAGIC  * The paths to the meta data backing the table **`customer-data`** (the schema, partitioning info and other table properties) are stored elsewhere 
// MAGIC   - This is called the **metastore**.
// MAGIC 
// MAGIC Suppose, we add more data to **`parquetDataPath`**, 
// MAGIC  * Then, we need to run a separate step for the metastore to become aware of this.
// MAGIC  * We use the **`MSCK REPAIR TABLE`** command. 
// MAGIC  * **`MSCK`** stands for "**M**eta**S**tore **C**hec**K**", modeled after Unix **`FSCK`** (**F**ile **S**ystem **C**hec**K**)
// MAGIC 
// MAGIC Schema on read is explained in more detail <a href="https://stackoverflow.com/a/11764519/53495#" target="_blank">in this article</a>.

// COMMAND ----------

println(parquetDataPath)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MSCK REPAIR TABLE customer_data

// COMMAND ----------

// MAGIC %md
// MAGIC After using `MSCK REPAIR TABLE`, the count is correct.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*) FROM customer_data

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### CREATE Statement Using Databricks Delta Pipeline
// MAGIC 
// MAGIC Create a table called `<database-name>.customer_data_delta` using `DELTA` out of `<path-to-data> = deltaDataPath`     
// MAGIC 
// MAGIC The notation is:
// MAGIC > `CREATE TABLE IF NOT EXISTS <database-name>.customer_data_delta` <br>
// MAGIC   `USING DELTA` <br>
// MAGIC   `LOCATION <path-to-data> ` <br>
// MAGIC   
// MAGIC Then, perform SQL queries on the table you just created.
// MAGIC > `SELECT count(*) FROM <database-name>.customer_data_delta`
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how you do not have to specify a schema or partition info here:
// MAGIC * Databricks Delta stores schema and partition info in the `_delta_log` directory.
// MAGIC * It infers schema from the data sitting in `<path-to-data>`.

// COMMAND ----------

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS customer_data_delta 
  USING DELTA 
  LOCATION '%s' 
""".format(deltaDataPath))

// COMMAND ----------

// MAGIC %md
// MAGIC Perform a simple `count` query to verify the number of records.
// MAGIC 
// MAGIC Notice how the count is right off the bat; no need to worry about table repairs.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*) FROM customer_data_delta

// COMMAND ----------

// MAGIC %md
// MAGIC ## A New Notation
// MAGIC 
// MAGIC But, there is a more compact notation as well, one where you do not explicitly have to create a table.
// MAGIC 
// MAGIC Simply specify `delta.` along with the path to your Databricks Delta directory (in backticks!) directly in the SQL query.
// MAGIC * The dot in ```delta.`<path>` ``` means "Spark, recognize `<path>` as a Databricks Delta directory"
// MAGIC 
// MAGIC > ```SELECT count(*) FROM delta.`<path-to-Delta-data>` ```
// MAGIC 
// MAGIC We will use this notation extensively throughout the rest of the course.
// MAGIC 
// MAGIC In your own work, you may chose either notation:
// MAGIC * Sometimes, SQL queries are more readable than DataFrame queries.
// MAGIC 
// MAGIC Make sure you use BACKTICKS in the statement ``` delta.`<path-to-Delta-data>` ``` .

// COMMAND ----------

val sqlCmd = "SELECT count(*) FROM delta.`%s` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

// COMMAND ----------

// MAGIC %md
// MAGIC ##  The Transaction Log (Metadata)
// MAGIC Databricks Delta stores the schema, partitioning info and other table properties in the same place as the data:
// MAGIC  * The schema and partition info is located in the `00000000000000000000.json` file under the `_delta_log` directory as shown below.
// MAGIC  * Subsequent `write` operations create additional `json` files.
// MAGIC  * In addition to the schema, the `json` file(s) contain information such as
// MAGIC    - Which files were added.
// MAGIC    - Which files were removed.
// MAGIC    - Transaction IDs.
// MAGIC  * Each Delta table should correspond to a unique `_delta_log` directory.

// COMMAND ----------

dbutils.fs.head(deltaDataPath + "/_delta_log/00000000000000000000.json")

// COMMAND ----------

// MAGIC %md
// MAGIC Metadata is displayed through `DESCRIBE DETAIL <tableName>`.
// MAGIC 
// MAGIC As long as we have some data in place already for a Databricks Delta table, we can infer schema.

// COMMAND ----------

val sqlCmd = "DESCRIBE DETAIL delta.`%s` ".format(deltaDataPath)
display(spark.sql(sqlCmd))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Converting Parquet Workloads to Databricks Delta
// MAGIC 
// MAGIC A Databricks Delta workload is defined by the presence of the `_delta_log` directory containing metadata files.
// MAGIC 
// MAGIC Given a generic Parquet-based data lake, converting to Databricks Delta is quite straightforward.
// MAGIC 
// MAGIC Suppose our Parquet-based data lake is found under `/data-pipeline`.
// MAGIC 
// MAGIC To convert it to Databricks Delta, simply do
// MAGIC 
// MAGIC > ```CONVERT TO DELTA parquet.`/data-pipeline` ``` <br>
// MAGIC   ```[PARTITIONED BY (col_name1 col_type1, col_name2 col_type2, ...)] ```
// MAGIC   
// MAGIC More details in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/convert-to-delta.html" target="_blank">Porting Existing Workloads to Delta</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # LAB

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 1
// MAGIC 
// MAGIC Read in data in `outdoorSmallPath` using `inputSchema` to DataFrame `inventoryDF`.
// MAGIC 
// MAGIC Use appropriate options, given that this is a CSV file.

// COMMAND ----------

// ANSWER
val outdoorSmallPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"
val inputSchema = "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

val inventoryDF = spark
  .read      
  .option("header", "true")
  .schema(inputSchema)
  .csv(outdoorSmallPath)

// COMMAND ----------

// TEST - Run this cell to test your solution.
val inventoryCount = inventoryDF.count()

dbTest("Delta-02-schemas", 99999, inventoryCount)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 2
// MAGIC 
// MAGIC Write data to a Databricks path `inventoryDataPath = workingDir + "/inventory-data/"` 
// MAGIC * Make sure to set the `format` to `delta`
// MAGIC * Use overwrite mode 
// MAGIC * Partititon by `Country`

// COMMAND ----------

// ANSWER
val inventoryDataPath = workingDir + "/inventory-data/"

inventoryDF
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(inventoryDataPath)

// COMMAND ----------

// TEST - Run this cell to test your solution.
var tableNotEmpty = false
try {
  tableNotEmpty = spark.sql("SELECT count(*) FROM delta.`%s` ".format(inventoryDataPath)).first()(0).asInstanceOf[Long] > 0
} catch {
  case e: Exception => tableNotEmpty = false
}

dbTest("Delta-02-inventoryTableExists", true, tableNotEmpty)  

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Step 3
// MAGIC 
// MAGIC Count number of records found under `inventoryDataPath` where the `Country` is `Sweden`.

// COMMAND ----------

// ANSWER
val count = spark.sql("SELECT count(*) as total FROM delta.`%s` WHERE Country='Sweden'".format(inventoryDataPath)).first()(0).asInstanceOf[Long]

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-L2-inventoryDataDelta-count", 2925L, count)
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
// MAGIC In this lesson we learned:
// MAGIC * That Databricks Delta overcomes the schema-on-read problem 
// MAGIC   - where data is applied to a plan or schema as it is pulled out of a stored location, rather than as it goes into a stored location.
// MAGIC * About a compact notation that allows you to work with Databricks Delta data as tables (without having to explicitly create tables).
// MAGIC * How to convert existing Parquet-based workloads to Databricks Delta workloads.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC Start the next lesson, [Append]($./Delta 03 - Append).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>
// MAGIC * <a href="https://en.wikipedia.org/wiki/Partition_(database)#" target="_blank">Database Partitioning</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
