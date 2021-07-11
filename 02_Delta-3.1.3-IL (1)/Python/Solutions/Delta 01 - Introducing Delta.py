# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Before You Start</h2>
# MAGIC 
# MAGIC Before starting this course, you will need to create a cluster and attach it to this notebook.
# MAGIC 
# MAGIC Please configure your cluster to use Databricks Runtime version **6.2** which includes:
# MAGIC - Python Version 3.x
# MAGIC - Scala Version 2.11
# MAGIC - Apache Spark 2.4.4
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use an ML or GPU accelerated runtimes
# MAGIC 
# MAGIC Step-by-step instructions for creating a cluster are included here:
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-azure" target="_blank">Azure Databricks</a>
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-aws" target="_blank">Databricks on AWS</a>
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-ce" target="_blank">Databricks Community Edition (CE)</a>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This courseware has been tested against the specific DBR listed above. Using an untested DBR may yield unexpected results and/or various errors. If the required DBR has been deprecated, please <a href="https://academy.databricks.com/" target="_blank">download an updated version of this course</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC In general, all courses are designed to run on one of the following Databricks platforms:
# MAGIC * Databricks Community Edition (CE)
# MAGIC * Databricks (an AWS hosted service)
# MAGIC * Azure-Databricks (an Azure-hosted service)
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Some features are not available on the Community Edition, which limits the ability of some courses to be executed in that environment. Please see the course's prerequisites for specific information on this topic.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Additionally, private installations of Databricks (e.g., accounts provided by your employer) may have other limitations imposed, such as aggressive permissions and or language restrictions such as prohibiting the use of Scala which will further inhibit some courses from being executed in those environments.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** All courses provided by Databricks Academy rely on custom variables, functions, and settings to provide you with the best experience possible.
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # The Challenge with Data Lakes
# MAGIC ### Or, it's not a Data Lake, it's a Data Swamp
# MAGIC 
# MAGIC 
# MAGIC A <b>Data Lake</b>: 
# MAGIC * Is a storage repository that inexpensively stores a vast amount of raw data in its native format.
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * May contain operational relational databases with live transactional data.
# MAGIC * In effect, it's a dumping ground of amorphous data.
# MAGIC 
# MAGIC To extract meaningful information out of a Data Lake, we need to resolve problems like:
# MAGIC * Schema enforcement when new tables are introduced 
# MAGIC * Table repairs when any new data is inserted into the data lake
# MAGIC * Frequent refreshes of metadata 
# MAGIC * Bottlenecks of small file sizes for distributed computations
# MAGIC * Difficulty re-sorting data by an index (i.e. userID) if data is spread across many files and partitioned by i.e. eventTime

# COMMAND ----------

# MAGIC %md
# MAGIC # The Solution: Databricks Delta
# MAGIC 
# MAGIC Databricks Delta is a unified data management system that brings reliability and performance (10-100x faster than Spark on Parquet) to cloud data lakes.  Delta's core abstraction is a Spark table with built-in reliability and performance optimizations.
# MAGIC 
# MAGIC You can read and write data stored in Databricks Delta using the same familiar Apache Spark SQL batch and streaming APIs you use to work with Hive tables or DBFS directories. Databricks Delta provides the following functionality:<br><br>
# MAGIC 
# MAGIC * <b>ACID transactions</b> - Multiple writers can simultaneously modify a data set and see consistent views.
# MAGIC * <b>DELETES/UPDATES/UPSERTS</b> - Writers can modify a data set without interfering with jobs reading the data set.
# MAGIC * <b>Automatic file management</b> - Data access speeds up by organizing data into large files that can be read efficiently.
# MAGIC * <b>Statistics and data skipping</b> - Reads are 10-100x faster when statistics are tracked about the data in each file, allowing Delta to avoid reading irrelevant information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC During the course of this lesson, files, tables, and other artifacts may have been created.
# MAGIC 
# MAGIC These resources create clutter, consume resources (generally in the form of storage), and may potentially incur some [minor] long-term expense.
# MAGIC 
# MAGIC You can remove these artifacts by running the **`Classroom-Cleanup`** cell below.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lesson, [Create]($./Delta 02 - Create).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
