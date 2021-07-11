// Databricks notebook source

spark.conf.set("com.databricks.training.module-name", "delta")
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

displayHTML("Preparing the learning environment...")

// COMMAND ----------

// MAGIC %run "./Common-Notebooks/Common"

// COMMAND ----------

allDone(courseAdvertisements)

