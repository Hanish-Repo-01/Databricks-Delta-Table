# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Integration Tests
# MAGIC The purpose of this notebook is to faciliate testing of our systems.

# COMMAND ----------

import os

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

currentVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
print(currentVersion)

spark.conf.set("com.databricks.training.expected-dbr", currentVersion)

# COMMAND ----------

# MAGIC %run ./Common

# COMMAND ----------

allDone(courseAdvertisements)

