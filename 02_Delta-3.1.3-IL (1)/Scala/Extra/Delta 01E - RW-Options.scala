// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## What are the configuration options for a Delta readers and writers?
// MAGIC 
// MAGIC <pre style="color:black; font-weight:normal; font-family:Courier New; font-size:14px; margin-left:15px">
// MAGIC myDF = spark.read.option("config-property","config-value")
// MAGIC                   .format("delta")
// MAGIC                   .load("/path/to/delta-table")
// MAGIC                   
// MAGIC spark.write.option("config-property","config-value")
// MAGIC                   .format("delta")
// MAGIC                   .save("/path/to/delta-dir")`               
// MAGIC </pre>
// MAGIC 
// MAGIC | Option Name 	| Type 	| Default Value 	| Reading/ Writing  	| Description 	| Docs link 	|
// MAGIC |----------------------	|---------	|---------------	|-------------------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|--------------------------------------------------------------------------------------------------------------------	|
// MAGIC | `replaceWhere` 	| String 	| none 	| Writing 	| An option to overwrite only the data that matches predicates over partition columns. You can selectively overwrite only the data that matches predicates over partition columns. The following option atomically replaces the month of January with the dataframe data: `.option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'")` 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#overwrite-using-dataframes) 	|
// MAGIC | `mergeSchema` 	| Boolean 	| † 	| Writing 	| An option to allow automatic schema merging during a write operation. Automerging is off when table ACLs are enabled. Columns that are present in the DataFrame but missing from the table are automatically added as part of a write transaction when either this option is `true` or when property `spark.databricks.delta.schema.autoMerge` is set to `true`. Cannot be used with `INSERT INTO` or `.write.insertInto()`. 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#automatic-schema-update) 	|
// MAGIC | `overwriteSchema` 	| Boolean 	| False 	| Writing 	| An option to allow overwriting schema and partitioning during an overwrite write operation. If ACLs are enabled, we can't change the schema of an operation through a write, which requires MODIFY permissions, when schema changes require OWN permissions. By default, overwriting the data in a table does not overwrite the schema. When overwriting a table using `mode("overwrite")` without `replaceWhere`, you may still want to override the schema of the data being written. You can choose to replace the schema and partitioning of the table by setting this to true. 	| [Docs](https://docs.databricks.com/delta/delta-batch.html#replace-table-schema) 	|
// MAGIC | `maxFilesPerTrigger` 	| Integer 	| 1000 	| Reading 	| For streaming: This specifies the maximum number of new files to be considered in every trigger. Must be a positive integer and default is 1000. 	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#delta-table-as-a-stream-source) 	|
// MAGIC | `ignoreFileDeletion` 	| ------ 	| ------ 	|  	| For streaming: **Deprecated** as of [4.1](https://docs.databricks.com/release-notes/runtime/4.1.html#deprecations). Use `ignoreDeletes` or `ignoreChanges`. 	| [Docs](https://docs.databricks.com/release-notes/runtime/4.1.html#deprecations) 	|
// MAGIC | `ignoreDeletes` 	| Boolean 	| False 	| Reading 	|  For streaming: Ignore transactions that delete data at partition boundaries. For example, if your source table is partitioned by date, and you delete data older than 30 days, the deletion will not be propagated downstream, but the stream can continue to operate.   	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes)  |
// MAGIC | `ignoreChanges` 	| Boolean 	| False 	| Reading 	| For streaming: Re-process updates if files had to be rewritten in the source table due to a data changing operation such as `UPDATE`, `MERGE INTO`, `DELETE` (within partitions), or `OVERWRITE`. Unchanged rows may still be emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not propagated downstream. Option `ignoreChanges` subsumes `ignoreDeletes`. Therefore if you use `ignoreChanges`, your stream will not be disrupted by either deletions or updates to the source table. 	| [Docs](https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes) 	|
// MAGIC | `optimizeWrite` 	| Boolean 	| False 	| Writing 	| Whether to add an adaptive shuffle before writing out the files to break skew and coalesce data into chunkier files. 	| [Docs](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/DeltaOptions.scala) 	|
// MAGIC 
// MAGIC † Default value derived from `sqlConf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)`

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
