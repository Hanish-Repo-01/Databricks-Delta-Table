// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Course Agenda
// MAGIC ## DB 200 - Managed Delta Lakes
// MAGIC ### Day-1 AM
// MAGIC 
// MAGIC <table>
// MAGIC <tr><td>30 m</td><td>Setup</td><td><i>Student and instructor introductions, roll call, setup.</i></td></tr>
// MAGIC <tr><td>20 m</td><td>[Introducing Delta]($../Delta 01 - Introducing Delta)</td><td><i>What is Delta? Why use Delta? </i></td></tr>
// MAGIC <tr><td>10 m</td><td>Break</td><td><i></i></td></tr>
// MAGIC <tr><td>30 m</td><td>[Create]($../Delta 02 - Create)</td><td><i>Write to (and Create) Delta tables</i></td></tr>
// MAGIC <tr><td>20 m</td><td>[Append]($../Delta 03 - Append)</td><td><i>Append to existing Delta tables</i></td></tr>
// MAGIC <tr><td>10 m</td><td>Break</td><td><i></i></td></tr>
// MAGIC <tr><td>30 m</td><td>[Upsert]($../Delta 04 - Upsert)</td><td><i>Use Databricks Delta to atomically insert a row, or, if the row already exists, update the row</i></td></tr>
// MAGIC <tr><td>30 m</td><td>[Streaming]($../Delta 05 - Streaming)</td><td><i>Read and write streaming data into and from Delta tables</i></td></tr>
// MAGIC </table>
// MAGIC 
// MAGIC ### Day-1 PM
// MAGIC 
// MAGIC <table>
// MAGIC <tr><td>30 m</td><td>[Optimization]($../Delta 06 - Optimization)</td><td><i>Use Delta's advanced features to speed up your queries</i></td></tr>
// MAGIC <tr><td>20 m</td><td>[Architecture]($../Delta 07 - Architecture)</td><td><i>Apply Delta to seamlessly processes batch and streaming data: The Bronze-Silver-Gold Pipeline</i></td></tr>
// MAGIC <tr><td>10 m</td><td>Break</td><td><i></i></td></tr>
// MAGIC <tr><td>50 m</td><td>[Time Travel]($../Delta 08 - Time Travel)</td><td><i>Apply Delta Time Travel to revert to an older version of a dataset</i></td></tr>
// MAGIC <tr><td>10 m</td><td>Break</td><td><i></i></td></tr>
// MAGIC <tr><td>90 m</td><td>[Capstone Project]($../Delta 99 - Capstone Project)</td><td><i>Hands-on practice with a capstone project</i></td></tr>
// MAGIC <tr><td>30 m</td><td>Q&A</td><td><i>Questions & Answers, class wrap-up</i></td></tr>
// MAGIC </table>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
