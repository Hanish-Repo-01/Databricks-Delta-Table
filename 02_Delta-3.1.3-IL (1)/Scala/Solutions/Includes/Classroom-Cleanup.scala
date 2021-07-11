// Databricks notebook source

spark.conf.set("spark.sql.streaming.stopTimeout", 2*60*1000)
classroomCleanup(daLogger, courseType, username, moduleName, lessonName, true)

// COMMAND ----------

showStudentSurvey()

