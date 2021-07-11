# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Dummy-Data-Generator-Test
# MAGIC The purpose of this notebook is to faciliate testing of the dummy data generator.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

courseType = "test"
moduleName = getModuleName()
lessonName = getLessonName()
username = getUsername()
userhome = getUserhome()
workingDir = getWorkingDir(courseType)
databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

# COMMAND ----------

# MAGIC %run ./Dummy-Data-Generator

# COMMAND ----------

# name: String = "Name", 
# id: String = "ID", 
# password: String = "Password", 
# amount: String = "Amount", 
# percent: String = "Percent", 
# probability: String = "Probability", 
# yesNo: String = "YesNo", 
# UTCTime: String = "UTCTime"
from pyspark.sql import Row
from pyspark.sql.types import *
import os

# default rows
testDefaultRowsDF = DummyData("test_1_python").toDF()
assert testDefaultRowsDF.count() == 300

# custom rows
testCustomRowsDF = DummyData("test_2_python", numRows=50).toDF()
assert testCustomRowsDF.count() == 50

# custom field names
testCustomField1DF = DummyData("test_3_python").addNames("EmployeeName").toDF()
fields1 = testCustomField1DF.schema.names 
assert "EmployeeName" in fields1
assert testCustomField1DF.schema.fields[1].dataType == StringType()

testCustomField2DF = DummyData("test_4_python").renameId("EmployeeID").toDF()
fields2 = testCustomField2DF.schema.names 
assert "EmployeeID" in fields2
assert testCustomField2DF.schema.fields[0].dataType == LongType()

testCustomField3DF = DummyData("test_5_python").addPasswords("Yak").toDF()
fields3 = testCustomField3DF.schema.names
assert testCustomField3DF.schema.fields[1].dataType == StringType()
assert "Yak" in fields3

testCustomField4DF = DummyData("test_6_python").addDoubles("Salary").toDF()
fields4 = testCustomField4DF.schema.names 
assert testCustomField4DF.schema.fields[1].dataType == DoubleType()
assert "Salary" in fields4

testCustomField5DF = DummyData("test_7_python").addIntegers("ZeroTo100").toDF()
fields5 = testCustomField5DF.schema.names 
assert testCustomField5DF.schema.fields[1].dataType == IntegerType()
assert "ZeroTo100" in fields5

testCustomField6DF = DummyData("test_8_python").addProportions("Prob").toDF()
fields6 = testCustomField6DF.schema.names 
assert testCustomField6DF.schema.fields[1].dataType == DoubleType()
assert "Prob" in fields6 

testCustomField7DF = DummyData("test_9_python").addBooleans("isOK").toDF()
fields7 = testCustomField7DF.schema.names 
assert testCustomField7DF.schema.fields[1].dataType == BooleanType()
assert "isOK" in fields7

testCustomField8DF = DummyData("test_10_python").addTimestamps("Timestamp").toDF()
fields8 = testCustomField8DF.schema.names 
assert testCustomField8DF.schema.fields[1].dataType == IntegerType()
assert "Timestamp" in fields8

# all custome field names, custom rows
testCustomFieldsDF = (DummyData("test_11_python", numRows=50)
    .addNames("EmployeeName")
    .renameId("EmployeeID")
    .addPasswords("Yak")
    .addDoubles("Salary")
    .addIntegers("ZeroTo100")
    .addProportions("Prob")
    .addBooleans("isOK")
    .addTimestamps("Timestamp")
    .toDF())
fields = testCustomFieldsDF.schema.names
assert fields == ['EmployeeID', 'EmployeeName', 'Yak', 'Salary', 'ZeroTo100', 'Prob', 'isOK', 'Timestamp']
assert testCustomFieldsDF.count() == 50
assert testCustomFieldsDF.schema.fields[0].dataType == LongType()
assert testCustomFieldsDF.schema.fields[1].dataType == StringType()
assert testCustomFieldsDF.schema.fields[2].dataType == StringType()
assert testCustomFieldsDF.schema.fields[3].dataType == DoubleType()
assert testCustomFieldsDF.schema.fields[4].dataType == IntegerType()
assert testCustomFieldsDF.schema.fields[5].dataType == DoubleType()
assert testCustomFieldsDF.schema.fields[6].dataType == BooleanType()
assert testCustomFieldsDF.schema.fields[7].dataType == IntegerType()

# needs to be done: restructure testing format to match class-utility-methods
# needs to be done: add unit testing for the DummyData.add*() methods and their parameters
# needs to be done: add value testing

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")

