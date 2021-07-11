// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Dummy-Data-Generator-Test
// MAGIC The purpose of this notebook is to faciliate testing of the dummy data generator.

// COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

val courseType = "test"
val moduleName = getModuleName()
val lessonName = getLessonName()
val username = getUsername()
val userhome = getUserhome()
val workingDir = getWorkingDir(courseType)
val databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

// COMMAND ----------

// MAGIC %run ./Dummy-Data-Generator

// COMMAND ----------

// Initialize the username
spark.conf.set("com.databricks.training.username", "test")
spark.conf.set("com.databricks.training.userhome", "dbfs:/user/test")

// COMMAND ----------

// name: String = "Name", 
// id: String = "ID", 
// password: String = "Password", 
// amount: String = "Amount", 
// percent: String = "Percent", 
// probability: String = "Probability", 
// yesNo: String = "YesNo", 
// UTCTime: String = "UTCTime"
import org.apache.spark.sql.types.{StringType, DoubleType, IntegerType, BooleanType, LongType}

// default rows
val testDefaultRowsDF = new DummyData("test_1_scala").toDF
assert(testDefaultRowsDF.count == 300)

// custom rows
val testCustomRowsDF = new DummyData("test_2_scala", numRows=50).toDF
assert(testCustomRowsDF.count == 50)

// custom field names
val testCustomField1DF = new DummyData("test_3_scala").addNames("EmployeeName").toDF
val fields1 = testCustomField1DF.schema.names 
assert(fields1.contains("EmployeeName"))
assert(testCustomField1DF.schema.fields(1).dataType == StringType)

val testCustomField2DF = new DummyData("test_4_scala").renameId("EmployeeID").toDF
val fields2 = testCustomField2DF.schema.names 
assert(fields2.contains("EmployeeID"))
assert(testCustomField2DF.schema.fields(0).dataType == LongType)

val testCustomField3DF = new DummyData("test_5_scala").addPasswords("Yak").toDF
val fields3 = testCustomField3DF.schema.names
assert(testCustomField3DF.schema.fields(1).dataType == StringType)
assert(fields3.contains("Yak"))

val testCustomField4DF = new DummyData("test_6_scala").addDoubles("Salary").toDF
val fields4 = testCustomField4DF.schema.names 
assert(testCustomField4DF.schema.fields(1).dataType == DoubleType)
assert(fields4.contains("Salary"))

val testCustomField5DF = new DummyData("test_7_scala").addIntegers("ZeroTo100").toDF
val fields5 = testCustomField5DF.schema.names 
assert(testCustomField5DF.schema.fields(1).dataType == IntegerType)
assert(fields5.contains("ZeroTo100"))

val testCustomField6DF = new DummyData("test_8_scala").addProportions("Prob").toDF
val fields6 = testCustomField6DF.schema.names 
assert(testCustomField6DF.schema.fields(1).dataType == DoubleType)
assert(fields6.contains("Prob")) 

val testCustomField7DF = new DummyData("test_9_scala").addBooleans("isOK").toDF
val fields7 = testCustomField7DF.schema.names 
assert(testCustomField7DF.schema.fields(1).dataType == BooleanType)
assert(fields7.contains("isOK"))

val testCustomField8DF = new DummyData("test_10_scala").addTimestamps("Timestamp").toDF
val fields8 = testCustomField8DF.schema.names 
assert(testCustomField8DF.schema.fields(1).dataType == IntegerType)
assert(fields8.contains("Timestamp"))

// all custome field names, custom rows
val testCustomFieldsDF = new DummyData("test_11_scala", numRows=50)
    .addNames("EmployeeName")
    .renameId("EmployeeID")
    .addPasswords("Yak")
    .addDoubles("Salary")
    .addIntegers("ZeroTo100")
    .addProportions("Prob")
    .addBooleans("isOK")
    .addTimestamps("Timestamp")
    .toDF
val fields = testCustomFieldsDF.schema.names
assert(fields.deep == Array("EmployeeID", "EmployeeName", "Yak", "Salary", "ZeroTo100", "Prob", "isOK", "Timestamp").deep)
assert(testCustomFieldsDF.count == 50)
assert(testCustomFieldsDF.schema.fields(0).dataType == LongType)
assert(testCustomFieldsDF.schema.fields(1).dataType == StringType)
assert(testCustomFieldsDF.schema.fields(2).dataType == StringType)
assert(testCustomFieldsDF.schema.fields(3).dataType == DoubleType)
assert(testCustomFieldsDF.schema.fields(4).dataType == IntegerType)
assert(testCustomFieldsDF.schema.fields(5).dataType == DoubleType)
assert(testCustomFieldsDF.schema.fields(6).dataType == BooleanType)
assert(testCustomFieldsDF.schema.fields(7).dataType == IntegerType)

// needs to be done: restructure testing format to match class-utility-methods
// needs to be done: add unit testing for the DummyData.add*() methods and their parameters
// needs to be done: add value testing

// COMMAND ----------

spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")

// COMMAND ----------


