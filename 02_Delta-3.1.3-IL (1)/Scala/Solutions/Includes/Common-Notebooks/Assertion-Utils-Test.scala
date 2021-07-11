// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Assertion-Utils-Test
// MAGIC The purpose of this notebook is to faciliate testing of assertions.

// COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

// MAGIC %md
// MAGIC # TestSuite

// COMMAND ----------

println(s"Score:      ${TestResultsAggregator.score}")
println(s"Max Score:  ${TestResultsAggregator.maxScore}")
println(s"Percentage: ${TestResultsAggregator.percentage}")

println("-"*80)

// COMMAND ----------

val suiteA = TestSuite()
suiteA.test("ScalaTest-1", s"My first scala test",  testFunction = () => true)
suiteA.test("ScalaTest-2", s"My second scala test", testFunction = () => false)
suiteA.test("ScalaTest-3", null,                    testFunction = () => true)
suiteA.test("ScalaTest-4", s"My fourth scala test", testFunction = () => false)

for (testResult <- suiteA.testResults) {
  println(s"${testResult.test.id}: ${testResult.status}")
}
println("-"*80)

println(s"Score:      ${suiteA.score}")
println(s"Max Score:  ${suiteA.maxScore}")
println(s"Percentage: ${suiteA.percentage}")

println("-"*80)

assert(suiteA.score == 2, s"A.score: ${suiteA.score}")
assert(suiteA.maxScore == 4, s"A.maxScmaxScoreore: ${suiteA.maxScore}")
assert(suiteA.percentage == 50, s"A.percentage: ${suiteA.percentage}")

// COMMAND ----------

val suiteB = TestSuite()
suiteB.test("ScalaTest-5", "My fifth scala test",   () => true, points=3)
suiteB.testEquals("ScalaTest-6", "My sixth scala test",   "cat", "doc", points=3)
suiteB.testEquals("ScalaTest-7", "My seventh scala test", 99, 100-1,  points=3)

suiteB.testContains("List-Pass", "A list contains",          Seq("dog","bird","cat"), "cat",  points=3)
suiteB.testContains("List-Fail", "A list does not contain",  Seq("dog","bird","cat"), "cow",  points=3)

suiteB.testFloats("Floats-Pass", "Floats that match",        1.001, 1.002, 0.01,  points=3)
suiteB.testFloats("Floats-Fail", "Floats that do not match", 1.001, 1.002, 0.001, points=3)

val dfA = Seq(("Duck", 10000), ("Mouse", 60000)).toDF("LastName", "MaxSalary")
val dfB = Seq(("Duck", 10000), ("Chicken", 60000)).toDF("LastName", "MaxSalary")
val dfC = Seq(("Duck", 10000), ("Mouse", 60000)).toDF("LastName", "MaxSalary")

suiteB.testRows("Rows-Pass", "Rows that match",        dfA.collect()(0), dfB.collect()(0) )
suiteB.testRows("Rows-Fail", "Rows that do not match", dfA.collect()(1), dfB.collect()(1) )

suiteB.testDataFrames("DF-Pass", "DataFrames that match",        dfA, dfC, true, true)
suiteB.testDataFrames("DF-Fail", "DataFrames that do not match", dfA, dfB, true, true )

//////////////////////////////////////////
// Print the reulsts
println("-"*80)
for (testResult <- suiteB.testResults) {
  println(s"${testResult.test.id}: ${testResult.status} (${testResult.points}/${testResult.test.points})")
}

println("-"*80)

println(s"Score:      ${suiteB.score}")
println(s"Max Score:  ${suiteB.maxScore}")
println(s"Percentage: ${suiteB.percentage}")

println("-"*80)

assert(suiteB.score == 14, s"B.score: ${suiteB.score}")
assert(suiteB.maxScore == 25, s"B.maxScore: ${suiteB.maxScore}")
assert(suiteB.percentage == 56, s"B.percentage: ${suiteB.percentage}")

// COMMAND ----------

val suiteC = TestSuite()

println(s"Score:      ${suiteC.score}")
println(s"Max Score:  ${suiteC.maxScore}")
println(s"Percentage: ${suiteC.percentage}")

println("-"*80)

assert(suiteC.score == 0, s"C.score: ${suiteC.score}")
assert(suiteC.maxScore == 0, s"C.maxScore: ${suiteC.maxScore}")
assert(suiteC.percentage == 0, s"C.percentage: ${suiteC.percentage}")

// COMMAND ----------

suiteA.displayResults()

// COMMAND ----------

suiteB.displayResults()

// COMMAND ----------

suiteC.displayResults()

// COMMAND ----------

for ( (key, testResult) <- TestResultsAggregator.testResults) {
  println(s"${testResult.test.id}: ${testResult.status} (${testResult.points})")
}
println("-"*80)

println(s"Score:      ${TestResultsAggregator.score}")
println(s"Max Score:  ${TestResultsAggregator.maxScore}")
println(s"Percentage: ${TestResultsAggregator.percentage}")

println("-"*80)

// COMMAND ----------

TestResultsAggregator.displayResults

// COMMAND ----------

assert(TestResultsAggregator.score == 16, s"TR.score: ${TestResultsAggregator.score}")
assert(TestResultsAggregator.maxScore == 29, s"TR.maxScore: ${TestResultsAggregator.maxScore}")
assert(TestResultsAggregator.percentage == 55, s"TR.percentage: ${TestResultsAggregator.percentage}")

// COMMAND ----------

val suiteX = TestSuite()

try {
  suiteX.test(null, null, () => true)
  throw new Exception("Expexted an IllegalArgumentException")
} catch {
  case e:IllegalArgumentException => ()
}

try {
  suiteX.test("abc", null, () => true)
  suiteX.test("abc", null, () => true)
  throw new Exception("Expexted an IllegalArgumentException")
} catch {
  case e:IllegalArgumentException => ()
}

// COMMAND ----------

// MAGIC %md
// MAGIC # dbTest()

// COMMAND ----------

dbTest("ScalaTest-1", "cat", "cat")

try {
  dbTest("ScalaTest-2", "cat", "dog")  
} catch {
  case _: AssertionError => ()
}
// dbTest("ScalaTest-3", 999, 666)  

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareFloats
// MAGIC 
// MAGIC ```compareFloats(floatA, floatB, tolerance)```

// COMMAND ----------

assert(compareFloats(1, 1.toInt) == true)

assert(compareFloats(100.001, 100.toInt) == true)
assert(compareFloats(100.001, 100.toLong) == true)
assert(compareFloats(100.001, 100.toFloat) == true)
assert(compareFloats(100.001, 100.toDouble) == true)

assert(compareFloats(100.toInt, 100.001) == true)
assert(compareFloats(100.toLong, 100.001) == true)
assert(compareFloats(100.toFloat, 100.001) == true)
assert(compareFloats(100.toDouble, 100.001) == true)

assert(compareFloats(100.001, "blah") == false)
assert(compareFloats("blah", 100.001) == false)

assert(compareFloats(1.0, 1.0) == true)

assert(compareFloats(1.0, 1.2, .0001) == false)
assert(compareFloats(1.0, 1.02, .0001) == false)
assert(compareFloats(1.0, 1.002, .0001) == false)
assert(compareFloats(1.0, 1.0002, .0001) == false)
assert(compareFloats(1.0, 1.00002, .0001) == true)
assert(compareFloats(1.0, 1.000002, .0001) == true)

assert(compareFloats(1.2, 1.0, .0001) == false)
assert(compareFloats(1.02, 1.0, .0001) == false)
assert(compareFloats(1.002, 1.0, .0001) == false)
assert(compareFloats(1.0002, 1.0, .0001) == false)
assert(compareFloats(1.00002, 1.0, .0001) == true)
assert(compareFloats(1.000002, 1.0, .0001) == true)

assert(compareFloats(1, null) == false)
assert(compareFloats(null, 1) == false)
assert(compareFloats(null, null) == true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareSchemas
// MAGIC 
// MAGIC ```compareSchemas(schemaA, schemaB, testColumnOrder=True, testNullable=False)```

// COMMAND ----------

import org.apache.spark.sql.types._ 
val _expectedSchema = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", DoubleType, true)
  ))

val _studentSchema1 = StructType(List(
  StructField("MaxSalary", DoubleType, true),
  StructField("LastName", StringType, true)
  ))

val _studentSchema2 = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", BooleanType, true)
  ))

val _studentSchema3 = StructType(List(
  StructField("LastName", StringType, false),
  StructField("MaxSalary", DoubleType, true)
))

val _studentSchema4 = StructType(List(
  StructField("LastName", StringType, true),
  StructField("MaxSalary", DoubleType, true),
  StructField("Country", StringType, true)
  ))

assert(compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=false, testNullable=true) == true,  "out of order, ignore order")
assert(compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=true, testNullable=true) == false,  "out of order, preserve order")
assert(compareSchemas(_expectedSchema, _studentSchema2, testColumnOrder=false, testNullable=true) == false, "different types")

assert(compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=true, testNullable=true) == false,  "check nullable")  
assert(compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=true, testNullable=false) == true,  "don't check nullable")

assert(compareSchemas(_expectedSchema, _studentSchema4, testColumnOrder=true, testNullable=true) == false,  "left side < right size")
assert(compareSchemas(_studentSchema4, _expectedSchema, testColumnOrder=true, testNullable=true) == false,  "left side > right side")

assert(compareSchemas(null,            _studentSchema3, testColumnOrder=true, testNullable=true) == false, "Null schemaA")
assert(compareSchemas(null,            null,            testColumnOrder=true, testNullable=true) == true,  "Null schemaA and schemaB")
assert(compareSchemas(_studentSchema2, null,            testColumnOrder=true, testNullable=true) == false, "Null schemaB")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Testing compareRows
// MAGIC 
// MAGIC ```compareRows(rowA, rowB)```

// COMMAND ----------

val bla = Row("Duck", 10000.0)
bla.schema

// COMMAND ----------

val df1 = Seq(("Duck", 10000.0), ("Mouse", 60000.0)).toDF("LastName", "MaxSalary")
val _rowA = df1.collect()(0)
val _rowB = df1.collect()(1)

val df2 = Seq((10000.0, "Duck")).toDF("MaxSalary", "LastName")
val _rowC = df2.collect()(0)

val df3 = Seq(("Duck", 10000.0, "Anaheim")).toDF("LastName", "MaxSalary", "City")
val _rowD = df3.collect()(0)

// no schema!
val _rowE =  Row("Duck", 10000.0)
val _rowF =  Row("Mouse", 60000.0)

assert(compareRows(_rowA, _rowB) == false)                   // different schemas
assert(compareRows(_rowA, _rowA) == true)                    // compare to self
assert(compareRows(_rowA, _rowC) == true)                    // order reversed
assert(compareRows(null, _rowA) == false, "null _rowA")      // Null rowA
assert(compareRows(null, null) == true, "null _rowA, rowB")  // Null rowA and rowB
assert(compareRows(_rowA, null) == false, "null _rowB")      // Null rowB
assert(compareRows(_rowA, _rowD) == false)                   // _rowA smaller than _rowD
assert(compareRows(_rowD, _rowA) == false)                   // _rowD smaller than _rowA 
assert(compareRows(_rowE, _rowF) == false)                   // no schemas, different
assert(compareRows(_rowE, _rowE) == true)                    // no schemas, same
assert(compareRows(_rowE, _rowA) == true)                    // no schemas / schema, same content
assert(compareRows(_rowE, _rowB) == false)                    // no schemas / schema, different content

