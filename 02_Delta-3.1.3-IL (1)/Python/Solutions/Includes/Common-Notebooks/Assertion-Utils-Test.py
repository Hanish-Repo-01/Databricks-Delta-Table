# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Assertion-Utils-Test
# MAGIC The purpose of this notebook is to faciliate testing of assertions.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

# MAGIC %md
# MAGIC # TestSuite

# COMMAND ----------

print(f"Score:      {TestResultsAggregator.score}")
print(f"Max Score:  {TestResultsAggregator.maxScore}")
print(f"Percentage: {TestResultsAggregator.percentage}")

print("-"*80)

# Once accessed, it must be reset
TestResultsAggregator = __TestResultsAggregator()

# COMMAND ----------

suiteA = TestSuite()
suiteA.test("PythonTest-1", f"My first python test",  lambda: True)
suiteA.test("PythonTest-2", f"My second python test", lambda: False)
suiteA.test("PythonTest-3", None,                     lambda: True)
suiteA.test("PythonTest-4", f"My fourth python test", lambda: False)

for testResult in list(suiteA.testResults):
  print(f"{testResult.test.id}: {testResult.status}")

print("-"*80)

print(f"Score:      {suiteA.score}")
print(f"Max Score:  {suiteA.maxScore}")
print(f"Percentage: {suiteA.percentage}")

print("-"*80)

assert suiteA.score == 2, f"A.score: {suiteA.score}"
assert suiteA.maxScore == 4, f"A.maxScmaxScoreore: {suiteA.maxScore}"
assert suiteA.percentage == 50, f"A.percentage: {suiteA.percentage}"

# COMMAND ----------

suiteB = TestSuite()
suiteB.test("PythonTest-5", "My fifth python test",   lambda: True,  points=3)
suiteB.testEquals("PythonTest-6", "My sixth scala test", "cat", "doc", points=3)
suiteB.testEquals("PythonTest-7", "My seventh scala test", 99, 100-1,  points=3)

suiteB.testContains("List-Pass", "A list contains",          ["dog","bird","cat"], "cat", points=3)
suiteB.testContains("List-Fail", "A list does not contain",  ["dog","bird","cat"], "cow", points=3)

suiteB.testFloats("Floats-Pass", "Floats that match",        1.001, 1.002, 0.01,  points=3)
suiteB.testFloats("Floats-Fail", "Floats that do not match", 1.001, 1.002, 0.001, points=3)

listA = [("Duck", 10000), ("Mouse", 60000)]
dfA = spark.createDataFrame(listA)

listB = [("Duck", 10000), ("Chicken", 60000)]
dfB = spark.createDataFrame(listB)

listC = [("Duck", 10000), ("Mouse", 60000)]
dfC = spark.createDataFrame(listC)

suiteB.testRows("Rows-Pass", "Rows that match",        dfA.collect()[0], dfB.collect()[0] )
suiteB.testRows("Rows-Fail", "Rows that do not match", dfA.collect()[1], dfB.collect()[1] )

suiteB.testDataFrames("DF-Pass", "DataFrames that match",        dfA, dfC, True, True)
suiteB.testDataFrames("DF-Fail", "DataFrames that do not match", dfA, dfB, True, True)

##########################################
# Print the results
for testResult in list(suiteB.testResults):
  print(f"{testResult.test.id}: {testResult.status} ({testResult.points}/{testResult.test.points}): {testResult.exception}")

print("-"*80)

print(f"Score:      {suiteB.score}")
print(f"Max Score:  {suiteB.maxScore}")
print(f"Percentage: {suiteB.percentage}")

print("-"*80)

assert suiteB.score == 14, f"B.score: {suiteB.score}"
assert suiteB.maxScore == 25, f"B.maxScore: {suiteB.maxScore}"
assert suiteB.percentage == 56, f"B.percentage: {suiteB.percentage}"

# COMMAND ----------

suiteC = TestSuite()

print(f"Score:      {suiteC.score}")
print(f"Max Score:  {suiteC.maxScore}")
print(f"Percentage: {suiteC.percentage}")

print("-"*80)

assert suiteC.score == 0, f"C.score: {suiteC.score}"
assert suiteC.maxScore == 0, f"C.maxScore: {suiteC.maxScore}"
assert suiteC.percentage == 0, f"C.percentage: {suiteC.percentage}"

# COMMAND ----------

suiteA.displayResults()

# COMMAND ----------

suiteB.displayResults()

# COMMAND ----------

suiteC.displayResults()

# COMMAND ----------

for key in TestResultsAggregator.testResults:
  testResult = TestResultsAggregator.testResults[key]
  print(f"""{testResult.test.id}: {testResult.status} ({testResult.points})""")

print("-"*80)

print(f"Score:      {TestResultsAggregator.score}")
print(f"Max Score:  {TestResultsAggregator.maxScore}")
print(f"Percentage: {TestResultsAggregator.percentage}")

print("-"*80)

# COMMAND ----------

TestResultsAggregator.displayResults()

# COMMAND ----------

assert TestResultsAggregator.score == 16, f"TR.score: {TestResultsAggregator.score}"
assert TestResultsAggregator.maxScore == 29, f"TR.maxScore: {TestResultsAggregator.maxScore}"
assert TestResultsAggregator.percentage == 55, f"TR.percentage: {TestResultsAggregator.percentage}"

# COMMAND ----------

suiteX = TestSuite()

try:
  suiteX.test(None, None, lambda:True)
  raise Exception("Expexted a value error")
except ValueError:
  pass

try:
  suiteX.test("abc", None, lambda:True)
  suiteX.test("abc", None, lambda:True)
  raise Exception("Expected a value error")
except ValueError:
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC # dbTest()

# COMMAND ----------

dbTest("PythonTest-1", "cat", "cat")

try:
  dbTest("PythonTest-2", "cat", "dog")
except AssertionError:
  pass

# dbTest("PythonTest-3", 999, 666)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareFloats
# MAGIC 
# MAGIC ```compareFloats(floatA, floatB, tolerance)```

# COMMAND ----------

assert compareFloats(1, 1) == True

# No long or double equivilent to Scala
assert compareFloats(100.001, int(100)) == True
assert compareFloats(100.001, float(100)) == True

assert compareFloats(int(100), 100.001) == True
assert compareFloats(float(100), 100.001) == True

assert compareFloats(100.001, "blah") == False
assert compareFloats("blah", 100.001) == False

assert compareFloats(1.0, 1.0) == True

assert compareFloats(1.0, 1.2, .0001) == False
assert compareFloats(1.0, 1.02, .0001) == False
assert compareFloats(1.0, 1.002, .0001) == False
assert compareFloats(1.0, 1.0002, .0001) == False
assert compareFloats(1.0, 1.00002, .0001) == True
assert compareFloats(1.0, 1.000002, .0001) == True

assert compareFloats(1.2, 1.0, .0001) == False
assert compareFloats(1.02, 1.0, .0001) == False
assert compareFloats(1.002, 1.0, .0001) == False
assert compareFloats(1.0002, 1.0, .0001) == False
assert compareFloats(1.00002, 1.0, .0001) == True
assert compareFloats(1.000002, 1.0, .0001) == True

assert compareFloats(1, None) == False
assert compareFloats(None, 1) == False
assert compareFloats(None, None) == True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareSchemas
# MAGIC 
# MAGIC ```compareSchemas(schemaA, schemaB, testColumnOrder=True, testNullable=False)```

# COMMAND ----------

from pyspark.sql.types import *
_expectedSchema = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", DoubleType(), True)
])

_studentSchema1 = StructType([
  StructField("MaxSalary", DoubleType(), True),
  StructField("LastName", StringType(), True)
])

_studentSchema2 = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", BooleanType(), True)
])

_studentSchema3 = StructType([
  StructField("LastName", StringType(), False),
  StructField("MaxSalary", DoubleType(), True)
])

_studentSchema4 = StructType([
  StructField("LastName", StringType(), True),
  StructField("MaxSalary", DoubleType(), True),
  StructField("Country", StringType(), True)
])

assert compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=False, testNullable=True) == True, "out of order, ignore order"
assert compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=True, testNullable=True) == False, "out of order, preserve order"

assert compareSchemas(_expectedSchema, _studentSchema2, testColumnOrder=True, testNullable=True) == False, "different types"

assert compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=True, testNullable=True) == False, "check nullable"
assert compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=True, testNullable=False) == True, "don't check nullable"

assert compareSchemas(_expectedSchema, _studentSchema4, testColumnOrder=True, testNullable=True) == False, "left side < right size"
assert compareSchemas(_studentSchema4, _expectedSchema, testColumnOrder=True, testNullable=True) == False, "left side > right side"

assert compareSchemas(None,            _studentSchema3, testColumnOrder=True, testNullable=False) == False, "Null schemaA"
assert compareSchemas(None,            None,            testColumnOrder=True, testNullable=False) == True,  "Null schemaA and schemaB"
assert compareSchemas(_studentSchema2, None,            testColumnOrder=True, testNullable=False) == False, "Null schemaB"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing compareRows
# MAGIC 
# MAGIC ```compareRows(rowA, rowB)```

# COMMAND ----------

from pyspark.sql import Row
_rowA = Row(LastName="Duck", MaxSalary=10000)
_rowB = Row(LastName="Mouse", MaxSalary=60000)
_rowC = Row(MaxSalary=10000, LastName="Duck")
_rowD = Row(LastName="Duck", MaxSalary=10000, City="Anaheim")

assert compareRows(_rowA, _rowB) == False       # different schemas
assert compareRows(_rowA, _rowA) == True        # compare to self
assert compareRows(_rowA, _rowC) == True        # order reversed
assert compareRows(None, _rowA) == False        # Null rowA
assert compareRows(None, None) == True          # Null rowA and rowB
assert compareRows(_rowA, None) == False        # Null rowB
assert compareRows(_rowA, _rowD) == False       # _rowA smaller than _rowD
assert compareRows(_rowD, _rowA) == False       # _rowD bigger than _rowA

# note Python doesn't allow you to define rows without a schema
# _rowE = Row("Duck", 10000)

