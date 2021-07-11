// Databricks notebook source

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

val courseAdvertisements = scala.collection.mutable.Map[String,(String,String,String)]()

// COMMAND ----------

// MAGIC %run ./Utility-Methods

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer
val scalaTests = ArrayBuffer.empty[Boolean]
def functionPassed(result: Boolean) = {
  if (result) {
    scalaTests += true
  } else {
    scalaTests += false
  } 
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `printRecordsPerPartition`

// COMMAND ----------

def testPrintRecordsPerPartition(): Boolean = {
  
  // Import Data
  val peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
  
  // Get printed results
  import java.io.ByteArrayOutputStream
  val printedOutput = new ByteArrayOutputStream
  Console.withOut(printedOutput) {printRecordsPerPartition(peopleDF)}
  
  // Setup tests
  import scala.collection.mutable.ArrayBuffer
  val testsPassed = ArrayBuffer.empty[Boolean]
  
  def passedTest(result: Boolean, message: String = null) = {
    if (result) {
      testsPassed += true
    } else {
      testsPassed += false
      println(s"Failed Test: $message")
    } 
  }
  
  
  // Test if correct number of partitions are printing
  try {
    assert(
      (for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) yield element).size == peopleDF.rdd.getNumPartitions
    )
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "The correct number of partitions were not identified for printRecordsPerPartition")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for the correct number of partitions were not identified for printRecordsPerPartition")
    }
  }
  
  // Test if each printed partition has a record number associated
  try {
    val recordMap = for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) 
      yield Map(
        element.slice(element.indexOf("#") + 1, element.indexOf(":") - element.indexOf("#")) -> element.split(" ")(1).replace(",", "").toInt
      )
    val recordCounts = (for (map <- recordMap if map.keySet.toSeq(0).toInt.isInstanceOf[Int]) yield map.getOrElse(map.keySet.toSeq(0), -1))
    recordCounts.foreach(x => assert(x.isInstanceOf[Int]))
    recordCounts.foreach(x => assert(x != -1))
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "Not every partition has an associated record count")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for not every partition having an associated record count")
    }
  }
  
  // Test if the sum of the printed number of records per partition equals the total number of records
  try {
    val printedSum = (
      for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) yield element.split(" ")(1).replace(",", "").toInt
    ).sum
    assert(printedSum == peopleDF.count)
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "The sum of the number of records per partition does not match the total number of records")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for the sum of the number of records per partition not matching the total number of records")
    }
  }
  

  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for printRecordsPerPartition passed")
    true
  } else {
    println(s"$numTestsPassed of ${testsPassed.length} tests for printRecordsPerPartition passed")
    false
  }
}

functionPassed(testPrintRecordsPerPartition())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `computeFileStats`

// COMMAND ----------

def testComputeFileStats(): Boolean = {
  
  // Set file path
  val filePath = "/mnt/training/global-sales/transactions/2017.parquet"
  
  // Run and get output
  val output = computeFileStats(filePath)
  
  // Setup tests
  import scala.collection.mutable.ArrayBuffer
  val testsPassed = ArrayBuffer.empty[Boolean]
  
  def passedTest(result: Boolean, message: String = null) = {
    if (result) {
      testsPassed += true
    } else {
      testsPassed += false
      println(s"Failed Test: $message")
    } 
  }
  
  
  // Test if result is correct structure
  try {
    assert(output.getClass.getName.startsWith("scala.Tuple"))
    assert(output.productArity == 2)
    assert(output._1.isInstanceOf[Long])
    assert(output._2.isInstanceOf[Long])
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "The incorrect structure is returned for computeFileStats")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for the incorrect structure being returned for computeFileStats")
    }
  }
  
  // Test that correct result is returned
  try {
    assert(output._1 == 6276)
    assert(output._2 == 1269333224)
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "The incorrect result is returned for computeFileStats")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for the incorrect result being returned for computeFileStats")
    }
  }
  
  // Test that nonexistent file path throws error
  try {
    computeFileStats("alkshdahdnoinscoinwincwinecw/cw/cw/cd/c/wcdwdfobnwef")
    passedTest(false, "A nonexistent file path did not throw an error for computeFileStats")
  } catch {
    case a: java.io.FileNotFoundException => {
      passedTest(true)
    }
    case _: Exception => {
      passedTest(false, "non-descript error for a nonexistent file path throwing an error for computeFileStats")
    }
  }
  

  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for computeFileStats passed")
    true
  } else {
    println(s"$numTestsPassed of ${testsPassed.length} tests for computeFileStats passed")
    false
  }
}

functionPassed(testComputeFileStats())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `cacheAs`

// COMMAND ----------

def testCacheAs(): Boolean = {
  
  import org.apache.spark.storage.StorageLevel
  // Import DF
  val inputDF = spark.read.parquet("/mnt/training/global-sales/transactions/2017.parquet").limit(100)
  
  // Setup tests
  import scala.collection.mutable.ArrayBuffer
  val testsPassed = ArrayBuffer.empty[Boolean]
  
  def passedTest(result: Boolean, message: String = null) = {
    if (result) {
      testsPassed += true
    } else {
      testsPassed += false
      println(s"Failed Test: $message")
    } 
  }
  
  
  // Test uncached table gets cached
  try {
    cacheAs(inputDF, "testCacheTable12344321", StorageLevel.MEMORY_ONLY)
    assert(spark.catalog.isCached("testCacheTable12344321"))
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "Uncached table was not cached for cacheAs")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for uncached table being cached for cacheAs")
    }
  }
  
  // Test cached table gets recached
  try {
    cacheAs(inputDF, "testCacheTable12344321", StorageLevel.MEMORY_ONLY)
    assert(spark.catalog.isCached("testCacheTable12344321"))
    spark.catalog.uncacheTable("testCacheTable12344321")
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "Cached table was not recached for cacheAs")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for cached table being recached for cacheAs")
    }
  }

  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for cacheAs passed")
    true
  } else {
    println(s"$numTestsPassed of ${testsPassed.length} tests for cacheAs passed")
    false
  }
}

functionPassed(testCacheAs())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `benchmarkCount()`

// COMMAND ----------

def testBenchmarkCount(): Boolean = {
  
  def testFunction() = {
    spark.createDataFrame(Seq((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12)))
  }
  val output = benchmarkCount(testFunction)
  
  // Setup tests
  import scala.collection.mutable.ArrayBuffer
  val testsPassed = ArrayBuffer.empty[Boolean]
  
  def passedTest(result: Boolean, message: String = null) = {
    if (result) {
      testsPassed += true
    } else {
      testsPassed += false
      println(s"Failed Test: $message")
    } 
  }
  
  
  // Test that correct structure is returned
  try {
    assert(output.getClass.getName.startsWith("scala.Tuple"))
    assert(output.productArity == 3)
    assert(output._1.isInstanceOf[org.apache.spark.sql.DataFrame])
    assert(output._2.isInstanceOf[Long])
    assert(output._3.isInstanceOf[Long])
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "Correct structure not returned for benchmarkCount")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for correct structure being returned for benchmarkCount")
    }
  }
  
  // Test that correct result is returned
  try {
    assert(output._1.rdd.collect().deep == testFunction().rdd.collect().deep)
    assert(output._2 == testFunction().count())
    assert(output._3 > 0)
    assert(output._3 < 10000)
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "Uncached table was not cached for cacheAs")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for uncached table being cached for cacheAs")
    }
  }

  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for benchmarkCount passed")
    true
  } else {
    println(s"$numTestsPassed of ${testsPassed.length} tests for benchmarkCount passed")
    false
  }
}

functionPassed(testBenchmarkCount())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test **`untilStreamIsReady()`**

// COMMAND ----------

val dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
val dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

val initialDF = spark
  .readStream                             // Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)        // Force processing of only 1 file per trigger 
  .schema(dataSchema)                     // Required for all streaming DataFrames
  .json(dataPath)                         // The stream's source directory and file type

val name = "Testing_123"

display(initialDF, streamName = name)
untilStreamIsReady(name)
assert(spark.streams.active.length == 1, "Expected 1 active stream, found " + spark.streams.active.length)

// COMMAND ----------

for (stream <- spark.streams.active) {
  stream.stop()
  var queries = spark.streams.active.filter(_.name == stream.name)
  while (queries.length > 0) {
    Thread.sleep(5*1000) // Give it a couple of seconds
    queries = spark.streams.active.filter(_.name == stream.name)
  }
  println("""The stream "%s" has beend terminated.""".format(stream.name))
}

// COMMAND ----------

val numTestsPassed = scalaTests.groupBy(identity).mapValues(_.size)(true)
if (numTestsPassed == scalaTests.length) {
  println(s"All $numTestsPassed tests for Scala passed")
} else {
  println(s"$numTestsPassed of ${scalaTests.length} tests for Scala passed")
  throw new Exception(s"$numTestsPassed of ${scalaTests.length} tests for Scala passed")
}

