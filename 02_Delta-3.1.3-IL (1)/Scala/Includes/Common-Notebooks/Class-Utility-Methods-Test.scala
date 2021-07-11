// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Class-Utility-Methods-Test
// MAGIC The purpose of this notebook is to faciliate testing of courseware-specific utility methos.

// COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %md
// MAGIC a lot of these tests evolve around the current DBR version.
// MAGIC 
// MAGIC It shall be assumed that the cluster is configured properly and that these tests are updated with each publishing of courseware against a new DBR

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

def functionPassed(result: Boolean) = {
  if (!result) {
    assert(false, "Test failed")
  } 
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `getTags`

// COMMAND ----------

def testGetTags(): Boolean = {
  
  val testTags = getTags()
  
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
  
  // Test that tags result is correct type
  try {
    assert(testTags.isInstanceOf[Map[com.databricks.logging.TagDefinition,String]])
    passedTest(true)
  } catch {
    case a: AssertionError => {
      passedTest(false, "tags is not an instance of Map[com.databricks.logging.TagDefinition,String]")
    }
    case _: Exception => {
      passedTest(false, "non-descript error for tags being an instance of Map[com.databricks.logging.TagDefinition,String]")
    }
  }

  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for getTags passed")
    true
  } else {
    throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getTags passed")
  }
}

functionPassed(testGetTags())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `getTag()`

// COMMAND ----------

def testGetTag(): Boolean = {
  
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
  
  // Test that returns null when not present
  try {
    assert(getTag("thiswillneverbeincluded") == null)
    passedTest(true)
  } catch {
    case e: Exception => passedTest(false, "tag value for 'thiswillneverbeincluded' is not null")
  }
  
  // Test that default value is returned when not present
  try {
    assert(getTag("thiswillneverbeincluded", "default-test").contentEquals("default-test"))
    passedTest(true)
  } catch {
    case e: Exception => passedTest(false, "tag value for 'thiswillneverbeincluded' is not the default value")
  }
  
  // Test that correct result is returned when default value is not set
  try {
    val orgId = getTags().collect({ case (t, v) if t.name == "orgId" => v }).toSeq(0)
    assert(orgId.isInstanceOf[String])
    assert(orgId.size > 0)
    assert(orgId.contentEquals(getTag("orgId")))
    passedTest(true)
  } catch {
    case e: Exception => passedTest(false, "Unexpected tag value returned for getTag")
  }

  // Print final info and return
  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for getTag passed")
    true
  } else {
    throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getTag passed")
  }
}

functionPassed(testGetTag())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test `getDbrMajorAndMinorVersions()`

// COMMAND ----------

def testGetDbrMajorAndMinorVersions(): Boolean = {
  // We cannot rely on the assumption that all courses are
  // running latestDbrMajor.latestDbrMinor. The best we
  // can do here is make sure it matches the environment
  // variable from which it came.
  val dbrVersion = System.getenv().get("DATABRICKS_RUNTIME_VERSION")
  
  val (major,minor) = getDbrMajorAndMinorVersions()
  assert(dbrVersion.startsWith(f"${major}.${minor}"), f"Found ${major}.${minor} for ${dbrVersion}")
  
  return true
}

functionPassed(testGetDbrMajorAndMinorVersions())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test `getPythonVersion()`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `getUsername()`

// COMMAND ----------

def testGetUsername(): Boolean = {
  val username = getUsername()
  assert(username.isInstanceOf[String])
  assert(!username.contentEquals(""))
  
  return true
}
functionPassed(testGetUsername())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `getUserhome`

// COMMAND ----------

def testGetUserhome(): Boolean = {
  val userhome = getUserhome()
  assert(userhome.isInstanceOf[String])
  assert(!userhome.contentEquals(""))
  assert(userhome == "dbfs:/user/" + getUsername())
    
  return true
}

functionPassed(testGetUserhome())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `assertDbrVersion`

// COMMAND ----------

def testAssertDbrVersion(): Boolean = {
  
  val (majorVersion, minorVersion) = getDbrMajorAndMinorVersions()
  val major = majorVersion.toInt
  val minor = minorVersion.toInt

  val goodVersions = Seq(
    ("SG1", major-1, minor-1),
    ("SG2", major-1, minor),
    ("SG3", major, minor-1),
    ("SG4", major, minor)
  )
  
  for ( (name, testMajor, testMinor) <- goodVersions) {
    println(f"-- ${name} ${testMajor}.${testMinor}")
    assertDbrVersion(null, testMajor, testMinor, false)
    println(f"-"*80)
  }

  val badVersions = Seq(
    ("SB1", major+1, minor+1),
    ("SB2", major+1, minor),
    ("SB3", major, minor+1)
  )

  for ( (name, testMajor, testMinor) <- badVersions) {
    try {
      println(f"-- ${name} ${testMajor}.${testMinor}")
      assertDbrVersion(null, testMajor, testMinor, false)
      throw new Exception("Expected AssertionError")
    } catch {
      case e: AssertionError => println(e)
    }
    println(f"-"*80)
  }
  return true
}
functionPassed(testAssertDbrVersion())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `assertIsMlRuntime`

// COMMAND ----------

// def testAssertIsMlRuntime(): Boolean = {
    
//   assertIsMlRuntime("5.5.x-ml-scala2.11")
//   assertIsMlRuntime("5.5.x-cpu-ml-scala2.11")

//   try {
//     assertIsMlRuntime("5.5.x-scala2.11")
//     assert(false, s"Expected to throw an IllegalArgumentException")
//   } catch {
//     case _: AssertionError => ()
//   }

//   try {
//     assertIsMlRuntime("5.5.xml-scala2.11")
//     assert(false, s"Expected to throw an IllegalArgumentException")
//   } catch {
//     case _: AssertionError => ()
//   }

//   return true
// }
// functionPassed(testAssertIsMlRuntime())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test Legacy Functions
// MAGIC 
// MAGIC Note: Legacy functions will not be tested. Use at your own risk.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Test `createUserDatabase`

// COMMAND ----------

def testCreateUserDatabase(): Boolean = {

  val courseType = "wa"
  val username = "mickey.mouse@disney.com"
  val moduleName = "Testing-Stuff 101"
  val lessonName = "TS 03 - Underwater Basket Weaving"
  
  // Test that correct database name is returned
  val expectedDatabaseName = "mickey_mouse_disney_com" + "_" + "testing_stuff_101" + "_" + "ts_03_underwater_basket_weaving" + "_" + "s" + "wa"
  
  val databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
  assert(databaseName == expectedDatabaseName)
  
  val actualDatabaseName = createUserDatabase(courseType, username, moduleName, lessonName)
  assert(actualDatabaseName == expectedDatabaseName)

  assert(spark.sql(s"SHOW DATABASES LIKE '$expectedDatabaseName'").first.getAs[String]("databaseName") == expectedDatabaseName)
  assert(spark.sql("SELECT current_database()").first.getAs[String]("current_database()") == expectedDatabaseName)
  
  return true
}
functionPassed(testCreateUserDatabase())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test `getExperimentId()`

// COMMAND ----------

def testGetExperimentId(): Boolean = {
  
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
  
  // Test that result is correct type
  try {
    assert(getExperimentId().isInstanceOf[Long])
    passedTest(true)
  } catch {
    case e: Exception => passedTest(false, "result of getExperimentId is not of Long type")
  }
  
  // Note that the tags are an immutable map so we can't test other values
  // Another option is add parameters to getExperimentId, but that could break existing uses
  
  // Test that result comes out as expected
  try {
    val notebookId = try {
      com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.TagDefinition("notebookId", ""))
    } catch {
      case e: Exception => null
    }
    val jobId = try {
      com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.TagDefinition("jobId", ""))
    } catch {
      case e: Exception => null
    }
    val expectedResult = if (notebookId != null){
      notebookId.toLong
    } else {
      if (jobId != null) {
        jobId.toLong
      } else {
        0
      }
    }
    assert(expectedResult == getExperimentId())
    passedTest(true)
  } catch {
    case e: Exception => passedTest(false, "unexpected result for getExperimentId")
  }
  
  val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
  if (numTestsPassed == testsPassed.length) {
    println(s"All $numTestsPassed tests for getExperimentId passed")
    true
  } else {
    throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getExperimentId passed")
  }
}

functionPassed(testGetExperimentId())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test `classroomCleanup()`

// COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), false)

// COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), true)

// COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), false)

// COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test FILL_IN

// COMMAND ----------

println(FILL_IN)
println(FILL_IN.VALUE)
println(FILL_IN.ARRAY)
println(FILL_IN.SCHEMA)
println(FILL_IN.ROW)
println(FILL_IN.LONG)
println(FILL_IN.INT)
println(FILL_IN.DATAFRAME)
println(FILL_IN.DATASET)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Test `showStudentSurvey()`

// COMMAND ----------

val html = renderStudentSurvey()

// COMMAND ----------

showStudentSurvey()

