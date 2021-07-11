// Databricks notebook source

val courseType = "il"
val courseAdvertisements = scala.collection.mutable.Map[String,(String,String,String)]()

displayHTML("Preparing the Scala environment...")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Utility-Methods

// COMMAND ----------

val moduleName = getModuleName()
val lessonName = getLessonName()
val username = getUsername()
val userhome = getUserhome()
val workingDir = getWorkingDir(courseType)
val databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

// ****************************************************************************
// Advertise variables we declared for the user - there are more, but these are the common ones
// ****************************************************************************
courseAdvertisements += ("moduleName" ->   ("v", moduleName,   "No additional information was provided."))
courseAdvertisements += ("lessonName" ->   ("v", lessonName,   "No additional information was provided."))
courseAdvertisements += ("username" ->     ("v", username,     "No additional information was provided."))
courseAdvertisements += ("userhome" ->     ("v", userhome,     "No additional information was provided."))
courseAdvertisements += ("workingDir" ->   ("v", workingDir,   "No additional information was provided."))
courseAdvertisements += ("databaseName" -> ("d", databaseName, "This is a private, per-notebook, database used to provide isolation from other users and exercises."))

// ****************************************************************************
// Advertise functions we declared for the user - there are more, but these are the common ones
// ****************************************************************************
courseAdvertisements += ("untilStreamIsReady" -> ("f", "name, progressions=3", """
  <div>Introduced in the course <b>Structured Streaming</b>, this method blocks until the stream is actually ready for processing.</div>
  <div>By default, it waits for 3 progressions of the stream to ensure sufficent data has been processed.</div>"""))
courseAdvertisements += ("stopAllStreams" -> ("f", "", """
  <div>Introduced in the course <b>Structured Streaming</b>, this method stops all active streams while providing extra exception handling.</div>
  <div>It is functionally equivilent to:<div>
  <div><code>for (s <- spark.streams.active) s.stop()</code></div>"""))

displayHTML("Defining custom variables for this lesson...")

// ****************************************************************************
// Advertise are done here 100% of the time so that the are consistently 
// formatted. If left to each lesson, the meta data associated to each has to 
// be maintained in dozens of different places. This means that if a course
// developer doesn't use a variable/function, then can simply suppress it later
// by setting com.databricks.training.suppress.{key} to "true"
// ****************************************************************************

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

// MAGIC %run ./Dummy-Data-Generator

// COMMAND ----------

// MAGIC %run ./Dataset-Mounts

// COMMAND ----------

classroomCleanup(daLogger, courseType, username, moduleName, lessonName, false)

// COMMAND ----------

assertDbrVersion(spark.conf.get("com.databricks.training.expected-dbr", null))

