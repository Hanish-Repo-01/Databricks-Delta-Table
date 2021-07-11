// Databricks notebook source

//*******************************************
// TAG API FUNCTIONS
//*******************************************

// Get all tags
def getTags(): Map[com.databricks.logging.TagDefinition,String] = {
  com.databricks.logging.AttributionContext.current.tags
} 

// Get a single tag's value
def getTag(tagName: String, defaultValue: String = null): String = {
  val values = getTags().collect({ case (t, v) if t.name == tagName => v }).toSeq
  values.size match {
    case 0 => defaultValue
    case _ => values.head.toString
  }
}

//*******************************************
// Get Databricks runtime major and minor versions
//*******************************************

def getDbrMajorAndMinorVersions(): (Int, Int) = {
  val dbrVersion = System.getenv().get("DATABRICKS_RUNTIME_VERSION")
  val Array(dbrMajorVersion, dbrMinorVersion, _*) = dbrVersion.split("""\.""")
  return (dbrMajorVersion.toInt, dbrMinorVersion.toInt)
}


//*******************************************
// USER, USERNAME, AND USERHOME FUNCTIONS
//*******************************************

// Get the user's username
def getUsername(): String = {
  return try {
    dbutils.widgets.get("databricksUsername")
  } catch {
    case _: Exception => getTag("user", java.util.UUID.randomUUID.toString.replace("-", ""))
  }
}

// Get the user's userhome
def getUserhome(): String = {
  val username = getUsername()
  return s"dbfs:/user/$username"
}

def getModuleName(): String = {
  // This will/should fail if module-name is not defined in the Classroom-Setup notebook
  return spark.conf.get("com.databricks.training.module-name")
}

def getLessonName(): String = {
  // If not specified, use the notebook's name.
  return dbutils.notebook.getContext.notebookPath.get.split("/").last
}

def getWorkingDir(courseType:String): String = {
  val langType = "s" // for scala
  val moduleName = getModuleName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
  val lessonName = getLessonName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
  val workingDir = "%s/%s/%s_%s%s".format(getUserhome(), moduleName, lessonName, langType, courseType)
  return workingDir.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
}

//**********************************
// VERSION ASSERTION FUNCTIONS
//**********************************

// When migrating DBR versions this should be one
// of the only two places that needs to be updated

val latestDbrMajor = 6
val latestDbrMinor = 2

// Assert an appropriate Databricks Runtime version
def assertDbrVersion(expected:String, latestMajor:Int = latestDbrMajor, latestMinor:Int = latestDbrMinor, display:Boolean=true):String = {
  
  var expMajor = latestMajor
  var expMinor = latestMinor
  
  if (expected != null && expected.trim() != "" && expected != "{{dbr}}") {
    expMajor = expected.split('.')(0).toInt
    expMinor = expected.split('.')(1).toInt
  }
  
  val (major, minor) = getDbrMajorAndMinorVersions()

  if ((major < expMajor) || (major == expMajor && minor < expMinor)) {
    throw new AssertionError(s"This notebook must be run on DBR $expMajor.$expMinor or newer. Your cluster is using $major.$minor. You must update your cluster configuration before proceeding.")
  }
  
  val html = if (major != expMajor || minor != expMinor) {
    s"""
      <div style="color:red; font-weight:bold">WARNING: This notebook was tested on DBR $expMajor.$expMinor but we found DBR $major.$minor.</div>
      <div style="font-weight:bold">Using an untested DBR may yield unexpected results and/or various errors</div>
      <div style="font-weight:bold">Please update your cluster configuration and/or <a href="https://academy.databricks.com/" target="_blank">download a newer version of this course</a> before proceeding.</div>
    """
  } else {
    s"Running on <b>DBR $major.$minor</b>"
  }

  if (display) 
    displayHTML(html) 
  else 
    println(html)
  
  return s"$major.$minor"
}

// Assert that the Databricks Runtime is an ML Runtime
// def assertIsMlRuntime(testValue:String = null):Unit = {
  
//   val sourceValue = if (testValue != null) testValue
//   else getRuntimeVersion()

//   if (sourceValue.contains("-ml-") == false) {
//     throw new AssertionError(s"This notebook must be ran on a Databricks ML Runtime, found $sourceValue.")
//   }
// }


//**********************************
// LEGACY TESTING FUNCTIONS
//********************************getDbrMajorAndMinorVersions**

import org.apache.spark.sql.DataFrame

// Test results map to store results
val testResults = scala.collection.mutable.Map[String, (Boolean, String)]()

// Hash a string value
def toHash(value:String):Int = {
  import org.apache.spark.sql.functions.hash
  import org.apache.spark.sql.functions.abs
  spark.createDataset(List(value)).select(abs(hash($"value")).cast("int")).as[Int].first()
}

// Clear the testResults map
def clearYourResults(passedOnly:Boolean = true):Unit = {
  val whats = testResults.keySet.toSeq.sorted
  for (what <- whats) {
    val passed = testResults(what)._1
    if (passed || passedOnly == false) testResults.remove(what)
  }
}

// Validate DataFrame schema
def validateYourSchema(what:String, df:DataFrame, expColumnName:String, expColumnType:String = null):Unit = {
  val label = s"$expColumnName:$expColumnType"
  val key = s"$what contains $label"
  
  try{
    val actualTypeTemp = df.schema(expColumnName).dataType.typeName
    val actualType = if (actualTypeTemp.startsWith("decimal")) "decimal" else actualTypeTemp
    
    if (expColumnType == null) {
      testResults.put(key,(true, "validated"))
      println(s"""$key: validated""")
      
    } else if (actualType == expColumnType) {
      val answerStr = "%s:%s".format(expColumnName, actualType)
      testResults.put(key,(true, "validated"))
      println(s"""$key: validated""")
      
    } else {
      val answerStr = "%s:%s".format(expColumnName, actualType)
      testResults.put(key,(false, answerStr))
      println(s"""$key: NOT matching ($answerStr)""")
    }
  } catch {
    case e:java.lang.IllegalArgumentException => {
      testResults.put(key,(false, "-not found-"))
      println(s"$key: NOT found")
    }
  }
}

// Validate an answer
def validateYourAnswer(what:String, expectedHash:Int, answer:Any):Unit = {
  // Convert the value to string, remove new lines and carriage returns and then escape quotes
  val answerStr = if (answer == null) "null" 
  else answer.toString

  val hashValue = toHash(answerStr)

  if (hashValue == expectedHash) {
    testResults.put(what,(true, answerStr))
    println(s"""$what was correct, your answer: ${answerStr}""")
  } else{
    testResults.put(what,(false, answerStr))
    println(s"""$what was NOT correct, your answer: ${answerStr}""")
  }
}

// Summarize results in the testResults function
def summarizeYourResults():Unit = {
  var html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""

  val whats = testResults.keySet.toSeq.sorted
  for (what <- whats) {
    val passed = testResults(what)._1
    val answer = testResults(what)._2
    val color = if (passed) "green" else "red" 
    val passFail = if (passed) "passed" else "FAILED" 
    html += s"""<tr style='font-size:larger; white-space:pre'>
                  <td>${what}:&nbsp;&nbsp;</td>
                  <td style="color:${color}; text-align:center; font-weight:bold">${passFail}</td>
                  <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;${answer}</td>
                </tr>"""
  }
  html += "</table></body></html>"
  displayHTML(html)
}

// Log test results to a file
def logYourTest(path:String, name:String, value:Double):Unit = {
  if (path.contains("\"")) throw new AssertionError("The name cannot contain quotes.")
  
  dbutils.fs.mkdirs(path)

  val csv = """ "%s","%s" """.format(name, value).trim()
  val file = "%s/%s.csv".format(path, name).replace(" ", "-").toLowerCase
  dbutils.fs.put(file, csv, true)
}

// Load the test results log file
def loadYourTestResults(path:String):org.apache.spark.sql.DataFrame = {
  return spark.read.schema("name string, value double").csv(path)
}

// Load the test result log file into map
def loadYourTestMap(path:String):scala.collection.mutable.Map[String,Double] = {
  case class TestResult(name:String, value:Double)
  val rows = loadYourTestResults(path).collect()
  
  val map = scala.collection.mutable.Map[String,Double]()
  for (row <- rows) map.put(row.getString(0), row.getDouble(1))
  
  return map
}

//**********************************
// USER DATABASE FUNCTIONS
//**********************************

def getDatabaseName(courseType:String, username:String, moduleName:String, lessonName:String):String = {
  val langType = "s" // for scala
  var databaseName = username + "_" + moduleName + "_" + lessonName + "_" + langType + courseType
  databaseName = databaseName.toLowerCase
  databaseName = databaseName.replaceAll("[^a-zA-Z0-9]", "_")
  return databaseName.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
}

// Create a user-specific database
def createUserDatabase(courseType:String, username:String, moduleName:String, lessonName:String):String = {
  val databaseName = getDatabaseName(courseType, username, moduleName, lessonName)

  spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(databaseName))
  spark.sql("USE %s".format(databaseName))

  return databaseName
}

//**********************************
// ML FLOW UTILITY FUNCTIONS
//**********************************

// Create the experiment ID from available notebook information
def getExperimentId(): Long = {
  val notebookId = getTag("notebookId", null)
  val jobId = getTag("jobId", null)
  
  
  val retval = (notebookId != null) match { 
      case true => notebookId.toLong
      case false => (jobId != null) match { 
        case true => jobId.toLong
        case false => 0
      }
  }
  
  spark.conf.set("com.databricks.training.experimentId", retval)
  retval
}

// ****************************************************************************
// Utility method to determine whether a path exists
// ****************************************************************************

def pathExists(path:String):Boolean = {
  try {
    dbutils.fs.ls(path)
    return true
  } catch{
    case e: Exception => return false
  } 
}

// ****************************************************************************
// Utility method for recursive deletes
// Note: dbutils.fs.rm() does not appear to be truely recursive
// ****************************************************************************

def deletePath(path:String):Unit = {
  val files = dbutils.fs.ls(path)

  for (file <- files) {
    val deleted = dbutils.fs.rm(file.path, true)
    
    if (deleted == false) {
      if (file.isDir) {
        deletePath(file.path)
      } else {
        throw new java.io.IOException("Unable to delete file: " + file.path)
      }
    }
  }
  
  if (dbutils.fs.rm(path, true) == false) {
    throw new java.io.IOException("Unable to delete directory: " + path)
  }
}

// ****************************************************************************
// Utility method to clean up the workspace at the end of a lesson
// ****************************************************************************

def classroomCleanup(daLogger:DatabricksAcademyLogger, courseType:String, username:String, moduleName:String, lessonName:String, dropDatabase: Boolean):Unit = {
  
  var actions = ""

  // Stop any active streams
  for (stream <- spark.streams.active) {
    stream.stop()
    
    // Wait for the stream to stop
    var queries = spark.streams.active.filter(_.name == stream.name)

    while (queries.length > 0) {
      Thread.sleep(5*1000) // Give it a couple of seconds
      queries = spark.streams.active.filter(_.name == stream.name)
    }

    actions += s"""<li>Terminated stream: <b>${stream.name}</b></li>"""
  }
  
  // Drop the tables only from specified database
  val database = getDatabaseName(courseType, username, moduleName, lessonName)
  try {
    val tables = spark.sql(s"show tables from $database").select("tableName").collect()
    for (row <- tables){
      var tableName = row.getAs[String]("tableName")
      spark.sql("drop table if exists %s.%s".format(database, tableName))

      // In some rare cases the files don't actually get removed.
      Thread.sleep(1000) // Give it just a second...
      val hivePath = "dbfs:/user/hive/warehouse/%s.db/%s".format(database, tableName)
      dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
      
      actions += s"""<li>Dropped table: <b>${tableName}</b></li>"""
    } 
  } catch {
    case _: Exception => () // ignored
  }

  // Drop the database if instructed to
  if (dropDatabase){
    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")

    // In some rare cases the files don't actually get removed.
    Thread.sleep(1000) // Give it just a second...
    val hivePath = "dbfs:/user/hive/warehouse/%s.db".format(database)
    dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
    
    actions += s"""<li>Dropped database: <b>${database}</b></li>"""
  }
  
  // Remove files created from previous runs
  val path = getWorkingDir(courseType)
  if (pathExists(path)) {
    deletePath(path)

    actions += s"""<li>Removed working directory: <b>$path</b></li>"""
  }
  
  var htmlMsg = "Cleaning up the learning environment..."
  if (actions.length == 0) htmlMsg += "no actions taken."
  else htmlMsg += s"<ul>$actions</ul>"
  displayHTML(htmlMsg)
  
  if (dropDatabase) daLogger.logEvent("Classroom-Cleanup-Final")
  else daLogger.logEvent("Classroom-Cleanup-Preliminary")
}

// ****************************************************************************
// Utility method to delete a database
// ****************************************************************************

def deleteTables(database:String):Unit = {
  spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(database))
}

// ****************************************************************************
// DatabricksAcademyLogger and Student Feedback
// ****************************************************************************

class DatabricksAcademyLogger() extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler._

  val hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"

  def logEvent(eventId: String, message: String = null):Unit = {
    import org.apache.http.entity._
    import org.apache.http.impl.client.{HttpClients, BasicResponseHandler}
    import org.apache.http.client.methods.HttpPost
    import java.net.URLEncoder.encode
    import org.json4s.jackson.Serialization
    implicit val formats = org.json4s.DefaultFormats

    val start = System.currentTimeMillis // Start the clock

    var client:org.apache.http.impl.client.CloseableHttpClient = null

    try {
      val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
      val username = encode(getUsername(), utf8)
      val moduleName = encode(getModuleName(), utf8)
      val lessonName = encode(getLessonName(), utf8)
      val event = encode(eventId, utf8)
      val url = s"$hostname/logger"
    
      val content = Map(
        "tags" ->       getTags().map(x => (x._1.name, s"${x._2}")),
        "moduleName" -> getModuleName(),
        "lessonName" -> getLessonName(),
        "orgId" ->      getTag("orgId", "unknown"),
        "username" ->   getUsername(),
        "eventId" ->    eventId,
        "eventTime" ->  s"${System.currentTimeMillis}",
        "language" ->   getTag("notebookLanguage", "unknown"),
        "notebookId" -> getTag("notebookId", "unknown"),
        "sessionId" ->  getTag("sessionId", "unknown"),
        "message" ->    message
      )
      
      val client = HttpClients.createDefault()
      val httpPost = new HttpPost(url)
      val entity = new StringEntity(Serialization.write(content))      

      httpPost.setEntity(entity)
      httpPost.setHeader("Accept", "application/json")
      httpPost.setHeader("Content-type", "application/json")

      val response = client.execute(httpPost)
      
      val duration = System.currentTimeMillis - start // Stop the clock
      org.apache.log4j.Logger.getLogger(getClass).info(s"DAL-$eventId-SUCCESS: Event completed in $duration ms")
      
    } catch {
      case e:Exception => {
        val duration = System.currentTimeMillis - start // Stop the clock
        val msg = s"DAL-$eventId-FAILURE: Event completed in $duration ms"
        org.apache.log4j.Logger.getLogger(getClass).error(msg, e)
      }
    } finally {
      if (client != null) {
        try { client.close() } 
        catch { case _:Exception => () }
      }
    }
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = logEvent("JobEnd: " + jobEnd.jobId)
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = logEvent("JobStart: " + jobStart.jobId)
}

def showStudentSurvey():Unit = {
  val html = renderStudentSurvey()
  displayHTML(html);
}

def renderStudentSurvey():String = {
  import java.net.URLEncoder.encode
  val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
  val username = encode(getUsername(), utf8)
  val userhome = encode(getUserhome(), utf8)

  val moduleName = encode(getModuleName(), utf8)
  val lessonName = encode(getLessonName(), utf8)
  val lessonNameUnencoded = getLessonName()
  
  val apiEndpoint = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"

  import scala.collection.Map
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.write
  import org.json4s.JsonDSL._

  implicit val formats: DefaultFormats = DefaultFormats

  val feedbackUrl = s"$apiEndpoint/feedback";
  
  val html = """
  <html>
  <head>
    <script src="https://files.training.databricks.com/static/js/classroom-support.min.js"></script>
    <script>
<!--    
      window.setTimeout( // Defer until bootstrap has enough time to async load
        () => { 
          $("#divComment").css("display", "visible");

          // Emulate radio-button like feature for multiple_choice
          $(".multiple_choicex").on("click", (evt) => {
                const container = $(evt.target).parent();
                $(".multiple_choicex").removeClass("checked"); 
                $(".multiple_choicex").removeClass("checkedRed"); 
                $(".multiple_choicex").removeClass("checkedGreen"); 
                container.addClass("checked"); 
                if (container.hasClass("thumbsDown")) { 
                    container.addClass("checkedRed"); 
                } else { 
                    container.addClass("checkedGreen"); 
                };
                
                // Send the like/dislike before the comment is shown so we at least capture that.
                // In analysis, always take the latest feedback for a module (if they give a comment, it will resend the like/dislike)
                var json = {
                  moduleName:  "GET_MODULE_NAME", 
                  lessonName:  "GET_LESSON_NAME", 
                  orgId:       "GET_ORG_ID",
                  username:    "GET_USERNAME",
                  language:    "scala",
                  notebookId:  "GET_NOTEBOOK_ID",
                  sessionId:   "GET_SESSION_ID",
                  survey: $(".multiple_choicex.checked").attr("value"), 
                  comment: $("#taComment").val() 
                };
                
                $("#vote-response").html("Recording your vote...");

                $.ajax({
                  type: "PUT", 
                  url: "FEEDBACK_URL", 
                  data: JSON.stringify(json),
                  dataType: "json",
                  processData: false
                }).done(function() {
                  $("#vote-response").html("Thank you for your vote!<br/>Please feel free to share more if you would like to...");
                  $("#divComment").show("fast");
                }).fail(function() {
                  $("#vote-response").html("There was an error submitting your vote");
                }); // End of .ajax chain
          });


           // Set click handler to do a PUT
          $("#btnSubmit").on("click", (evt) => {
              // Use .attr("value") instead of .val() - this is not a proper input box
              var json = {
                moduleName:  "GET_MODULE_NAME", 
                lessonName:  "GET_LESSON_NAME", 
                orgId:       "GET_ORG_ID",
                username:    "GET_USERNAME",
                language:    "scala",
                notebookId:  "GET_NOTEBOOK_ID",
                sessionId:   "GET_SESSION_ID",
                survey: $(".multiple_choicex.checked").attr("value"), 
                comment: $("#taComment").val() 
              };

              $("#feedback-response").html("Sending feedback...");

              $.ajax({
                type: "PUT", 
                url: "FEEDBACK_URL", 
                data: JSON.stringify(json),
                dataType: "json",
                processData: false
              }).done(function() {
                  $("#feedback-response").html("Thank you for your feedback!");
              }).fail(function() {
                  $("#feedback-response").html("There was an error submitting your feedback");
              }); // End of .ajax chain
          });
        }, 2000
      );
-->
    </script>    
    <style>
.multiple_choicex > img {    
    border: 5px solid white;
    border-radius: 5px;
}
.multiple_choicex.choice1 > img:hover {    
    border-color: green;
    background-color: green;
}
.multiple_choicex.choice2 > img:hover {    
    border-color: red;
    background-color: red;
}
.multiple_choicex {
    border: 0.5em solid white;
    background-color: white;
    border-radius: 5px;
}
.multiple_choicex.checkedGreen {
    border-color: green;
    background-color: green;
}
.multiple_choicex.checkedRed {
    border-color: red;
    background-color: red;
}
    </style>
  </head>
  <body>
    <h2 style="font-size:28px; line-height:34.3px"><img style="vertical-align:middle" src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/>What did you think?</h2>
    <p>Please let us know if you liked this notebook, <b>LESSON_NAME_UNENCODED</b></p>
    <div id="feedback" style="clear:both;display:table;">
      <span class="multiple_choicex choice1 thumbsUp" value="positive"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-up.png"/></span>
      <span class="multiple_choicex choice2 thumbsDown" value="negative"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-down.png"/></span>
      <div id="vote-response" style="color:green; margin:1em 0; font-weight:bold">&nbsp;</div>
      <table id="divComment" style="display:none; border-collapse:collapse;">
        <tr>
          <td style="padding:0"><textarea id="taComment" placeholder="How can we make this lesson better? (optional)" style="height:4em;width:30em;display:block"></textarea></td>
          <td style="padding:0"><button id="btnSubmit" style="margin-left:1em">Send</button></td>
        </tr>
      </table>
    </div>
    <div id="feedback-response" style="color:green; margin-top:1em; font-weight:bold">&nbsp;</div>
  </body>
  </html>
  """

  return html.replaceAll("GET_MODULE_NAME", getModuleName())
             .replaceAll("GET_LESSON_NAME", getLessonName())
             .replaceAll("GET_ORG_ID", getTag("orgId", "unknown"))
             .replaceAll("GET_USERNAME", getUsername())
             .replaceAll("GET_NOTEBOOK_ID", getTag("notebookId", "unknown"))
             .replaceAll("GET_SESSION_ID", getTag("sessionId", "unknown"))
             .replaceAll("LESSON_NAME_UNENCODED", lessonNameUnencoded)
             .replaceAll("FEEDBACK_URL", feedbackUrl)

}

// ****************************************************************************
// Facility for advertising functions, variables and databases to the student
// ****************************************************************************
def allDone(advertisements: scala.collection.mutable.Map[String,(String,String,String)]):Unit = {
  
  var functions = scala.collection.mutable.Map[String,(String,String,String)]()
  var variables = scala.collection.mutable.Map[String,(String,String,String)]()
  var databases = scala.collection.mutable.Map[String,(String,String,String)]()
  
  for ( (key,value) <- advertisements) {
    if (value._1 == "f" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
      functions += (key -> value)
    }
  }
  
  for ( (key,value) <- advertisements) {
    if (value._1 == "v" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
      variables += (key -> value)
    }
  }
  
  for ( (key,value) <- advertisements) {
    if (value._1 == "d" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
      databases += (key -> value)
    }
  }
  
  var html = ""
  if (functions.size > 0) {
    html += "The following functions were defined for you:<ul style='margin-top:0'>"
    for ( (key,value) <- functions) {
      html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
        <span style="color: green; font-weight:bold">${key}</span>
        <span style="font-weight:bold">(</span>
        <span style="color: green; font-weight:bold; font-style:italic">${value._2}</span>
        <span style="font-weight:bold">)</span>
        <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
        </li>"""
    }
    html += "</ul>"
  }
  if (variables.size > 0) {
    html += "The following variables were defined for you:<ul style='margin-top:0'>"
    for ( (key,value) <- variables) {
      html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
        <span style="color: green; font-weight:bold">${key}</span>: <span style="font-style:italic; font-weight:bold">${value._2} </span>
        <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
        </li>"""
    }
    html += "</ul>"
  }
  if (databases.size > 0) {
    html += "The following database were created for you:<ul style='margin-top:0'>"
    for ( (key,value) <- databases) {
      html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
        Now using the database identified by <span style="color: green; font-weight:bold">${key}</span>: 
        <div style="font-style:italic; font-weight:bold">${value._2}</div>
        <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
        </li>"""
    }
    html += "</ul>"
  }
  html += "All done!"
  displayHTML(html)
}

// ****************************************************************************
// Placeholder variables for coding challenge type specification
// ****************************************************************************
object FILL_IN {
  val VALUE = null
  val ARRAY = Array(Row())
  val SCHEMA = org.apache.spark.sql.types.StructType(List())
  val ROW = Row()
  val LONG: Long = 0
  val INT: Int = 0
  def DATAFRAME = spark.emptyDataFrame
  def DATASET = spark.createDataset(Seq(""))
}

// ****************************************************************************
// Initialize the logger so that it can be used down-stream
// ****************************************************************************
val daLogger = new DatabricksAcademyLogger()
// if (spark.conf.get("com.databricks.training.studentStatsService.registered", null) != "registered") {
//   sc.addSparkListener(daLogger)
//   spark.conf.set("com.databricks.training.studentStatsService", "registered")
// }
daLogger.logEvent("Initialized", "Initialized the Scala DatabricksAcademyLogger")

displayHTML("Defining courseware-specific utility methods...")

