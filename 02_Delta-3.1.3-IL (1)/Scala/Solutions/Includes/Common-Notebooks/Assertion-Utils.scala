// Databricks notebook source

//*******************************************
// Deprecated test functions
//*******************************************

def dbTest[T](id: String, expected: T, result: => T): Unit = {
  val eventId = "Test-" + (if (id != null) id else java.util.UUID.randomUUID)
  val evaluation = (result == expected)
  val status = if (evaluation) "passed" else "failed"
  daLogger.logEvent(id, s"$eventId\nDBTest Assertion\n$status\n1")

  assert(evaluation, s"$result does not equal $expected expected")
}

//*******************************************
// TEST SUITE CLASSES
//*******************************************

// Test case
case class TestCase(description:String,
                    testFunction:()=>Any,
                    id:String=null,
                    dependsOn:Seq[String]=Nil,
                    escapeHTML:Boolean=false,
                    points:Int=1)

// Test result
case class TestResult(test: TestCase, skipped:Boolean = false, debug:Boolean = false) {
  val exception: Option[Throwable] = {
    if (skipped)
      None
    else if (debug) {
      if (test.testFunction() != false)
        None
      else 
        Some(new AssertionError("Test returned false"))
    } else {
      try {
        assert(test.testFunction() != false, "Test returned false")
        None
      } catch {
        case e: Exception => Some(e)
        case e: AssertionError => Some(e)
      }
    }
  }

  val passed: Boolean = !skipped && exception.isEmpty

  val message: String = {
    exception.map(ex => {
      val msg = ex.getMessage()
      if (msg == null || msg.isEmpty()) ex.toString() else msg
    }).getOrElse("")
  }
  
  val status: String = if (skipped) "skipped" else if (passed) "passed" else "failed"
  
  val points: Int = if (passed) test.points else 0
}

val testResultsStyle = """
<style>
  table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
  caption { text-align: left; padding: 5px }
  th, td { border: 1px solid #ddd; padding: 5px }
  th { background-color: #ddd }
  .passed { background-color: #97d897 }
  .failed { background-color: #e2716c }
  .skipped { background-color: #f9d275 }
  .results .points { display: none }
  .results .message { display: none }
  .results .passed::before  { content: "Passed" }
  .results .failed::before  { content: "Failed" }
  .results .skipped::before { content: "Skipped" }
  .grade .passed  .message:empty::before { content:"Passed" }
  .grade .failed  .message:empty::before { content:"Failed" }
  .grade .skipped .message:empty::before { content:"Skipped" }
</style>
    """.trim

object TestResultsAggregator {
  val testResults = scala.collection.mutable.Map[String,TestResult]()
  def update(result:TestResult):TestResult = {
    testResults.put(result.test.id, result)
    return result
  }
  def score = testResults.values.map(_.points).sum
  def maxScore = testResults.values.map(_.test.points).sum
  def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
  def displayResults():Unit = {
    displayHTML(testResultsStyle + s"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">${score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">${maxScore-score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">${percentage}%</td></tr>
    </table>
    """)
  }
}

// Test Suite
case class TestSuite(initialTestCases : TestCase*) {
  
  val testCases : scala.collection.mutable.ArrayBuffer[TestCase] = scala.collection.mutable.ArrayBuffer[TestCase]()
  val ids : scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
  
  initialTestCases.foreach(addTest)
  
  import scala.collection.mutable.ListBuffer
  import scala.xml.Utility.escape
  
  def runTests(debug:Boolean = false):scala.collection.mutable.ArrayBuffer[TestResult] = {
    val failedTests = scala.collection.mutable.Set[String]()
    val results = scala.collection.mutable.ArrayBuffer[TestResult]()
    for (testCase <- testCases) {
      val skip = testCase.dependsOn.exists(failedTests contains _)
      val result = TestResult(testCase, skip, debug)

      if (result.passed == false && testCase.id != null) {
        failedTests += testCase.id
      }
      val eventId = "Test-" + (if (result.test.id != null) result.test.id 
                          else if (result.test.description != null) result.test.description.replaceAll("[^a-zA-Z0-9_]", "").toUpperCase() 
                          else java.util.UUID.randomUUID)
      val message = s"${eventId}\n${result.test.description}\n${result.status}\n${result.points}"
      daLogger.logEvent(eventId, message)
      
      results += result
      TestResultsAggregator.update(result)
    }
    return results
  }

  lazy val testResults = runTests()

  private def display(cssClass:String="results", debug:Boolean=false) : Unit = {
    val testResults = if (!debug) this.testResults else runTests(debug=true)
    val lines = ListBuffer[String]()
    lines += testResultsStyle
    lines += s"<table class='$cssClass'>"
    lines += "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"
    for (result <- testResults) {
      val resultHTML = s"<td class='result ${result.status}'><span class='message'>${result.message}</span></td>"
      val descriptionHTML = if (result.test.escapeHTML) escape(result.test.description) else result.test.description
      lines += s"  <tr><td class='points'>${result.points}</td><td class='test'>$descriptionHTML</td>$resultHTML</tr>"
    }
    lines += s"  <caption class='points'>Score: $score</caption>"
    lines += "</table>"
    val html = lines.mkString("\n")
    displayHTML(html)
  }
  
  def displayResults() : Unit = {
    display("results")
  }
  
  def grade() : Int = {
    display("grade")
    score
  }
  
  def debug() : Unit = {
    display("grade", debug=true)
  }
  
  def score = testResults.map(_.points).sum
  
  def maxScore : Integer = testCases.map(_.points).sum

  def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
  
  def addTest(testCase: TestCase):TestSuite = {
    if (testCase.id == null) throw new IllegalArgumentException("The test cases' id must be specified")
    if (ids.contains(testCase.id)) throw new IllegalArgumentException(f"Duplicate test case id: {testCase.id}")
    testCases += testCase
    ids += testCase.id
    return this
  }
  
  def test(id:String, description:String, testFunction:()=>Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testEquals(id:String, description:String, valueA:Any, valueB:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> (valueA == valueB)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testFloats(id:String, description:String, valueA: Any, valueB: Any, tolerance: Double=0.01, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareFloats(valueA, valueB, tolerance)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testRows(id:String, description:String, rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareRows(rowA, rowB)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testDataFrames(id:String, description:String, dfA:org.apache.spark.sql.DataFrame, dfB:org.apache.spark.sql.DataFrame, testColumnOrder:Boolean, testNullable:Boolean, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareDataFrames(dfA, dfB, testColumnOrder, testNullable)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testSchemas(id:String, description:String, schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, testColumnOrder: Boolean, testNullable: Boolean, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareSchemas(schemaA, schemaB, testColumnOrder, testNullable)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }

  def testContains(id:String, description:String, list:Seq[Any], value:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> list.contains(value)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
}

//*******************************************
// SCORE FUNCTIONS AND PLACEHOLDER GENERATION
//*******************************************

def getQueryString(df: org.apache.spark.sql.Dataset[Row]): String = {
  df.explain.toString
}

//*******************************************
// Test Suite comparison functions
//*******************************************

def compareFloats(valueA: Any, valueB: Any, tolerance: Double=0.01): Boolean = {
  // Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  //        compareFloats(valueA, valueB, tolerance=0.001)
  
  import scala.math
  try{
       if (valueA == null && valueB == null) {
         true
       } else if (valueA == null || valueB == null) {
         false
       } else {
         math.abs(valueA.asInstanceOf[Number].doubleValue - valueB.asInstanceOf[Number].doubleValue) <= tolerance 
       }
  } catch {
    case e: ClassCastException => {
      false
   }   
  }
}


def compareRows(rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row): Boolean = {
  // Usage: compareRows(rowA, rowB)
  // compares two rows as unordered Maps

  if (rowA == null && rowB == null) {
    true
    
  } else if (rowA == null || rowB == null) {
    false
    
  // for some reason, the schema didn't make it
  } else if (rowA.schema == null || rowB.schema == null) {
    rowA.toSeq.toSet == rowB.toSeq.toSet
    
  } else {
    rowA.getValuesMap[String](rowA.schema.fieldNames.toSeq) == rowB.getValuesMap[String](rowB.schema.fieldNames.toSeq)
  }
}


def compareDataFrames(dfA: org.apache.spark.sql.DataFrame, dfB: org.apache.spark.sql.DataFrame, testColumnOrder: Boolean, testNullable: Boolean): Boolean = {
  if (dfA == null && dfB == null) return true
  if (dfA == null || dfB == null) return false
  if (compareSchemas(dfA.schema, dfB.schema, testColumnOrder, testNullable) == false) return false
  if (dfA.count != dfB.count) return false

  val rowsA = dfA.collect()
  val rowsB = dfB.collect()
  
  for (i <- 0 until rowsA.length) {
    val rowA = rowsA(i)
    val rowB = rowsB(i)
    for (column <- dfA.columns) {
      val valueA = rowA.getAs[Any](column)
      val valueB = rowB.getAs[Any](column)
      if (valueA != valueB) return false
    }
  }
  return true
}


def compareSchemas(schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, testColumnOrder: Boolean, testNullable: Boolean): Boolean = {
  
  if (schemaA == null && schemaB == null) return true
  if (schemaA == null || schemaB == null) return false
  
  var schA = schemaA.toSeq
  var schB = schemaB.toSeq

  if (testNullable == false) {   
    schA = schemaA.map(_.copy(nullable=true)) 
    schB = schemaB.map(_.copy(nullable=true)) 
  }

  if (testColumnOrder == true) {
    schA == schB
  } else {
    schA.toSet == schB.toSet
  }
}

displayHTML("Initializing Databricks Academy's testing framework...")

