# Databricks notebook source

import pyspark
from typing import Callable, Any, Iterable, List, Set, Tuple

#############################################
# Deprecated test functions
#############################################

def dbTest(id, expected, result):
  import uuid
  
  if id: eventId = "Test-"+id 
  else: eventId = "Test-"+str(uuid.uuid1())

  evaluation = str(expected) == str(result)
  status = "passed" if evaluation else "failed"
  daLogger.logEvent(id, f"{eventId}\nDBTest Assertion\n{status}\n1")

  assert evaluation, f"{result} does not equal expected {expected}"
  
#############################################
# Test Suite classes
#############################################

# Test case
class TestCase(object):
  __slots__=('description', 'testFunction', 'id', 'dependsOn', 'escapeHTML', 'points')
  def __init__(self,
               description:str,
               testFunction:Callable[[], Any],
               id:str=None,
               dependsOn:Iterable[str]=[],
               escapeHTML:bool=False,
               points:int=1):
    
    self.description=description
    self.testFunction=testFunction
    self.id=id
    self.dependsOn=dependsOn
    self.escapeHTML=escapeHTML
    self.points=points

# Test result
class TestResult(object):
  __slots__ = ('test', 'skipped', 'debug', 'passed', 'status', 'points', 'exception', 'message')
  def __init__(self, test, skipped = False, debug = False):
    try:
      self.test = test
      self.skipped = skipped
      self.debug = debug
      if skipped:
        self.status = 'skipped'
        self.passed = False
        self.points = 0
      else:
        assert test.testFunction() != False, "Test returned false"
        self.status = "passed"
        self.passed = True
        self.points = self.test.points
      self.exception = None
      self.message = ""
    except Exception as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = repr(self.exception)
      if (debug and not isinstance(e, AssertionError)):
        raise e

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

  
testResultsStyle = """
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
    """.strip()


class __TestResultsAggregator(object):
  testResults = dict()
  
  def update(self, result:TestResult):
    self.testResults[result.test.id] = result
    return result
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults.values()))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults.values()))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)

  def displayResults(self):
    displayHTML(testResultsStyle + f"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.maxScore-self.score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
    </table>
    """)
# Lazy-man's singleton
TestResultsAggregator = __TestResultsAggregator()


# Test suite class
class TestSuite(object):
  def __init__(self, initialTestCases: Iterable[TestCase] = None) -> None:
    self.ids = set()
    self.testCases = list()
    if initialTestCases:
      for tC in initialTestCases:
        self.addTest(tC)
        
  @lazy_property
  def testResults(self) -> List[TestResult]:
    return self.runTests()
  
  def runTests(self, debug=False) -> List[TestResult]:
    import re
    import uuid
    failedTests = set()
    testResults = list()

    for test in self.testCases:
      skip = any(testId in failedTests for testId in test.dependsOn)
      result = TestResult(test, skip, debug)

      if (not result.passed and test.id != None):
        failedTests.add(test.id)

      if result.test.id: eventId = "Test-"+result.test.id 
      elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
      else: eventId = "Test-"+str(uuid.uuid1())
      message = f"{eventId}\n{result.test.description}\n{result.status}\n{result.points}"
      daLogger.logEvent(eventId, message)

      testResults.append(result)
      TestResultsAggregator.update(result)
    
    return testResults

  def _display(self, cssClass:str="results", debug=False) -> None:
    from html import escape
    testResults = self.testResults if not debug else self.runTests(debug=True)
    lines = []
    lines.append(testResultsStyle)
    lines.append("<table class='"+cssClass+"'>")
    lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
    for result in testResults:
      resultHTML = "<td class='result "+result.status+"'><span class='message'>"+result.message+"</span></td>"
      descriptionHTML = escape(str(result.test.description)) if (result.test.escapeHTML) else str(result.test.description)
      lines.append("  <tr><td class='points'>"+str(result.points)+"</td><td class='test'>"+descriptionHTML+"</td>"+resultHTML+"</tr>")
    lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
    lines.append("</table>")
    html = "\n".join(lines)
    displayHTML(html)
  
  def displayResults(self) -> None:
    self._display("results")
  
  def grade(self) -> int:
    self._display("grade")
    return self.score
  
  def debug(self) -> None:
    self._display("grade", debug=True)
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)

  def addTest(self, testCase: TestCase):
    if not testCase.id: raise ValueError("The test cases' id must be specified")
    if testCase.id in self.ids: raise ValueError(f"Duplicate test case id: {testCase.id}")
    self.testCases.append(testCase)
    self.ids.add(testCase.id)
    return self
  
  def test(self, id:str, description:str, testFunction:Callable[[], Any], points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testEquals(self, id:str, description:str, valueA, valueB, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: valueA == valueB
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
    
  def testFloats(self, id:str, description:str, valueA, valueB, tolerance=0.01, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareFloats(valueA, valueB, tolerance)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  def testRows(self, id:str, description:str, rowA: pyspark.sql.Row, rowB: pyspark.sql.Row, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareRows(rowA, rowB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testDataFrames(self, id:str, description:str, dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, testColumnOrder: bool, testNullable: bool, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareDataFrames(dfA, dfB, testColumnOrder, testNullable)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testSchemas(self, id:str, description:str, schemaA: pyspark.sql.types.StructType, schemaB: pyspark.sql.types.StructType, testColumnOrder: bool, testNullable: bool, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareSchemas(schemaA, schemaB, testColumnOrder, testNullable)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testContains(self, id:str, description:str, listOfValues, value, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: value in listOfValues
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

#############################################
# Test Suite utilities
#############################################


def getQueryString(df: pyspark.sql.DataFrame) -> str:
  # redirect sys.stdout to a buffer
  import sys, io
  stdout = sys.stdout
  sys.stdout = io.StringIO()

  # call module
  df.explain(extended=True)

  # get output and restore sys.stdout
  output = sys.stdout.getvalue()
  sys.stdout = stdout

  return output


#############################################
# Test Suite comparison functions
#############################################

def compareFloats(valueA, valueB, tolerance=0.01):
  # Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  #        compareFloats(valueA, valueB, tolerance=0.001)
  from builtins import abs 
  
  try:
    if (valueA == None and valueB == None):
         return True
      
    else:
         return abs(float(valueA) - float(valueB)) <= tolerance 
      
  except:
    return False
  
  
def compareRows(rowA: pyspark.sql.Row, rowB: pyspark.sql.Row):
  # Usage: compareRows(rowA, rowB)
  # compares two Dictionaries
  
  if (rowA == None and rowB == None):
    return True
  
  elif (rowA == None or rowB == None):
    return False
  
  else: 
    return rowA.asDict() == rowB.asDict()


def compareDataFrames(dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, testColumnOrder: bool, testNullable: bool):
  if dfA == None and dfB == None: return True
  if dfA == None or dfB == None: return False
  if compareSchemas(dfA.schema, dfB.schema, testColumnOrder, testNullable) == False: return False
  if dfA.count() != dfB.count(): return False

  rowsA = dfA.collect()
  rowsB = dfB.collect()

  for i in range(0, len(rowsA)):
    rowA = rowsA[i]
    rowB = rowsB[i]
    for column in dfA.columns:
      valueA = rowA[column]
      valueB = rowB[column]
      if (valueA != valueB): return False

  return True


def compareSchemas(schemaA: pyspark.sql.types.StructType, schemaB: pyspark.sql.types.StructType, testColumnOrder: bool, testNullable: bool): 
  
  from pyspark.sql.types import StructField
  
  if (schemaA == None and schemaB == None): return True
  if (schemaA == None or schemaB == None): return False
  
  schA = schemaA
  schB = schemaB

  if (testNullable == False):  
      schA = [StructField(s.name, s.dataType, True) for s in schemaA]
      schB = [StructField(s.name, s.dataType, True) for s in schemaB]

  if (testColumnOrder == True):
    return [schA] == [schB]
  else:
    return set(schA) == set(schB)
  
displayHTML("Initializing Databricks Academy's testing framework...")

