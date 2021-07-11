# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Class-Utility-Methods-Test
# MAGIC The purpose of this notebook is to faciliate testing of courseware-specific utility methos.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %md
# MAGIC a lot of these tests evolve around the current DBR version.
# MAGIC 
# MAGIC It shall be assumed that the cluster is configured properly and that these tests are updated with each publishing of courseware against a new DBR

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

def functionPassed(result):
  if not result:
    raise AssertionError("Test failed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTags`

# COMMAND ----------

def testGetTags():
  
    testTags = getTags()
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test that getTags returns correct type
    testsPassed.append(None)
    try:
        from py4j.java_collections import JavaMap
        assert isinstance(getTags(), JavaMap)
        passedTest(True)
    except:
        passedTest(False, "The correct type is not returned by getTags")
        
    # Test that getTags does not return an empty dict
    testsPassed.append(None)
    try:
        assert len(testTags) > 0
        passedTest(True)
    except:
        passedTest(False, "A non-empty dict is returned by getTags")
    
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getTags passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getTags passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetTags()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTag()`

# COMMAND ----------

def testGetTag():
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test that getTag returns null when defaultValue is not set and tag is not present
    testsPassed.append(None)
    try:
        assert getTag("thiswillneverbeincluded") == None
        passedTest(True)
    except:
        passedTest(False, "NoneType is not returned when defaultValue is not set and tag is not present for getTag")
        
    # Test that getTag returns defaultValue when it is set and tag is not present
    testsPassed.append(None)
    try:
        assert getTag("thiswillneverbeincluded", "default-value") == "default-value"
        passedTest(True)
    except:
        passedTest(False, "defaultValue is not returned when defaultValue is set and tag is not present for getTag")
        
    # Test that getTag returns correct value when default value is not set and tag is present
    testsPassed.append(None)
    try:
        orgId = getTags()["orgId"]
        assert isinstance(orgId, str)
        assert len(orgId) > 0
        assert orgId == getTag("orgId")
        passedTest(True)
    except:
        passedTest(False, "A non-empty dict is returned by getTags")
    
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getTag passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getTag passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetTag()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getDbrMajorAndMinorVersions()`

# COMMAND ----------

def testGetDbrMajorAndMinorVersions():
  import os
  # We cannot rely on the assumption that all courses are
  # running latestDbrMajor.latestDbrMinor. The best we
  # can do here is make sure it matches the environment
  # variable from which it came.
  dbrVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
  
  (major,minor) = getDbrMajorAndMinorVersions()
  assert dbrVersion.startswith(f"{major}.{minor}"), f"Found {major}.{minor} for {dbrVersion}"
  
  return True
      
functionPassed(testGetDbrMajorAndMinorVersions()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getPythonVersion()`

# COMMAND ----------

def testGetPythonVersion():
    
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test output for structure
    testsPassed.append(None)
    try:
        pythonVersion = getPythonVersion()
        assert isinstance(pythonVersion, str)
        assert len(pythonVersion.split(".")) >= 2
        passedTest(True)
    except:
        passedTest(False, "pythonVersion does not match expected structure")
        
    # Test output for correctness
    testsPassed.append(None)
    try:
        pythonVersion = getPythonVersion()
        assert pythonVersion[0] == "2" or pythonVersion[0] == "3"
        passedTest(True)
    except:
        passedTest(False, "pythonVersion does not match expected value")
        

    # Print final info and return
    if all(testsPassed):
        print('All {} tests for getPythonVersion passed'.format(len(testsPassed)))
        return True
    else:
        raise Exception('{} of {} tests for getPythonVersion passed'.format(testsPassed.count(True), len(testsPassed)))

functionPassed(testGetPythonVersion()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUsername()`

# COMMAND ----------

def testGetUsername():
  username = getUsername()
  assert isinstance(username, str)
  assert username != ""
  
  return True
    
functionPassed(testGetUsername()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUserhome`

# COMMAND ----------

def testGetUserhome():
  userhome = getUserhome()
  assert isinstance(userhome, str)
  assert userhome != ""
  assert userhome == "dbfs:/user/" + getUsername()
    
  return True

functionPassed(testGetUserhome()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertDbrVersion`

# COMMAND ----------

def testAssertDbrVersion():
  
  (majorVersion, minorVersion) = getDbrMajorAndMinorVersions()
  major = int(majorVersion)
  minor = int(minorVersion)

  goodVersions = [
    ("PG1", major-1, minor-1),
    ("PG2", major-1, minor),
    ("PG3", major, minor-1),
    ("PG4", major, minor)
  ]
  
  for (name, testMajor, testMinor) in goodVersions:
    print(f"-- {name} {testMajor}.{testMinor}")
    assertDbrVersion(None, testMajor, testMinor, False)
    print(f"-"*80)

  badVersions = [
    ("PB1", major+1, minor+1),
    ("PB2", major+1, minor),
    ("PB3", major, minor+1)
  ]

  for (name, testMajor, testMinor) in badVersions:
    try:
      print(f"-- {name} {testMajor}.{testMinor}")
      assertDbrVersion(None, testMajor, testMinor, False)
      raise Exception("Expected AssertionError")
      
    except AssertionError as e:
      print(e)
    
    print(f"-"*80)

  return True
        
functionPassed(testAssertDbrVersion())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertIsMlRuntime`

# COMMAND ----------

# def testAssertIsMlRuntime():

#   assertIsMlRuntime("6.3.x-ml-scala2.11")
#   assertIsMlRuntime("6.3.x-cpu-ml-scala2.11")

#   try:
#     assertIsMlRuntime("5.5.x-scala2.11")
#     assert False, "Expected to throw an ValueError"
#   except AssertionError:
#     pass

#   try:
#     assertIsMlRuntime("5.5.xml-scala2.11")
#     assert False, "Expected to throw an ValueError"
#   except AssertionError:
#     pass

#   return True

# functionPassed(testAssertIsMlRuntime())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Legacy Functions
# MAGIC 
# MAGIC Note: Legacy functions will not be tested. Use at your own risk.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `createUserDatabase`

# COMMAND ----------

def testCreateUserDatabase(): 

  courseType = "wa"
  username = "mickey.mouse@disney.com"
  moduleName = "Testing-Stuff 101"
  lessonName = "TS 03 - Underwater Basket Weaving"
  
  # Test that correct database name is returned
  expectedDatabaseName = "mickey_mouse_disney_com" + "_" + "testing_stuff_101" + "_" + "ts_03_underwater_basket_weaving" + "_" + "p" + "wa"
  
  databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
  assert databaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)
  
  actualDatabaseName = createUserDatabase(courseType, username, moduleName, lessonName)
  assert actualDatabaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)

  assert spark.sql(f"SHOW DATABASES LIKE '{expectedDatabaseName}'").first()["databaseName"] == expectedDatabaseName
  assert spark.sql("SELECT current_database()").first()["current_database()"] == expectedDatabaseName
  
  return True

functionPassed(testCreateUserDatabase())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getExperimentId()`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `classroomCleanup()`

# COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test FILL_IN

# COMMAND ----------

print(FILL_IN)
print(FILL_IN.VALUE)
print(FILL_IN.LIST)
print(FILL_IN.SCHEMA)
print(FILL_IN.ROW)
print(FILL_IN.INT)
print(FILL_IN.DATAFRAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `showStudentSurvey()`

# COMMAND ----------

html = renderStudentSurvey()
print(html)

# COMMAND ----------

showStudentSurvey()

# COMMAND ----------


