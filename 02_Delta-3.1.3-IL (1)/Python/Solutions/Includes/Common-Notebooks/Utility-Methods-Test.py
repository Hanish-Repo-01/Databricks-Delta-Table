# Databricks notebook source

spark.conf.set("com.databricks.training.module-name", "common-notebooks")

courseAdvertisements = dict()

# COMMAND ----------

# MAGIC %run ./Utility-Methods

# COMMAND ----------

pythonTests = []
def functionPassed(result):
  if result:
    pythonTests.append(True)
  else:
    pythonTests.append(False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `printRecordsPerPartition`

# COMMAND ----------

def testPrintRecordsPerPartition():
  
    # Import data
    peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
    
    # Get printed results
    import io
    from contextlib import redirect_stdout

    f = io.StringIO()
    with redirect_stdout(f):
        printRecordsPerPartition(peopleDF)
    out = f.getvalue()
  
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test if correct number of partitions are printing
    testsPassed.append(None)
    try:
        assert int(out[out.rfind('#') + 1]) == peopleDF.rdd.getNumPartitions()
        passedTest(True)
    except:
        passedTest(False, "The correct number of partitions were not identified for printRecordsPerPartition")
        
    # Test if each printed partition has a record number associated
    testsPassed.append(None)
    try:
        output_list = [
          {val.split(" ")[0].replace("#", "").replace(":", ""): int(val.split(" ")[1].replace(",", ""))} 
          for val in out.split("\n") if val and val[0] == "#"
        ]
        assert all([isinstance(x[list(x.keys())[0]], int) for x in output_list])
        passedTest(True)
    except:
        passedTest(False, "Not every partition has an associated record count")
        
    # Test if the sum of the printed number of records per partition equals the total number of records
    testsPassed.append(None)
    try:
        printedSum = sum([
          int(val.split(" ")[1].replace(",", ""))
          for val in out.split("\n") if val and val[0] == "#"
        ])
      
        assert printedSum == peopleDF.count()
        passedTest(True)
    except:
        passedTest(False, "The sum of the number of records per partition does not match the total number of records")
    
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for printRecordsPerPartition passed'.format(len(testsPassed)))
        return True
    else:
        print('{} of {} tests for printRecordsPerPartition passed'.format(testsPassed.count(True), len(testsPassed)))
        return False

functionPassed(testPrintRecordsPerPartition()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `computeFileStats`

# COMMAND ----------

def testComputeFileStats():
  
    # Set file path
    filePath = "/mnt/training/global-sales/transactions/2017.parquet"
  
    # Run and get output
    output = computeFileStats(filePath)
  
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test if correct structure is returned
    testsPassed.append(None)
    try:
        assert isinstance(output, tuple)
        assert len(output) == 2
        assert isinstance(output[0], int)
        assert isinstance(output[1], int)
        passedTest(True)
    except:
        passedTest(False, "The incorrect structure is returned for computeFileStats")
        
    # Test that correct result is returned
    testsPassed.append(None)
    try:
        assert output[0] == 6276
        assert output[1] == 1269333224
        passedTest(True)
    except:
        passedTest(False, "The incorrect result is returned for computeFileStats")
        
    # Test that nonexistent file path throws error
    testsPassed.append(None)
    try:
        computeFileStats("alkshdahdnoinscoinwincwinecw/cw/cw/cd/c/wcdwdfobnwef")
        passedTest(False, "A nonexistent file path did not throw an error for computeFileStats")
    except:
        passedTest(True)
     
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for computeFileStats passed'.format(len(testsPassed)))
        return True
    else:
        print('{} of {} tests for computeFileStats passed'.format(testsPassed.count(True), len(testsPassed)))
        return False

functionPassed(testComputeFileStats()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `cacheAs`

# COMMAND ----------

def testCacheAs():
  
    # Import DF
    inputDF = spark.read.parquet("/mnt/training/global-sales/transactions/2017.parquet").limit(100)
  
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test uncached table gets cached
    testsPassed.append(None)
    try:
        cacheAs(inputDF, "testCacheTable12344321")
        assert spark.catalog.isCached("testCacheTable12344321")
        passedTest(True)
    except:
        passedTest(False, "Uncached table was not cached for cacheAs")
        
    # Test cached table gets recached
    testsPassed.append(None)
    try:
        cacheAs(inputDF, "testCacheTable12344321")
        assert spark.catalog.isCached("testCacheTable12344321")
        passedTest(True)
    except:
        passedTest(False, "Cached table was not recached for cacheAs")
        
    # Test wrong level still gets cached
    testsPassed.append(None)
    try:
        spark.catalog.uncacheTable("testCacheTable12344321")
        cacheAs(inputDF, "testCacheTable12344321", "WRONG_LEVEL")
        assert spark.catalog.isCached("testCacheTable12344321")
        spark.catalog.uncacheTable("testCacheTable12344321")
        passedTest(True)
    except:
        passedTest(False, "Invalid storage level stopping caching for cacheAs")
        
     
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for cacheAs passed'.format(len(testsPassed)))
        return True
    else:
        print('{} of {} tests for cacheAs passed'.format(testsPassed.count(True), len(testsPassed)))
        return False

functionPassed(testCacheAs()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `benchmarkCount()`

# COMMAND ----------

def testBenchmarkCount():
  
    from pyspark.sql import DataFrame
    def testFunction():
      return spark.createDataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
    output = benchmarkCount(testFunction)
 
    # Setup tests
    testsPassed = []
    
    def passedTest(result, message = None):
        if result:
            testsPassed[len(testsPassed) - 1] = True
        else:
            testsPassed[len(testsPassed) - 1] = False
            print('Failed Test: {}'.format(message))
    
    # Test that correct structure is returned
    testsPassed.append(None)
    try:
        assert isinstance(output, tuple)
        assert len(output) == 3
        assert isinstance(output[0], DataFrame)
        assert isinstance(output[1], int)
        assert isinstance(output[2], float)
        passedTest(True)
    except:
        passedTest(False, "Correct structure not returned for benchmarkCount")
        
    # Test that correct result is returned
    testsPassed.append(None)
    try:
        assert output[0].rdd.collect() == testFunction().rdd.collect()
        assert output[1] == testFunction().count()
        assert output[2] > 0 and output[2] < 10000
        passedTest(True)
    except:
        passedTest(False, "Correct structure not returned for benchmarkCount")    
     
    # Print final info and return
    if all(testsPassed):
        print('All {} tests for benchmarkCount passed'.format(len(testsPassed)))
        return True
    else:
        print('{} of {} tests for benchmarkCount passed'.format(testsPassed.count(True), len(testsPassed)))
        return False

functionPassed(testBenchmarkCount()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test **`untilStreamIsReady()`**

# COMMAND ----------

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

initialDF = (spark
  .readStream                            # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger 
  .schema(dataSchema)                    # Required for all streaming DataFrames
  .json(dataPath)                        # The stream's source directory and file type
)

name = "Testing_123"

display(initialDF, streamName = name)
untilStreamIsReady(name)
assert len(spark.streams.active) == 1, "Expected 1 active stream, found " + str(len(spark.streams.active))

# COMMAND ----------

for stream in spark.streams.active:
  stream.stop()
  queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
  while (len(queries) > 0):
    time.sleep(5) # Give it a couple of seconds
    queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
  print("""The stream "{}" has been terminated.""".format(stream.name))

# COMMAND ----------

if all(pythonTests):
    print('All {} tests for Python passed'.format(len(pythonTests)))
else:
    print('{} of {} tests for Python passed'.format(pythonTests.count(True), len(pythonTests)))
    raise Exception('{} of {} tests for Python passed'.format(pythonTests.count(True), len(pythonTests)))

