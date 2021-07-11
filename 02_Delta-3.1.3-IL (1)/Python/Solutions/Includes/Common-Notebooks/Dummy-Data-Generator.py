# Databricks notebook source

class DummyData:
  from datetime import datetime
  from pyspark.sql import DataFrame
  from pyspark.sql import functions
  from pyspark.sql.window import Window
  from pyspark.sql.types import IntegerType, StringType, TimestampType, NullType
  from math import ceil
  from string import ascii_letters, digits
  import pyspark.sql.functions as F
  import re, random

  
  def __init__(self, tableName, defaultDatabaseName=databaseName, seed=None, numRows=300):
    
    self.__tableName = tableName
    self.__numRows = numRows
    
    # create database for user
    username = getUsername()
    userhome = getUserhome()

    self.__dbName = defaultDatabaseName
    self.__dbName = self.re.sub("[^a-zA-Z]", "", self.__dbName)

    spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(self.__dbName))

    # set initial seed number
    seed = userhome if seed is None else seed
    self.__seedNum = hash(seed)
      
    # initialize dataframe
    self.__id = "id"
    self.__df = spark.range(self.__numRows)
    
    # words reference
    self.__loremIpsum = "amet luctus venenatis lectus magna fringilla urna porttitor rhoncus dolor purus non enim praesent elementum facilisis leo vel fringilla est ullamcorper eget nulla facilisi etiam dignissim diam quis enim lobortis scelerisque fermentum dui faucibus in ornare quam viverra orci sagittis eu volutpat odio facilisis mauris sit amet massa vitae tortor condimentum lacinia quis vel eros donec ac odio tempor orci dapibus ultrices in iaculis nunc sed augue lacus viverra vitae congue eu consequat ac felis donec et odio pellentesque diam volutpat commodo sed egestas egestas fringilla phasellus faucibus scelerisque eleifend donec pretium vulputate sapien nec sagittis aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc sed velit dignissim sodales ut eu sem integer vitae justo eget magna fermentum iaculis eu non diam phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet enim tortor at auctor urna nunc id cursus metus aliquam eleifend mi in nulla posuere sollicitudin aliquam ultrices sagittis orci a scelerisque purus semper eget duis at tellus at urna condimentum mattis pellentesque id nibh tortor id aliquet lectus proin nibh nisl condimentum id venenatis a condimentum vitae sapien pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas sed tempus urna et pharetra pharetra massa"
    
    # states reference
    self.__states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
                     "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
                     "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio",
                     "Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia",
                     "Washington","West Virginia","Wisconsin","Wyoming"]
    
    # chars reference
    self.__chars = self.ascii_letters + self.digits
    
  def __getSeed(self):
    self.__seedNum += 1
    return self.__seedNum  
    
  def toDF(self):
    fullTableName = self.__dbName + "." + self.__tableName + "_p"
    self.__df.write.format("delta").mode("overwrite").saveAsTable(fullTableName)
    return spark.read.table(fullTableName).orderBy(self.__id)
  
  def renameId(self, name):
    self.__df = self.__df.withColumnRenamed(self.__id, name)
    self.__id = name
    return self
  
  def makeNull(self, name, proportion = 0.2): 
    self.__df = (self.addBooleans("_isNull", proportion).__df
           .withColumn(name, self.F.when(self.F.col("_isNull") == True, None)
           .otherwise(self.F.col(name)))
           .drop("_isNull"))
    return self
  
  def addIntegers(self, name: str, low: float = 0, high: float = 5000):
    self.__df = self.__df.withColumn(name, (self.F.rand(self.__getSeed()) * (high - low) + low).cast(self.IntegerType()))
    return self
  
  def addDoubles(self, name, low = 0, high = 5000, roundNum = 6):
    self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, roundNum))
    return self

  def addProportions(self, name, roundNum = 6):
    self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()), roundNum))
    return self
   
  def addBooleans(self, name, proportionTrue = 0.5):
    self.__df = self.__df.withColumn(name, self.F.rand(self.__getSeed()) < proportionTrue)
    return self
    
  def addPriceDoubles(self, name, low = 100, high = 5000):
    self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, 2))
    return self
    
  def addPriceStrings(self, name, low = 100, high = 5000):
    self.__df = self.__df.withColumn(name, self.F.format_number(self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, 2), 2))
    self.__df = self.__df.withColumn(name, self.F.concat(self.F.lit("$"), name))
    return self
  
  def addCategories(self, name, categories = ["first", "second", "third", "fourth"]):
    self.__df = self.__df.withColumn(name, (self.F.rand(self.__getSeed()) * len(categories)).cast(self.IntegerType()))
    tempDF = sqlContext.createDataFrame(zip(range(len(categories)), categories), schema=[name, name + "Text"])
    self.__df = self.__df.join(self.F.broadcast(tempDF), name)
    self.__df = self.__df.drop(name)
    self.__df = self.__df.withColumnRenamed(name + "Text", name)
    return self
  
  def addPasswords(self, name: str = "password"):
    w = self.Window().orderBy(self.F.lit('A'))
    passwordDF = (spark
                    .createDataFrame(
                      [''.join(self.__chars[self.random.randint(0, len(self.__chars) - 1)] for i in range(8,20)) for x in range(self.__numRows)], 
                      schema = StringType()
                    )
                    .withColumnRenamed("value", name)
                    .withColumn("row_num", self.F.row_number().over(w)))
    
    self.__df = self.__df.withColumn("row_num", self.F.row_number().over(w))
    self.__df = self.__df.join(passwordDF, "row_num").drop("row_num")
    return self
  
  def addWords(self, name, num = 5):
    loremCount = len(self.__loremIpsum.split(" "))
    words = (self.__loremIpsum + " ") * int(self.ceil(self.__numRows / float(loremCount)))
    word_list = words.split(" ")
    
    self.__df = self.__df.withColumn(name, self.F.lit(""))
    self.random.seed(self.__getSeed())
    
    for i in range(num):
      self.random.shuffle(word_list)
      word_data = list(zip(word_list, range(0, len(word_list))))
      
      wordsDF = (spark.createDataFrame(word_data, ["word" + str(i), self.__id])
                      .sort(self.__id).limit(self.__numRows))
      
      self.__df = (self.__df.join(wordsDF, self.__id)
                       .withColumn(name, self.F.concat(self.F.col(name), self.F.lit(" "), self.F.col("word" + str(i))))
                       .drop("word" + str(i)))
      
    self.__df = self.__df.withColumn(name, self.F.ltrim(self.F.col(name)))
    return self
    
  def addNames(self, name, num = 2):
    self.__df = self.addWords(name, num).__df.withColumn(name, self.F.initcap(self.F.col(name)))
    return self
    
  def addWordArrays(self, name, num = 5):
    self.__df = self.addWords(name, num).__df.withColumn(name, self.F.split(self.F.col(name), " "))
    return self

  def addTimestamps(self, name, start_date_expr = "2015-08-05 12:00:00", end_date_expr = "2019-08-05 12:00:00", format = "%Y-%m-%d %H:%M:%S"):
    start_timestamp = self.datetime.strptime(start_date_expr, format).timestamp()
    end_timestamp = self.datetime.strptime(end_date_expr, format).timestamp()
    self.__df = self.addIntegers(name, start_timestamp, end_timestamp).__df
    return self
  
  def addDateStrings(self, name, start_date_expr = "2015-08-05 12:00:00", end_date_expr = "2019-08-05 12:00:00", format = "yyyy-MM-dd HH:mm:ss"):
    self.__df = (self.addTimestamps(name, start_date_expr, end_date_expr).__df
                     .withColumn(name, self.F.date_format(self.F.col(name).cast(self.TimestampType()), format)))
    return self
  
  def addStates(self, name):
    self.__df = self.addCategories(name, self.__states).__df
    return self
  
  # needs to be done: add probabilities to categories
  # needs to be done: add arrays of all types
  # needs to be done: add normal distribution, skewed data
  # needs to be done: add data dependent on another column

displayHTML("Initializing Databricks Academy's services for generating dynamic data...")

