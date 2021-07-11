// Databricks notebook source

class DummyData(val tableName: String, val defaultDatabaseName:String = databaseName, val seed: String = null, val numRows: Int = 300) {
  import java.sql.Timestamp
  import java.time.format.DateTimeFormatter
  import java.time.LocalDateTime
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions.{broadcast, col, concat, date_format, explode, format_number, initcap, ltrim, monotonically_increasing_id, 
                                         lit, rand, randn, round, shuffle, split, udf, when, row_number}
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.types.{IntegerType, TimestampType}
  import scala.math.ceil
  import scala.util.Random

  // create database for user
  private val username = getUsername()
  private val userhome = getUserhome()
  
  private var dbName = defaultDatabaseName
  dbName = dbName.replaceAll("[^a-zA-Z]","")
  
  spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(dbName))
  
  // set initial seed number
  private var seedNum = if (seed != null) seed.hashCode else userhome.hashCode
  
  // initalize dataframe
  private var id = "id"
  private var df = spark.range(numRows).toDF
  
  // words reference
  private val loremIpsum = "amet luctus venenatis lectus magna fringilla urna porttitor rhoncus dolor purus non enim praesent elementum facilisis leo vel fringilla est ullamcorper eget nulla facilisi etiam dignissim diam quis enim lobortis scelerisque fermentum dui faucibus in ornare quam viverra orci sagittis eu volutpat odio facilisis mauris sit amet massa vitae tortor condimentum lacinia quis vel eros donec ac odio tempor orci dapibus ultrices in iaculis nunc sed augue lacus viverra vitae congue eu consequat ac felis donec et odio pellentesque diam volutpat commodo sed egestas egestas fringilla phasellus faucibus scelerisque eleifend donec pretium vulputate sapien nec sagittis aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc sed velit dignissim sodales ut eu sem integer vitae justo eget magna fermentum iaculis eu non diam phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet enim tortor at auctor urna nunc id cursus metus aliquam eleifend mi in nulla posuere sollicitudin aliquam ultrices sagittis orci a scelerisque purus semper eget duis at tellus at urna condimentum mattis pellentesque id nibh tortor id aliquet lectus proin nibh nisl condimentum id venenatis a condimentum vitae sapien pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas sed tempus urna et pharetra pharetra massa"
  
  // states reference
  private val states = List("Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
                    "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
                    "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio",
                    "Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia",
                    "Washington","West Virginia","Wisconsin","Wyoming")

  private def getSeed(): Int = {
    seedNum += 1
    seedNum
  }  
  
  def toDF(): DataFrame = {
    val fullTableName = dbName + "." + tableName + "_s"
    df.write.format("delta").mode("overwrite").saveAsTable(fullTableName)
    spark.read.table(fullTableName).orderBy(id)
  }  
  
  def renameId(name: String): this.type = {
    df = df.withColumnRenamed(id, name)
    id = name
    this
  }
  
  // TODO: add option to drop an exact number of values - e.g. 6 nulls
  def makeNull(name: String, proportion: Double = 0.2): this.type = {
    df = addBooleans("_isNull", proportion).df
           .withColumn(name, when($"_isNull" === true, null)
             .otherwise(col(name)))
           .drop("_isNull")
    this
  }
  
  def makeDuplicates(proportion: Double = 0.2): this.type = {
    df = df.union(df.sample(proportion))
    this
  }
  
  def addIntegers(name: String, low: Double = 0, high: Double = 5000): this.type = {
    df = df.withColumn(name, (rand(getSeed()) * (high - low) + low).cast(IntegerType))
    this
  }
  
  def addDoubles(name: String, low: Double = 0, high: Double = 5000, roundNum: Int = 6): this.type = {
    df = df.withColumn(name, round(rand(getSeed()) * (high - low) + low, roundNum))
    this
  }

  def addProportions(name: String, roundNum: Int = 6): this.type = {
    df = df.withColumn(name, round(rand(getSeed()), roundNum))
    this
  }
   
  def addBooleans(name: String, proportionTrue: Double = 0.5): this.type = {
    df = df.withColumn(name, rand(getSeed()) < proportionTrue)
    this
  }
    
  def addPriceDoubles(name: String, low: Double = 100, high: Double = 5000): this.type = {
    df = df.withColumn(name, round(rand(getSeed()) * (high - low) + low, 2))
    this
  }
    
  def addPriceStrings(name: String, low: Double = 100, high: Double = 5000): this.type = {
    df = df.withColumn(name, concat(lit("$"), format_number(round(rand(getSeed()) * (high - low) + low, 2), 2)))
    this
  }
    
  def addCategories(name: String, categories: Seq[String] = Seq("first", "second", "third", "fourth")): this.type = {

    df = df.withColumn(name, (rand(getSeed()) * categories.size).cast(IntegerType))
    val tempDF = categories.zipWithIndex.toDF(name + "Text", name)
    df = df.join(broadcast(tempDF), name).drop(name).withColumnRenamed(name + "Text", name)
       
    this
  }
  
  def addPasswords(name: String = "password"): this.type = {
    val w = Window.orderBy(lit("A"))
    val passwords = for (row <- df.collect()) 
      yield new scala.util.Random().alphanumeric.take(8 + new scala.util.Random().nextInt(20 - 8) + 1).mkString("")
    val passwordDF = passwords.toList
                              .toDF()
                              .withColumnRenamed("value", name)
                              .withColumn("row_num", row_number().over(w))
    
    df = df.withColumn("row_num", row_number().over(w))
    df = df.join(passwordDF, "row_num").drop("row_num")
    this
  }
    
  def addWords(name: String, num: Int = 5): this.type = {
    val loremCount = loremIpsum.split(" ").length
    val words = (loremIpsum + " ") * ceil(numRows / loremCount.toDouble).toInt
    val word_list = words.split(" ").toList
    
    // needs to be done: implement worsDF creation using spark/DF to scale
    
    df = df.withColumn(name, lit(""))
    
    val randomGen = new Random(getSeed())
    for (i <- 1 to num) {
      val wordsDF = spark.createDataset(randomGen.shuffle(word_list).zipWithIndex)
                         .toDF("word" + i.toString, id)
                         .sort(id)
                         .limit(numRows)
      
      df = df.join(wordsDF, id)
             .withColumn(name, concat(col(name), lit(" "), col("word" + i.toString)))
             .drop("word" + i.toString)
    }

    df = df.withColumn(name, ltrim(col(name)))
    this
  }
    
  def addNames(name: String, num: Int = 2): this.type = {
    df = addWords(name, num).df.withColumn(name, initcap(col(name)))
    this
  }
    
  def addWordArrays(name: String, num: Int = 5): this.type = {
    df = addWords(name, num).df.withColumn(name, split(col(name), " "))
    this
  }

  def addTimestamps(name: String, start_date_expr: String = "2015-08-05 12:00:00", end_date_expr: String = "2019-08-05 12:00:00", format: String = "yyyy-MM-dd HH:mm:ss"): this.type = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val start_timestamp = Timestamp.valueOf(LocalDateTime.parse(start_date_expr, formatter)).getTime() / 1000
    val end_timestamp = Timestamp.valueOf(LocalDateTime.parse(end_date_expr, formatter)).getTime() / 1000
    df = addIntegers(name, start_timestamp, end_timestamp).df
    this
  }
  
  def addDateStrings(name: String, start_date_expr: String = "2015-08-05 12:00:00", end_date_expr: String = "2019-08-05 12:00:00", format: String = "yyyy-MM-dd HH:mm:ss"): this.type = {
    df = addTimestamps(name, start_date_expr, end_date_expr).df
           .withColumn(name, date_format(col(name).cast(TimestampType), format))
    this
  }
  
  def addStates(name: String): this.type = {
    df = addCategories(name, this.states).df
    this
  }

  // needs to be done: add probabilities to categories
  // needs to be done: add arrays of all types
  // needs to be done: add normal distribution, skewed data
  // needs to be done: add data dependent on another column
  
}

displayHTML("Initializing Databricks Academy's services for generating dynamic data...")

