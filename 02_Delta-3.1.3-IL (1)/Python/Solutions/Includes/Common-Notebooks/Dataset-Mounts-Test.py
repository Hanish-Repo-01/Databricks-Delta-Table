# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Dataset-Mounts-Test
# MAGIC The purpose of this notebook is to faciliate testing of our systems.

# COMMAND ----------

spark.conf.set("com.databricks.training.module-name", "dataset-mounts-test")

# COMMAND ----------

# MAGIC %run ./Dataset-Mounts

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val testStart = System.currentTimeMillis
# MAGIC val mountPointBase = "/mnt/training-test"
# MAGIC 
# MAGIC def unmount(mountPoint:String):Unit = {
# MAGIC   try {
# MAGIC     dbutils.fs.unmount(mountPoint)
# MAGIC   } catch {
# MAGIC     case e:Exception => println(s"Not mounted: $mountPoint")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def testRegion(regionType:String, regionName:String, mapper: (String) => (String,Map[String,String])):Unit = {
# MAGIC   val start = System.currentTimeMillis
# MAGIC   
# MAGIC   val (source, extraConfigs) = mapper(regionName)
# MAGIC   val mountPoint = s"$mountPointBase-${regionType.toLowerCase}-$regionName"
# MAGIC   println(s"""\nTesting the $regionType region $regionName ($mountPoint)""")
# MAGIC 
# MAGIC   mountSource(true, false, mountPoint, source, extraConfigs)
# MAGIC   Thread.sleep(5*1000) // give it a second
# MAGIC   validateDatasets(mountPoint)
# MAGIC   
# MAGIC   val duration = (System.currentTimeMillis - start) / 1000.0
# MAGIC   println(f"...all tests passed in $duration%1.2f seconds!")
# MAGIC }
# MAGIC 
# MAGIC def validateDataset(mountPoint:String, target:String):Unit = {
# MAGIC   val map = scala.collection.mutable.Map[String,(Long,Long)]()
# MAGIC   for (file <- dbutils.fs.ls(s"/mnt/training/$target")) {
# MAGIC     map.put(file.name, (file.size, -1L))
# MAGIC   }
# MAGIC 
# MAGIC   val path = s"$mountPoint/$target"
# MAGIC   for (file <- dbutils.fs.ls(path)) {
# MAGIC       if (map.contains(file.name)) {
# MAGIC         val (sizes, _) = map(file.name)
# MAGIC         map.put(file.name, (sizes, file.size))
# MAGIC       } else {
# MAGIC         map.put(file.name, (-1, file.size))
# MAGIC       }
# MAGIC   }
# MAGIC   
# MAGIC   var errors = ""
# MAGIC   for (key <- map.keySet) {
# MAGIC     val (sizeA, sizeB) = map(key)
# MAGIC     if (sizeA == sizeB) {
# MAGIC       // Everything matches up... no issue here.
# MAGIC     } else if (sizeA == -1) {
# MAGIC       if (!key.endsWith("_$folder$"))
# MAGIC         errors += s"Extra file: $path$key\n"
# MAGIC     } else if (sizeB == -1) {
# MAGIC       errors += s"Missing file: $path$key\n"
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   errors = errors.trim()
# MAGIC   if (errors != "") {
# MAGIC     println(errors)
# MAGIC     throw new IllegalStateException(s"Errors were found while processing $path")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC def validateDatasets(mountPoint:String) {
# MAGIC   val paths = List(
# MAGIC     "",
# MAGIC     "301/",
# MAGIC     "Chicago-Crimes-2018.csv",
# MAGIC     "City-Data.parquet/",
# MAGIC     "EDGAR-Log-20170329/",
# MAGIC     "UbiqLog4UCI/",
# MAGIC     "_META/",
# MAGIC     "adventure-works/",
# MAGIC     "airbnb/",
# MAGIC     "airbnb-sf-listings.csv",
# MAGIC     "asa/",
# MAGIC     "auto-mpg.csv",
# MAGIC     "bigrams/",
# MAGIC     "bikeSharing/",
# MAGIC     "bostonhousing/",
# MAGIC     "cancer/",
# MAGIC     "countries/",
# MAGIC     "crime-data-2016/",
# MAGIC     "data/",
# MAGIC     "data-cleansing/",
# MAGIC     "databricks-blog.json",
# MAGIC     "databricks-datasets/",
# MAGIC     "dataframes/",
# MAGIC     "day-of-week/",
# MAGIC     "definitive-guide/",
# MAGIC     "dl/",
# MAGIC     "gaming_data/",
# MAGIC     "global-sales/",
# MAGIC     "graphx-demo/",
# MAGIC     "initech/",
# MAGIC     "ip-geocode.parquet/",
# MAGIC     "iris/",
# MAGIC     "mini_newsgroups/",
# MAGIC     "mnist/",
# MAGIC     "movie-reviews/",
# MAGIC     "movielens/",
# MAGIC     "movies/",
# MAGIC     "online_retail/",
# MAGIC     "philadelphia-crime-data-2015-ytd.csv",
# MAGIC     "purchases.txt",
# MAGIC     "sensor-data/",
# MAGIC     "ssn/",
# MAGIC     "stopwords",
# MAGIC     "structured-streaming/",
# MAGIC     "test.log",
# MAGIC     "tom-sawyer/",
# MAGIC     "tweets.txt",
# MAGIC     "twitter/",
# MAGIC     "wash_dc_crime_incidents_2013.csv",
# MAGIC     "wash_dc_crime_incidents_2015-10-03-to-2016-10-02.csv",
# MAGIC     "weather/",
# MAGIC     "wikipedia/",
# MAGIC     "wine.parquet/",
# MAGIC     "word-game-dict.txt",
# MAGIC     "zip3state.csv",
# MAGIC     "zips.json"
# MAGIC   )
# MAGIC   for (path <- paths) {
# MAGIC     validateDataset(mountPoint, path)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val awsRegions=List(
# MAGIC   "us-west-2",
# MAGIC   "ap-northeast-1",
# MAGIC   "ap-northeast-2",
# MAGIC   "ap-south-1",
# MAGIC   "ap-southeast-1",
# MAGIC   "ap-southeast-2",
# MAGIC   "ca-central-1",
# MAGIC   "eu-central-1",
# MAGIC   "eu-west-1",
# MAGIC   "eu-west-2",
# MAGIC   "eu-west-3",
# MAGIC   "sa-east-1",
# MAGIC   "us-east-1",
# MAGIC   "us-east-2"
# MAGIC ).map(_.toLowerCase())
# MAGIC 
# MAGIC val azureRegions=List(
# MAGIC  "AustraliaCentral",
# MAGIC  "AustraliaCentral2",
# MAGIC  "AustraliaEast",
# MAGIC  "AustraliaSoutheast",
# MAGIC  "CanadaCentral",
# MAGIC  "CanadaEast",
# MAGIC  "CentralIndia",
# MAGIC  "CentralUS",
# MAGIC  "EastAsia",
# MAGIC  "EastUS",
# MAGIC  "EastUS2",
# MAGIC  "JapanEast",
# MAGIC  "JapanWest",
# MAGIC  "NorthCentralUS",
# MAGIC  "NorthCentralUS",
# MAGIC  "NorthEurope",
# MAGIC  "SouthCentralUS",
# MAGIC  "SouthCentralUS",
# MAGIC  "SouthIndia",
# MAGIC  "SoutheastAsia",
# MAGIC  "UKSouth",
# MAGIC  "UKWest",
# MAGIC  "WestCentralUS",  // Azure Databricks isn't available in region, but we historically copied data here anyway.
# MAGIC  "WestEurope",
# MAGIC  "WestIndia",
# MAGIC  "WestUS",
# MAGIC  "WestUS2"
# MAGIC ).map(_.toLowerCase())

# COMMAND ----------

# MAGIC %scala
# MAGIC for (region <- awsRegions) {
# MAGIC   testRegion("AWS", region, getAwsMapping _)
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC for mount in (mount[0] for mount in dbutils.fs.mounts() if "training-test" in mount[0]):
# MAGIC   dbutils.fs.unmount(mount)

# COMMAND ----------

# MAGIC %scala
# MAGIC for (region <- azureRegions) {
# MAGIC   testRegion("Azure", region, getAzureMapping _)
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC println(f"...all tests passed in ${(System.currentTimeMillis - testStart) / 1000.0 / 60.0}%1.2f minutes!")

# COMMAND ----------

# MAGIC %python
# MAGIC for mount in (mount[0] for mount in dbutils.fs.mounts() if "training-test" in mount[0]):
# MAGIC   dbutils.fs.unmount(mount)
