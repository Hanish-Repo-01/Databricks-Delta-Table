# Databricks notebook source

courseType = "il"
courseAdvertisements = dict()

displayHTML("Preparing the Python environment...")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Utility-Methods

# COMMAND ----------

moduleName = getModuleName()
lessonName = getLessonName()
username = getUsername()
userhome = getUserhome()
workingDir = getWorkingDir(courseType)
databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

# ****************************************************************************
# Advertise variables we declared for the user - there are more, but these are the common ones
# ****************************************************************************
courseAdvertisements["moduleName"] =   ("v", moduleName,   "No additional information was provided.")
courseAdvertisements["lessonName"] =   ("v", lessonName,   "No additional information was provided.")
courseAdvertisements["username"] =     ("v", username,     "No additional information was provided.")
courseAdvertisements["userhome"] =     ("v", userhome,     "No additional information was provided.")
courseAdvertisements["workingDir"] =   ("v", workingDir,   "No additional information was provided.")
courseAdvertisements["databaseName"] = ("d", databaseName, "This is a private, per-notebook, database used to provide isolation from other users and exercises.")

# ****************************************************************************
# Advertise functions we declared for the user - there are more, but these are the common ones
# ****************************************************************************
courseAdvertisements["untilStreamIsReady"] = ("f", "name, progressions=3", """
  <div>Introduced in the course <b>Structured Streaming</b>, this method blocks until the stream is actually ready for processing.</div>
  <div>By default, it waits for 3 progressions of the stream to ensure sufficent data has been processed.</div>""")
courseAdvertisements["stopAllStreams"] = ("f", "", """
  <div>Introduced in the course <b>Structured Streaming</b>, this method stops all active streams while providing extra exception handling.</div>
  <div>It is functionally equivilent to:<div>
  <div><code>for (s <- spark.streams.active) s.stop()</code></div>""")

displayHTML("Defining custom variables for this lesson...")

# ****************************************************************************
# Advertise are done here 100% of the time so that the are consistently 
# formatted. If left to each lesson, the meta data associated to each has to 
# be maintained in dozens of different places. This means that if a course
# developer doesn't use a variable/function, then can simply suppress it later
# by setting com.databricks.training.suppress.{key} to "true"
# ****************************************************************************

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

# MAGIC %run ./Dummy-Data-Generator

# COMMAND ----------

# MAGIC %run ./Dataset-Mounts

# COMMAND ----------

# This script sets up MLflow and handles the case that 
# it is executed by Databricks' automated testing server

def mlflowAttached():
  try:
    import mlflow
    return True
  except ImportError:
    return False

if mlflowAttached():
  import os
  import mlflow
  from mlflow.tracking import MlflowClient
  from databricks_cli.configure.provider import get_config
  from mlflow.exceptions import RestException
  
  notebookId = getTag("notebookId")
  os.environ['DATABRICKS_HOST'] = get_config().host
  os.environ['DATABRICKS_TOKEN'] = get_config().token
  
  if notebookId:
    os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
                    
  else:
    # Handles notebooks run by test server (executed as run)
    # Convention is to use the notebook's name in the users' home directory which our testing framework abides by
    experiment_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    client = MlflowClient()
    
    try: experiment = client.get_experiment_by_name(experiment_name)
    except Exception as e: pass # experiment doesn't exists

    if experiment:
      # Delete past runs if possible
      try: client.delete_experiment(experiment.experiment_id) 
      except Exception as e: pass # ignored

    try: mlflow.create_experiment(experiment_name)
    except Exception as e: pass # ignored
    
    os.environ['MLFLOW_EXPERIMENT_NAME'] = experiment_name
  
  # Silence YAML deprecation issue https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
  os.environ["PYTHONWARNINGS"] = 'ignore::yaml.YAMLLoadWarning' 
  
None # suppress output

# COMMAND ----------

classroomCleanup(daLogger, courseType, username, moduleName, lessonName, False)
None # Suppress output

# COMMAND ----------

assertDbrVersion(spark.conf.get("com.databricks.training.expected-dbr", None))
None # Suppress output

