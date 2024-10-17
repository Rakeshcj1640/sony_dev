# Databricks notebook source
from pyspark import *
from pyspark.sql.functions import *

# COMMAND ----------

print("training")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Core
# MAGIC RDD DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame: Strucutured API

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame Function:</br>
# MAGIC alias,</br>
# MAGIC select,</br>
# MAGIC withColumnRenamed,</br>
# MAGIC withColumnsRenamed,</br>
# MAGIC withColumn
# MAGIC
# MAGIC pySpark Functions:</br>
# MAGIC col

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = ["id","name","age"]

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = "id int, name string, age int"
df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.select('*').display()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df1=df.select(col("id").alias("emp_id"),"age")

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df_new=df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"})

# COMMAND ----------

df.withColumn("current_data", current_date()).display()
