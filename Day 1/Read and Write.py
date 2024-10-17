# Databricks notebook source
input_path = "/Volumes/rakesh_databricks/default/raw/"

# COMMAND ----------

df = spark.read.csv(f"{input_path}sales.csv", header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df_customer = spark.read.json("/Volumes/rakesh_databricks/default/raw/customers.json")

# COMMAND ----------

df_customer.write.saveAsTable("customer")

# COMMAND ----------

display(df_customer)
