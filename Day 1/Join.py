# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_sales = spark.table("sales")

# COMMAND ----------

df_customers = spark.table("customers")

# COMMAND ----------

df_products = spark.table("products")

# COMMAND ----------

df_joined = df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"],"inner")

# COMMAND ----------

df_joined1 = df_sales.join(df_products,df_sales["product_id"]==df_products["product_id"],"inner")

# COMMAND ----------

display(df_joined)

# COMMAND ----------

display(df_joined1)

# COMMAND ----------


df_customers.filter(col("customer_id")==2).display()

# COMMAND ----------

df_customers.where((col("customer_id")==2) | (col("customer_id")==1)).display()

# COMMAND ----------

df_customers.sort("customer_city",asceding=False).display()

# COMMAND ----------

df_customers.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined = df_sales.join(df_customers,"customer_id","inner")

# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------

df_joined.count()
