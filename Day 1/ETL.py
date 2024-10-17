# Databricks notebook source
# MAGIC %run "/Workspace/Users/rakeshcj2019@gmail.com/Day 1/includes"

# COMMAND ----------

df_sales = spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df1 = add_ingestion(df_sales)

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

# MAGIC %md
# MAGIC Orders_date

# COMMAND ----------

df_orders = spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

df2 = add_ingestion(df_orders)

# COMMAND ----------

df2.write.mode("overwrite").saveAsTable("orders_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Products

# COMMAND ----------

df_products = spark.read.json(f"{input_path}products.json")

# COMMAND ----------

df3 = add_ingestion(df_products)

# COMMAND ----------

df3.write.mode("overwrite").saveAsTable("products")
