# Databricks notebook source
# MAGIC %md
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/rakesh_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/rakesh_databricks/default/raw/sales.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table products
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/rakesh_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales s
# MAGIC inner join customers c
# MAGIC on s.customer_id = c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers where customer_id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.customer_id, count(*)
# MAGIC from sales s
# MAGIC inner join customers c
# MAGIC on s.customer_id = c.customer_id 
# MAGIC group by c.customer_id
# MAGIC order by c.customer_id
