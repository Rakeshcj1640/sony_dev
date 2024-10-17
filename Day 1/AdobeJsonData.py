# Databricks notebook source
from pyspark.sql.functions import  *

# COMMAND ----------

jsonDate = [{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]

# COMMAND ----------

df = spark.createDataFrame(data=jsonDate)

# COMMAND ----------

df_b=df.withColumn("batters",explode(col("batters.batter")))\
    .withColumn("batter_type",col("batters.type"))\
        .withColumn("batter_id",col("batters.id")).drop("batters")

# COMMAND ----------

df_b.withColumn("topping",explode(col("topping"))).display()

# COMMAND ----------

df_b.display()

# COMMAND ----------

df.withColumn("batter_id", col("batter.batter"))
df = df.withColumn("batter_id", explode(col("batter"))).display()

# COMMAND ----------

display(df)
