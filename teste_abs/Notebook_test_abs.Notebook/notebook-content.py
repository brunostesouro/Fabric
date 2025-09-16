# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a99f21d-4de8-4298-a01b-3447b08b9d08",
# META       "default_lakehouse_name": "teste_abs",
# META       "default_lakehouse_workspace_id": "94061179-8192-4e7e-a03f-32adf4afa670",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a99f21d-4de8-4298-a01b-3447b08b9d08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


df = spark.sql("SELECT * FROM teste_abs.sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.drop("TaxAmount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("new_sales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
