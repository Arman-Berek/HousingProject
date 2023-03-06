# Databricks notebook source
# create dataframes as pandas
#%%pyspark
data_path = spark.read.load('abfss://housingdatastorage@data-post-etl.dfs.core.windows.net/joined_df.csv/', format='csv', header=True)
data_path.show(10)

print('Converting to Pandas.')

pdf = data_path.toPandas()
print(pdf)

# COMMAND ----------


