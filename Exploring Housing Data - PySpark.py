# Databricks notebook source
# MAGIC %md
# MAGIC ## HPI Master

# COMMAND ----------

# create dataframes as PySpark
hpi_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Repos/GitHub/HousingProject/HPI_master.csv")
)

# test that dfs loaded
# display((hpi_df.count(), len(hpi_df.columns))) # 121462 rows, 10 columns
# display(hpi_df.limit(10))

# filter to quarterly only
hpi_df_quarterly = hpi_df.filter(hpi_df.frequency == 'quarterly')
display((hpi_df_quarterly.count(), len(hpi_df_quarterly.columns))) # 117622, 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500

# COMMAND ----------

sp500_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Repos/GitHub/HousingProject/S&P500.csv")
)

# test that dfs loaded
display((sp500_df.count(), len(sp500_df.columns))) # 4784 x 6
display(sp500_df.limit(10))

# COMMAND ----------

# group by quarterly, average
from pyspark.sql.functions import *

quarterly_agg = (
    sp500_df.groupby(year("Time"), quarter("Time"))
    .agg(
        count("*").alias("biz_days"), avg("Close"), max("High"), min("Low")
    )
    .sort([asc("year(Time)"), asc("quarter(Time)")])
)
display(quarterly_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Federal Interest Rate

# COMMAND ----------

# create dataframes as PySpark
fed_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Repos/GitHub/HousingProject/fed_fund_rate.csv")
)

# test that dfs loaded
display((fed_df.count(), len(fed_df.columns))) # 4283 x 11
display(fed_df.limit(10))
