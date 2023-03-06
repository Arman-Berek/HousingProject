# Databricks notebook source
# imports

from pyspark.sql.functions import *

# COMMAND ----------

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
display((hpi_df.count(), len(hpi_df.columns))) # 121462 rows, 10 columns
display(hpi_df.limit(10))

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
    .option("dateFormat", "MMM dd, yyyy")
    .load("file:/Workspace/Repos/GitHub/HousingProject/S&P500_1975_Present.csv")
)

sp500_df = sp500_df.select([c if c == 'Time' else regexp_replace(c, ',', '').cast('float').alias(c) for c in sp500_df.columns])

# test that dfs loaded
display((sp500_df.count(), len(sp500_df.columns))) # 12147 x 7
display(sp500_df.limit(10))

# COMMAND ----------

# group by quarterly, average
quarterly_agg = (
    sp500_df.groupby(year("Time"), quarter("Time"))
    .agg(
        count("*").alias("biz_days"), avg('Open'), avg("Close"), max("High"), min("Low")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Storage Container

# COMMAND ----------

storage_name = "housingdatastorage"
container_name = "data-post-etl"
sas_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="

# Configure blob storage account access key globally
spark.conf.set(f"fs.azure.account.key.{storage_name}.blob.core.windows.net", sas_key)

output_blob_folder = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net"

outputs = {"sp500_quarterly_agg": quarterly_agg}

for fn, df in outputs.items():
    temp_folder = f"{output_blob_folder}/temp"
    # write the dataframe as a single file to blob storage
    try:
        (
            df.write.mode("overwrite")
            .option("header", "true")
            .format("com.databricks.spark.csv")
            .save(temp_folder)
        )

        # Get the CSV file(s) that was just saved
        output_files = [
            x for x in dbutils.fs.ls(temp_folder) if x.name.startswith("part-")
        ]

        # Move the CSV file(s) from the sub-folder (wrangled_data_folder) to the root of the blob container
        # While finalizing filename(s)
        if len(output_files) == 1:
            dbutils.fs.mv(
                output_files[0].path, f"{output_blob_folder}/sp500_quarterly_agg.csv"
            )
        else:
            for e, f in enumerate(output_files, 1):
                dbutils.fs.mv(
                    f.path, f"{output_blob_folder}/sp500_quarterly_agg_part{e:02d}.csv"
                )

        dbutils.fs.rm(temp_folder, True)
        display(f"Successfully wrote {fn} to blob container {container_name}.")
    except Exception as e:
        display(
            f"Operation for {fn} went wrong somewhere. It may still have worked, check {container_name}.\n"
        )
        print(e)
