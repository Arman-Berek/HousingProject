# Databricks notebook source
# imports

from pyspark.sql.functions import *

# Python Packages
import requests
import pandas as pd
import datetime as dt

# COMMAND ----------

# MAGIC %md
# MAGIC ## HPI Master: Extract & Transform

# COMMAND ----------

# create dataframe as PySpark
hpi_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/Workspace/Repos/GitHub/HousingProject/data/HPI_master.csv")
    .withColumnRenamed("yr", "year")
)

# test that dfs loaded
display((hpi_df.count(), len(hpi_df.columns)))  # 121462 rows, 10 columns
display(hpi_df.limit(10))

# COMMAND ----------

# filter to quarterly + State/City level
hpi_quarterly_agg = hpi_df.where(
    (hpi_df.frequency == "quarterly") & (hpi_df.level != "Puerto Rico")
)
display((hpi_quarterly_agg.count(), len(hpi_quarterly_agg.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500: Extract & Transform

# COMMAND ----------

sp500_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "MMM dd, yyyy")
    .load("file:/Workspace/Repos/GitHub/HousingProject/data/S&P500_1975_Present.csv")
)

sp500_df = sp500_df.select(
    [
        c if c == "Time" else regexp_replace(c, ",", "").cast("float").alias(c)
        for c in sp500_df.columns
    ]
)

# test that dfs loaded
display((sp500_df.count(), len(sp500_df.columns)))  # 12147 x 7
display(sp500_df.limit(10))

# COMMAND ----------

# group by quarterly, average
sp500_quarterly_agg = (
    sp500_df.groupby(year("Time"), quarter("Time"))
    .agg(avg("Open"), avg("Close"), max("High"), min("Low"))
    .withColumnRenamed("year(Time)", "Year")
    .withColumnRenamed("quarter(Time)", "Period")
    .sort([asc("Year"), asc("Period")])
)

display(sp500_quarterly_agg.tail(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarterly Federal Interest Rate (FRED St. Louis): Extract & Transform
# MAGIC URL: https://fred.stlouisfed.org/series/FEDFUNDS#0

# COMMAND ----------

# Variables
current_date = dt.date.today().strftime("%Y-%m-%d")
frequency = "Quarterly"
start_dt = "1975-01-01"  # "1954-07-01"
current_month_dt = dt.date.today().replace(day=1)  # "2023-02-01"

url_base = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC Sample CSV URL 
# MAGIC 
# MAGIC https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=FEDFUNDS&scale=left&cosd=1954-07-01&coed=2023-02-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-03-06&revision_date=2023-03-06&nd=1954-07-01

# COMMAND ----------

# Call Parameters
params = {
    "bgcolor": "#e1e9f0",
    "chart_type": "line",
    "drp": "0",
    "fo": "open sans",
    "graph_bgcolor": "#ffffff",
    "height": "450",
    "mode": "fred",
    "recession_bars": "on",
    "txtcolor": "#444444",
    "ts": "12",
    "tts": "12",
    "width": "968",
    "nt": "0",
    "thu": "0",
    "trc": "0",
    "show_legend": "yes",
    "show_axis_titles": "yes",
    "show_tooltip": "yes",
    "id": "FEDFUNDS",
    "scale": "left",
    "cosd": "1975-01-01",
    "coed": current_month_dt,  # "2023-02-01"
    "line_color": "#4572a7",
    "link_values": "false",
    "line_style": "solid",
    "mark_type": " none",
    "mw": "3",
    "lw": "2",
    "ost": "-99999",
    "oet": "99999",
    "mma": "0",
    "fml": "a",
    "fq": frequency,
    "fam": "avg",
    "fgst": "lin",
    "fgsnd": "2020-02-01",
    "line_index": "1",
    "transformation": "lin",
    "vintage_date": current_date,
    "revision_date": current_date,
    "nd": start_dt,
}

# COMMAND ----------

# Requst Call to get url
response = requests.get(url_base, params=params)
fed_fund_url = response.url
print(response)

# COMMAND ----------

# Pandas read url to csv
fed_fund_pandas_df = pd.read_csv(
    fed_fund_url, dtype={"FEDFUNDS": "float64"}, parse_dates=["DATE"], na_values="."
)

# Clean
# drop rows with no data
fed_fund_pandas_df = fed_fund_pandas_df.loc[~(fed_fund_pandas_df["FEDFUNDS"].isna())]

# Extract Period & Year
fed_fund_pandas_df["Period"] = fed_fund_pandas_df["DATE"].dt.quarter
fed_fund_pandas_df["Year"] = fed_fund_pandas_df["DATE"].dt.year

# drop date
fed_fund_pandas_df = fed_fund_pandas_df.drop(columns=["DATE"], errors="ignore")

# Rename column
fed_fund_pandas_df = fed_fund_pandas_df.rename(
    columns={"FEDFUNDS": "fed_interest_rate"}
)

display(fed_fund_pandas_df)
display(fed_fund_pandas_df.describe())

# COMMAND ----------

# convert pandas to PySpark
fed_df2 = spark.createDataFrame(fed_fund_pandas_df)
# final test that df properly went through ET
display((fed_df2.count(), len(fed_df2.columns)))  # 192 x 3
display(fed_df2.tail(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Data Frames on Period and Year

# COMMAND ----------

joined_df = (
    hpi_quarterly_agg
    .join(sp500_quarterly_agg, on=["Year", "Period"])
    .join(fed_df2, on=["Year", "Period"])
)

# test that join was successful
display((joined_df.count(), len(joined_df.columns)))
display(joined_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Storage Container
# MAGIC 
# MAGIC Storage account key is exposed here. In actual production, we'd set up a specific key for the container with Azure Key Vault:
# MAGIC 
# MAGIC https://learn.microsoft.com/en-us/azure/key-vault/secrets/overview-storage-keys

# COMMAND ----------

storage_name = "housingdatastorage"
container_name = "data-post-etl"
sas_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="

# Configure blob storage account access key
spark.conf.set(f"fs.azure.account.key.{storage_name}.blob.core.windows.net", sas_key)

output_blob_folder = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net"

outputs = {"joined_df": joined_df}
guarantee_one_part = True

for fn, df in outputs.items():
    temp_folder = f"{output_blob_folder}/temp"
    
    if guarantee_one_part:
        df = df.coalesce(1)
    
    # write the dataframe as a single file to blob storage
    try:
        (
            df
            .write.mode("overwrite")
            .option("header", "true")
            .format("com.databricks.spark.csv")
            .save(temp_folder)
        )

        # Get the CSV file(s) that were just saved
        output_files = [
            x for x in dbutils.fs.ls(temp_folder) if x.name.startswith("part-")
        ]

        # Move the CSV file(s) from the sub-folder (wrangled_data_folder) to the root of the blob container
        # While finalizing filename(s)
        if len(output_files) == 1:
            dbutils.fs.mv(output_files[0].path, f"{output_blob_folder}/{fn}.csv")
        else:
            for e, f in enumerate(output_files, 1):
                dbutils.fs.mv(f.path, f"{output_blob_folder}/{fn}_part{e:02d}.csv")

        dbutils.fs.rm(temp_folder, True)
        display(f"Successfully wrote {fn} to blob container {container_name}.")
    except Exception as e:
        display(
            f"Operation for {fn} went wrong somewhere. It may still have worked, check {container_name}.\n"
        )
        print(e)
