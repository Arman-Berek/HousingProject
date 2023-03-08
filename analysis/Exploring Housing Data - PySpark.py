# Databricks notebook source
# imports

from pyspark.sql.functions import *

# Python Packages
import requests
import pandas as pd
import datetime as dt

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
    .withColumnRenamed('yr', 'year')
)

# test that dfs loaded
display((hpi_df.count(), len(hpi_df.columns))) # 121462 rows, 10 columns
display(hpi_df.limit(10))


# COMMAND ----------


# filter to quarterly  and State/ City level
hpi_quarterly_agg = hpi_df.where((hpi_df.frequency == 'quarterly') & ((hpi_df.level == 'State') | (hpi_df.level == 'MSA')))
display(hpi_quarterly_agg)


# COMMAND ----------

# MAGIC %md
# MAGIC Change to bypass csv
# MAGIC ```
# MAGIC # Import csv from url
# MAGIC url_hpi = "https://www.fhfa.gov/HPI_master.csv"
# MAGIC df_hpi = pd.read_csv(url_hpi)
# MAGIC df_hpi = df_hpi.rename(columns={'yr': 'year'})
# MAGIC df_hpi = df_hpi.loc[((df.frequency == 'quarterly') & ((df_hpi.level == "State") | (df_hpi.level == 'MSA')))]
# MAGIC 
# MAGIC 
# MAGIC # create dataframes as PySpark
# MAGIC hpi_quarterly_agg = spark.createDataFrame(df_hpi)
# MAGIC # test that dfs loaded
# MAGIC display((hpi_quarterly_agg.count(), len(hpi_quarterly_agg.columns))) # 192 x 4
# MAGIC display(hpi_quarterly_agg.tail(5))
# MAGIC ```

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
sp500_quarterly_agg = (
    sp500_df.groupby(year("Time"), quarter("Time"))
    .agg(
       avg('Open'), avg("Close"), max("High"), min("Low")
    )
    .withColumnRenamed("year(Time)","Year").withColumnRenamed("quarter(Time)","Period")
    .sort([asc("Year"), asc("Period")])
)

display(sp500_quarterly_agg.tail(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Change
# MAGIC Able to pull in the SNP 500 bypassing the csv from NASDAQ

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC def get_snp500_quarterly():
# MAGIC     '''
# MAGIC     Get S&P 500 Quarterly from NASDAQ API at close
# MAGIC     https://data.nasdaq.com/data/MULTPL/SP500_REAL_PRICE_MONTH-sp-500-real-price-by-month
# MAGIC     
# MAGIC 
# MAGIC     Returns
# MAGIC     -------
# MAGIC     df_sp_quarterly : pandas.DataFrame
# MAGIC         dataframe .
# MAGIC 
# MAGIC     '''
# MAGIC     # https://data.nasdaq.com/data/MULTPL/SP500_REAL_PRICE_MONTH-sp-500-real-price-by-month
# MAGIC 
# MAGIC     # NASDAQ python call
# MAGIC     #quandl.get("MULTPL/SP500_REAL_PRICE_MONTH", authtoken="fJueoQJtK4zU-raf1asE")
# MAGIC 
# MAGIC     # CSV
# MAGIC     # url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_REAL_PRICE_MONTH.csv?api_key=fJueoQJtK4zU-raf1asE"
# MAGIC 
# MAGIC     dt_start = '1975-01-01'
# MAGIC     api_key = "fJueoQJtK4zU-raf1asE"
# MAGIC     url_base = "https://data.nasdaq.com/api/v3/datasets/MULTPL"
# MAGIC     stock = "SP500_REAL_PRICE_MONTH"
# MAGIC     
# MAGIC     params = {
# MAGIC         "api_key" : api_key
# MAGIC         }
# MAGIC     
# MAGIC     # url
# MAGIC     api_url = "{}/{}.csv".format(url_base, stock)
# MAGIC     
# MAGIC     # Get Request
# MAGIC     response = requests.get(api_url, params=params)
# MAGIC     print(response)
# MAGIC     url = response.url
# MAGIC     df = pd.read_csv(url)
# MAGIC     
# MAGIC     ## CLEAN
# MAGIC     # convert to datetime
# MAGIC     df['Date'] = pd.to_datetime(df['Date'])
# MAGIC     
# MAGIC     # filter df
# MAGIC     df = df.loc[df['Date'] >= dt_start]
# MAGIC     df = df.sort_values(['Date'], ascending=True)
# MAGIC     
# MAGIC     # add quarters
# MAGIC     df['Period'] = df['Date'].dt.quarter
# MAGIC     df['Year'] = df['Date'].dt.year
# MAGIC     
# MAGIC     # Group by quarters
# MAGIC     df_sp_quarterly = df.loc[:, ['Period', 'Year', 'Value']].groupby(['Period', 'Year']).mean().reset_index()
# MAGIC     df_sp_quarterly = df_sp_quarterly.sort_values(['Year', 'Period'], ascending=True).reset_index(drop=True)
# MAGIC     
# MAGIC     
# MAGIC     df_sp_quarterly = df_sp_quarterly.rename(columns={'Value' : 'SNP500_Value'})
# MAGIC     
# MAGIC     return df_sp_quarterly
# MAGIC 
# MAGIC 
# MAGIC df_snp_quarterly_pandas = get_snp500_quarterly()
# MAGIC # create dataframes as PySpark
# MAGIC sp500_quarterly_agg = spark.createDataFrame(df_snp_quarterly_pandas)
# MAGIC # test that dfs loaded
# MAGIC display((sp500_quarterly_agg.count(), len(sp500_quarterly_agg.columns))) # 192 x 4
# MAGIC display(sp500_quarterly_agg.tail(5))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quaterly Fed Interest Rate (FRED St. Louis)
# MAGIC URL: https://fred.stlouisfed.org/series/FEDFUNDS#0

# COMMAND ----------

import requests
import datetime as dt
import pandas as pd

# COMMAND ----------

# Variables
current_date = dt.date.today().strftime('%Y-%m-%d')
frequency ="Quarterly"
start_dt = "1975-01-01" # "1954-07-01"
current_month_dt = dt.date.today().replace(day=1) # "2023-02-01"

url_base = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC Sample CSV URL 
# MAGIC 
# MAGIC https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=968&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=FEDFUNDS&scale=left&cosd=1954-07-01&coed=2023-02-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-03-06&revision_date=2023-03-06&nd=1954-07-01

# COMMAND ----------

# Call Parameters
params = {
    "bgcolor" : "#e1e9f0",
    "chart_type" : "line",
    "drp" : "0",
    "fo" : "open sans",
    "graph_bgcolor" : "#ffffff",
    "height" : "450",
    "mode":"fred",
    "recession_bars":"on",
    "txtcolor":"#444444",
    "ts":"12",
    "tts":"12",
    "width":"968",
    "nt":"0",
    "thu":"0",
    "trc":"0",
    "show_legend":"yes",
    "show_axis_titles":"yes",
    "show_tooltip":"yes",
    "id":"FEDFUNDS",
    "scale" : "left",
    "cosd" : "1975-01-01",
    "coed" : current_month_dt, # "2023-02-01"
    "line_color" : "#4572a7",
    "link_values" : "false",
    "line_style" : "solid",
    "mark_type" :" none",
    "mw" : "3",
    "lw" : "2",
    "ost" : "-99999",
    "oet" : "99999",
    "mma" : "0",
    "fml" : "a",
    "fq" :  frequency,
    "fam" : "avg",
    "fgst" : "lin",
    "fgsnd" : "2020-02-01",
    "line_index" : "1",
    "transformation" : "lin",
    "vintage_date" : current_date,
    "revision_date" : current_date,
    "nd" : start_dt
    }


# COMMAND ----------

# Requst Call to get url
response = requests.get(url_base, params=params)
fed_fund_url = response.url
print(response)

# COMMAND ----------

# Pandas read url to csv
fed_fund_pandas_df= pd.read_csv(fed_fund_url)


# Clean
# drop rows with no data
fed_fund_pandas_df = fed_fund_pandas_df.loc[~(fed_fund_pandas_df["FEDFUNDS"] == ".")]

# Convert to date and year
fed_fund_pandas_df["DATE"] = pd.to_datetime(fed_fund_pandas_df["DATE"])

fed_fund_pandas_df["Period"] = fed_fund_pandas_df["DATE"].dt.quarter
fed_fund_pandas_df["Year"] = fed_fund_pandas_df["DATE"].dt.year

# drop date
fed_fund_pandas_df = fed_fund_pandas_df.drop(columns= ["DATE"], errors='ignore')

# Rename column
fed_fund_pandas_df = fed_fund_pandas_df.rename(columns={"FEDFUNDS" : "fed_interest_rate"})

display(fed_fund_pandas_df)
display(fed_fund_pandas_df.describe())

# COMMAND ----------

# create dataframes as PySpark
fed_df2 = spark.createDataFrame(fed_fund_pandas_df)
# test that dfs loaded
display((fed_df2.count(), len(fed_df2.columns))) # 192 x 4
display(fed_df2.tail(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Data Frames on Period and Year

# COMMAND ----------

joined_df = (
    hpi_quarterly_agg
    .join(sp500_quarterly_agg, on=['Year', 'Period'])
    .join(fed_df2, on=['Year', 'Period'])
)

# test that join was successful
display((joined_df.count(), len(joined_df.columns)))
display(joined_df.limit(10))

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

outputs = {"joined_df": joined_df}

for fn, df in outputs.items():
    temp_folder = f"{output_blob_folder}/temp"
    # write the dataframe as a single file to blob storage
    try:
        (
            df
            .coalesce(1)
            .write.mode("overwrite")
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

# COMMAND ----------


