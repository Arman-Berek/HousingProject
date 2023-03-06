# Databricks notebook source
# MAGIC %md
# MAGIC # Federal Fund Rate - Quarterly
# MAGIC ### FRED Economic Data | St. Louis FED
# MAGIC ###  Federal Funds Effective Rate (FEDFUNDS)
# MAGIC   URL: https://fred.stlouisfed.org/series/FEDFUNDS#0

# COMMAND ----------

# Pakackages
import requests
import pandas as pd
import datetime as dt

# COMMAND ----------

# Variables
current_date = dt.date.today().strftime('%Y-%m-%d')
frequency ="Quarterly"
start_dt = "1975-01-01" # "1954-07-01"
current_month_dt = dt.date.today().replace(day=1) # "2023-02-01"

# url
url_base = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# COMMAND ----------

# Call parameters
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

# Get data frame
response = requests.get(url_base, params=params)
df = pd.read_csv(response.url)

# COMMAND ----------

# Clean Data
# drop column with no data
df = df.loc[~(df["FEDFUNDS"] == ".")]

# COMMAND ----------

display(df.head(5))
display(df.tail(5))

# COMMAND ----------

# Clean Data
# drop rows with no data
df = df.loc[~(df["FEDFUNDS"] == ".")]

# Convert to date and year
df["DATE"] = pd.to_datetime(df["DATE"])

df["Period"] = df["DATE"].dt.quarter
df["Year"] = df["DATE"].dt.year

# drop date
df = df.drop(columns= ["DATE"], errors='ignore')

# Rename column
df = df.rename(columns={"FEDFUNDS" : "fed_interest_rate"})

display(df)
display(df.describe())

# COMMAND ----------


