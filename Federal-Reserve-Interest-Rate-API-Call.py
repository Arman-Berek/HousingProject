# Databricks notebook source
# MAGIC %md
# MAGIC # Federal Reserve Interest Rate
# MAGIC ## Federal Fund Rate
# MAGIC https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/average-interest-rates-on-u-s-treasury-securities

# COMMAND ----------

# Packages
import pandas as pd
import requests

# COMMAND ----------

# Variables
limit_size = 10000
treasury_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates"


# COMMAND ----------

# Request Call
params ={
    "limit" : limit_size
}

response = requests.get(treasury_url, params=params)

# COMMAND ----------

call_data = response.json()
data = call_data['data']
treasury_interest_rates = pd.DataFrame(data)
treasury_interest_rates

# COMMAND ----------

# Clean Dataa
treasury_interest_rates["record_date"] = pd.to_datetime(treasury_interest_rates["record_date"])

# COMMAND ----------

# Save to csv
treasury_interest_rates.to_csv("fed_fund_rate.csv", index= False)

# COMMAND ----------

