# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring HPI Correlations

# COMMAND ----------

# Packages
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from IPython.display import display, HTML

plt.style.use('fivethirtyeight')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Date

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Directory
# MAGIC ```
# MAGIC # create dataframes as pandas
# MAGIC storage_name = "housingdatastorage"
# MAGIC container_name = "data-post-etl"
# MAGIC sas_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="
# MAGIC 
# MAGIC storage_account_name = "housingdatastorage"
# MAGIC storage_account_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="
# MAGIC container = "data-post-etl"
# MAGIC # Configure blob storage account access key globally
# MAGIC spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC  source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
# MAGIC  mount_point = "/mnt/housing",
# MAGIC  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
# MAGIC )
# MAGIC 
# MAGIC dbutils.fs.ls("/mnt/housing")
# MAGIC ```

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ```
# MAGIC #spark.conf.set("fs.azure.account.key.housingdatastorage.blob.core.windows.net", "sp=racwdlme&st=2023-03-06T20:26:35Z&se=2023-03-14T03:26:35Z&spr=https&sv=2021-06-08&sr=c&sig=ny075CBun5bW2GX6VA%2FLhG48Z6tyNi%2BrWdvno67tnVo%3D")
# MAGIC #df = spark.read.csv("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv", header="true")
# MAGIC dbutils.fs.ls("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv")
# MAGIC ```

# COMMAND ----------

storage_account_name = 'housingdatastorage'
storage_account_access_key = '2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=='
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)


# COMMAND ----------

blob_container = 'data-post-etl'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/joined_df.csv"
dfImportSpark = spark.read.format("csv").load(filePath, inferSchema = True, header = True)


# COMMAND ----------

# Convert to pandas
display(dfImportSpark)
df_import = dfImportSpark.toPandas()
display(df_import.tail(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

#rename_map = {'index_nsa' : 'NonSeas HPI', 'avg(Close)' :  'S&P 500', 'fed_interest_rate' : 'Federal Interest Rate'}

rename_map = {'index_nsa' :  'HPI NonSeas', 'index_sa' : 'HPI Seas', 'avg(Close)' : 'S&P 500', "max(High)" : "S&P High", "min(Low)" : "S&P Low",  'fed_interest_rate' : 'Fed Interest Rate'}

# COMMAND ----------

# Variable
# {'all-transactions', 'expanded-data', 'purchase-only'}
hpi_flavor = "purchase-only" # 
hpi_type = "traditional"
start_dt_purch_only = "1991-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subet Data

# COMMAND ----------

df = df_import.copy()

# COMMAND ----------

date = pd.to_datetime(df['year'].astype(str) + 'Q' + df['period'].astype(str))
df['date'] = date
df = df.sort_values(['date'])
#df = df.set_index('date')

# COMMAND ----------

print("level:")
print(set(df["level"]))

print("\nhpi_flavor:")
print(set(df["hpi_flavor"]))

print("\nhpi_type:")
print(set(df["hpi_type"]))

# COMMAND ----------

# Get S&P 500
df_rates = df.loc[:, ['avg(Close)', 'max(High)', 'min(Low)', 'fed_interest_rate', 'date']].sort_values(['date'])
df_rates = df_rates.drop_duplicates(keep='first').set_index('date')
df_rates = df_rates.rename(columns=rename_map)
df_rates.tail(10)

df_rates_1991 = df_rates.loc[df_rates.index >= '1991-01-01']

# COMMAND ----------

# MAGIC %md
# MAGIC ### United States HPI

# COMMAND ----------

# Subset on USA Data Only
#df = df.loc[df['hpi_flavor'] == "all-transactions"]
df_usa_all = df.loc[(df['level'] == "USA or Census Division") & (df['place_name']=="United States")]
df_usa_all = df_usa_all.sort_values(['date'])
df_usa_all = df_usa_all.set_index('date')
display(df_usa_all)
display(df_usa_all.tail())
print(df_usa_all.shape)

# COMMAND ----------

df_usa_all_traditional = df_usa_all.loc[df_usa_all['hpi_type'] == 'traditional']
print(df_usa_all_traditional.shape)

# COMMAND ----------

df_usa_all_trans  = df_usa_all.loc[df_usa_all['hpi_flavor'] == 'all-transactions'] # does not have seasonal Adjusted
df_usa_exp_data   = df_usa_all.loc[df_usa_all['hpi_flavor'] == 'expanded-data']
df_usa_purch_only = df_usa_all.loc[df_usa_all['hpi_flavor'] == 'purchase-only']

# COMMAND ----------

colors = ["#30a2da", "#6d904f", '#8b8b8b', '#fc4f30', "#e5ae38"]


plt.figure(figsize=(8, 10))
plt.suptitle(' Time Series National HPI ', fontsize=24)
#plt.title('National HPI Comparison with Outside Factors')
# Daily Plot
plt.subplot(3,1, 1)
#plt.figure(figsize= ts_fig_size, dpi=80)
plt.title('HPI', fontsize=14)
# All Transactions
plt.plot(df_usa_all_trans['index_nsa'], '--', linewidth= 3, markersize=2, label = 'All Transactions (NonSeas)')

# Purchase Only
plt.plot(df_usa_purch_only['index_nsa'], '--', linewidth= 4, markersize=2, label= 'Purch Only (NonSeas)')
plt.plot(df_usa_purch_only['index_sa'], '-', linewidth= 3, markersize=2, label= 'Purch Only (Seas)')

# Expanded Data
plt.plot(df_usa_exp_data['index_nsa'], '--', linewidth= 2, markersize=2, label= 'Expanded (NonSeas)')
plt.plot(df_usa_exp_data['index_sa'], '-', linewidth= 2, markersize=2, label= 'Expanded (Seas)')

# Format
plt.xticks(rotation=0, ha='right')
#plt.xticks([])
plt.ylabel('HPI')
plt.legend()


# Fed Interest Rate
plt.subplot(3,1, 2)
plt.plot(df_rates['Fed Interest Rate'], '-', linewidth= 3, markersize=2, color="#30a2da", label="Fed Interest Rate")
plt.legend()
plt.title('Federal Interest Rate', fontsize=14)
plt.ylabel('% Points')


# Stock Market
plt.subplot(3,1, 3)
plt.plot(df_rates["S&P 500"], '-', linewidth= 3, markersize=2, color="#fc4f30", label="S&P 500")
plt.plot(df_rates["S&P High"], '--', linewidth= 2, markersize=2, color="#6d904f", label="S&P High")
plt.plot(df_rates["S&P Low"], '--', linewidth= 2, markersize=2, color="#e5ae38", label="S&P Low")
plt.legend()
plt.title('S&P 500', fontsize=14)
plt.xticks(rotation=0, ha='right')
plt.xlabel('Quarterly')
plt.ylabel('Stock Market Points')

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Traditional Purchase Only Data

# COMMAND ----------

plt.figure(figsize=(8, 10))
plt.suptitle(' Time Series Plot Purchase Only HPI ', fontsize=24)
#plt.title('National HPI Comparison with Outside Factors')
# Daily Plot
plt.subplot(3,1, 1)
#plt.figure(figsize= ts_fig_size, dpi=80)
plt.title('HPI', fontsize=14)

# Purchase Only
plt.plot(df_usa_purch_only['index_nsa'], '--', linewidth= 4, markersize=2, label= 'Purch Only (NonSeas)')
plt.plot(df_usa_purch_only['index_sa'], '-', linewidth= 3, markersize=2, label= 'Purch Only (Seas)')

# Format
plt.xticks(rotation=0, ha='right')
plt.ylabel('HPI')
plt.legend()


# Fed Interest Rate
plt.subplot(3,1, 2)
plt.plot(df_rates_1991['Fed Interest Rate'], '-', linewidth= 3, markersize=2, color="#30a2da", label="Fed Interest Rate")
plt.legend()
plt.title('Federal Interest Rate', fontsize=14)
plt.ylabel('% Points')


# Stock Market
plt.subplot(3,1, 3)
plt.plot(df_rates_1991["S&P 500"], '-', linewidth= 3, markersize=2, color="#fc4f30", label="S&P 500")
plt.plot(df_rates_1991["S&P High"], '--', linewidth= 2, markersize=2, color="#6d904f", label="S&P High")
plt.plot(df_rates_1991["S&P Low"], '--', linewidth= 2, markersize=2, color="#e5ae38", label="S&P Low")
plt.legend()
plt.title('S&P 500', fontsize=14)
plt.xticks(rotation=0, ha='right')
plt.xlabel('Quarterly')
plt.ylabel('Stock Market Points')

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correlation of Line plot data

# COMMAND ----------

# MAGIC %md
# MAGIC #### All Transactions (1975-Present)

# COMMAND ----------

df_usa_all_trans_corr = df_usa_all_trans.loc[:, ['index_nsa', 'avg(Close)',	'max(High)', 'min(Low)', 'fed_interest_rate']].rename(columns=rename_map).corr().round(2)
sns.heatmap(df_usa_all_trans_corr, annot=True)
plt.xticks(rotation=45, ha='right')
plt.title("HPI All Transactions (1975-Present)")
plt.show()


print("Correlation Between HPI and S&P 500: {}".format(round(df_usa_all_trans['index_nsa'].corr(df_usa_all_trans["avg(Close)"]), 3)))
print("Correlation Between HPI and Federal Interest Rate: {}".format(round(df_usa_all_trans['index_nsa'].corr(df_usa_all_trans["fed_interest_rate"]), 3)))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Purchase Only Correlations 1991 to present

# COMMAND ----------

df_usa_purch_corr = df_usa_purch_only.loc[:, ['index_sa', 'index_nsa', 'avg(Close)',	'max(High)', 'min(Low)', 'fed_interest_rate']].rename(columns=rename_map).corr().round(2)
sns.heatmap(df_usa_purch_corr, annot=True)
plt.xticks(rotation=45, ha='right')
plt.title("HPI Purchase Only (1991-Present)")
plt.show()


print("Correlation Between HPI and S&P 500: {}".format(round(df_usa_purch_only['index_nsa'].corr(df_usa_purch_only["avg(Close)"]), 3)))
print("Correlation Between HPI and Federal Interest Rate: {}".format(round(df_usa_purch_only['index_nsa'].corr(df_usa_purch_only["fed_interest_rate"]), 3)))

# COMMAND ----------

df_usa_purch_corr = df_usa_purch_only.loc[(df_usa_purch_only.index >= '2000-01-01'), ['index_sa', 'index_nsa', 'avg(Close)',	'max(High)', 'min(Low)', 'fed_interest_rate']].rename(columns=rename_map).corr().round(2)
sns.heatmap(df_usa_purch_corr, annot=True)
plt.xticks(rotation=45, ha='right')
plt.title("HPI Purchase Only (2000-Present)")
plt.show()


# COMMAND ----------

df_usa_purch_corr = df_usa_purch_only.loc[(df_usa_purch_only.index >= '2009-01-01'), ['index_sa', 'index_nsa', 'avg(Close)',	'max(High)', 'min(Low)', 'fed_interest_rate']].rename(columns=rename_map).corr().round(2)
sns.heatmap(df_usa_purch_corr, annot=True)
plt.xticks(rotation=45, ha='right')
plt.title("HPI Purchase Only (2009-Present)")
plt.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Correlations-Locations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Regions

# COMMAND ----------

# Variable
# {'all-transactions', 'expanded-data', 'purchase-only'}
hpi_flavor = "purchase-only" # 
hpi_type = "traditional"

# COMMAND ----------

df_usa = df.loc[(df['hpi_flavor'] == hpi_flavor) & (df['hpi_type'] == hpi_type) & (df['level'] == "USA or Census Division") & (df['place_name'] == "United States")]
df_usa = df_usa.set_index('date')
df_usa.head(5)

# COMMAND ----------

df_usa_all = df.loc[(df['hpi_flavor'] == hpi_flavor) & (df['level'] == "USA or Census Division")]
df_usa_all = df_usa_all.set_index(df_usa_all['date'])
regions = set(df_usa_all["place_name"])
print(regions)


# COMMAND ----------

region_df_list = []
for region in regions:
    region_df_list.append(df_usa_all.loc[df_usa_all['place_name'] == region, ['index_nsa']])
    
df_regions = pd.concat(region_df_list, axis=1)
df_regions.columns = list(regions)
df_regions
df_regions = pd.concat([df_regions, df_rates.iloc[:, :-1]], axis=1)
df_regions = df_regions.loc[df_regions.index >= start_dt_purch_only]

# COMMAND ----------

df_regions_only = df_regions.iloc[:, :-3]

plt.figure(figsize=(10, 5))
plt.plot(df_regions_only.iloc[:, 0:6], '-', label=df_regions_only.columns[0:6])
plt.plot(df_regions_only.iloc[:, 6:], '--', label=df_regions_only.columns[6:])
plt.legend()
plt.xlabel('Quarterly')
plt.ylabel('HPI')
plt.title('Nonseasonal HPI by Region')
plt.show()

# COMMAND ----------

df_regions.corr()
sns.heatmap(df_regions.corr().round(2), annot=False)
plt.xticks(rotation=45, ha='right')
plt.show()


pd.concat([df_regions_only, df_rates_1991.iloc[:, -1]], axis=1).corr().round(2).iloc[:, -1].to_frame()

# COMMAND ----------

# MAGIC %md
# MAGIC #### States

# COMMAND ----------



df_states = df.loc[(df['hpi_flavor'] == hpi_flavor) & (df['level'] == "State") & (df['hpi_type']== hpi_type)]
df_states = df_states.set_index(df_states['date'])
states = set(df_states["place_name"])
print(states)


states_df_list = []
states_df_list.append(df_usa.loc[:, ['index_nsa']])
for state in states:
    states_df_list.append(df_states.loc[df_states['place_name'] == state, ['index_nsa']])
    
df_all_states = pd.concat(states_df_list, axis=1)
df_all_states.columns = ['USA'] + list(states)

# COMMAND ----------

df_all_states.corr().head()
sns.heatmap(df_all_states.corr())
plt.show()

# COMMAND ----------

state_corr = df_all_states.corr().round(3)['USA'].sort_values(ascending=False)
state_corr.name = "Correlation With National HPI"
state_corr = state_corr.to_frame()
print("Most Correlated")
display(state_corr.head(10+1)[1:])
print('\n')
print("Least Correlated")
display(state_corr.tail(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cities

# COMMAND ----------

df_msa = df.loc[(df['hpi_flavor'] == hpi_flavor) & (df['level'] == "MSA") & (df['hpi_type']==hpi_type)]
df_msa = df_msa.set_index(df_msa['date'])
msas = set(df_msa["place_name"])
print(len(msas))


msa_df_list = []
msa_df_list.append(df_usa.loc[:, ['index_nsa']])
for msa in msas:
    msa_df_list.append(df_msa.loc[df_msa['place_name'] == msa, ['index_nsa']])
    
df_msa_all = pd.concat(msa_df_list, axis=1)
df_msa_all.columns = ['USA'] + list(msas)

# COMMAND ----------

msa_usa_corr = df_msa_all.dropna().corr()['USA'].sort_values(ascending=False).round(3)
msa_usa_corr.name = "Corr with National HPI"
msa_usa_corr = msa_usa_corr.to_frame()

# COMMAND ----------

print("Most Correlated:")
msa_usa_corr.head(10+1)[1:]

# COMMAND ----------

print("Least Correlated")
msa_usa_corr.tail(10)

# COMMAND ----------


