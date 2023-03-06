# Databricks notebook source
# Packages
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np


plt.style.use('fivethirtyeight')

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

#spark.conf.set("fs.azure.account.key.housingdatastorage.blob.core.windows.net", "sp=racwdlme&st=2023-03-06T20:26:35Z&se=2023-03-14T03:26:35Z&spr=https&sv=2021-06-08&sr=c&sig=ny075CBun5bW2GX6VA%2FLhG48Z6tyNi%2BrWdvno67tnVo%3D")
#df = spark.read.csv("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv", header="true")
dbutils.fs.ls("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv")

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

df = df_import.copy()

# COMMAND ----------

date = pd.to_datetime(df['year'].astype(str) + 'Q' + df['period'].astype(str))
df['date'] = date

# COMMAND ----------

df_state = df.loc[df['level'] == "State"]
set(df_state['place_name'])

# COMMAND ----------

df_state = df_state.loc[df_state['hpi_flavor'] == "all-transactions"]
df_nj = df_state.loc[df_state['place_name'] == "New Jersey"]
df_nj = df_nj.sort_values(['date'])
#df_nj.set_index('date')

# COMMAND ----------

plt.plot(df_nj['date'].values, df_nj['index_nsa'].values, '--')
plt.title("New Jersey")
plt.show()

# COMMAND ----------


