# Databricks notebook source
# create dataframes as pandas
storage_name = "housingdatastorage"
container_name = "data-post-etl"
sas_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="

storage_account_name = "housingdatastorage"
storage_account_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="
container = "data-post-etl"
# Configure blob storage account access key globally
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/housing",
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
)

dbutils.fs.ls("/mnt/housing")



# COMMAND ----------

spark.conf.set("fs.azure.account.key.housingdatastorage.blob.core.windows.net", "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g==")
df = spark.read.csv("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv", header="true")


# COMMAND ----------
