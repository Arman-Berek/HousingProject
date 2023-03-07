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

#spark.conf.set("fs.azure.account.key.housingdatastorage.blob.core.windows.net", "sp=racwdlme&st=2023-03-06T20:26:35Z&se=2023-03-14T03:26:35Z&spr=https&sv=2021-06-08&sr=c&sig=ny075CBun5bW2GX6VA%2FLhG48Z6tyNi%2BrWdvno67tnVo%3D")
#df = spark.read.csv("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv", header="true")
dbutils.fs.ls("wasbs://housingdatastorage@data-post-etl.blob.core.windows.net/joined_df.csv")

# COMMAND ----------

""
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

import seaborn as sns
import matplotlib.pyplot as plt

matrix = df_import.corr().round(2)
sns.heatmap(matrix, annot=True)
plt.show()

# COMMAND ----------


