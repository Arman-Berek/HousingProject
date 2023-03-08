# Databricks notebook source
# MAGIC %md
# MAGIC ## Load from Blob Storage, Set Up Spark SQL Instance

# COMMAND ----------

from pyspark.sql import SparkSession

storage_name = "housingdatastorage"
container_name = "data-post-etl"
sas_key = "2FNPZ9FWi4UZ5xJHcmd9J7bh4V2WUG0FIEgqjlZh0ykMy4DOV3s4raCri1HhEjjvD1jnkDx8Wlha+ASttpMp2g=="

# Configure blob storage account access key
spark.conf.set(f"fs.azure.account.key.{storage_name}.blob.core.windows.net", sas_key)

output_blob_folder = f"wasbs://{container_name}@{storage_name}.blob.core.windows.net"
input_file = 'joined_df.csv'

spark = SparkSession.builder.getOrCreate()
df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true").load(f'{output_blob_folder}/{input_file}'))

(df
     .withColumnRenamed('year', 'yr')
     .withColumnRenamed('avg(Open)', 'avg_open')
     .withColumnRenamed('avg(Close)', 'avg_close')
     .withColumnRenamed('max(High)', 'max_high')
     .withColumnRenamed('min(Low)', 'min_low')
      .createOrReplaceTempView("housing")
     )
display(spark.sql("""DESC housing"""))
display(spark.sql("""SELECT COUNT(*) FROM housing"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Queries

# COMMAND ----------

#functions created by Gavin
def compare_two_year_pcts(start_year, end_year, period=4):
    return f"""
SELECT place_name, pct_diff as pct_diff_{start_year}_{end_year}_{period} FROM
    (SELECT yr, place_name, 100*(LEAD(index_nsa) OVER (PARTITION BY place_name ORDER BY yr) - index_nsa)/index_nsa as pct_diff
    FROM housing
    WHERE period = {period} AND level = 'State' AND (yr = {start_year} OR yr = {end_year}) AND hpi_flavor = 'purchase-only')
WHERE pct_diff IS NOT NULL;
"""
  
def compare_consec_year_pcts(start_year, end_year, period=4):
        return f"""
SELECT place_name, yr, pct_diff as pct_diff_to_prior_year FROM
    (SELECT yr, place_name, 100*(index_nsa - prev_nsa) / prev_nsa as pct_diff
    FROM
        (SELECT yr, place_name, index_nsa, LEAD(index_nsa, -1) OVER (PARTITION BY place_name ORDER BY yr) AS prev_nsa
    FROM housing
    WHERE period = {period} AND level = 'State' AND yr BETWEEN {start_year} AND {end_year} AND hpi_flavor = 'purchase-only'));
"""

# COMMAND ----------

q = spark.sql(compare_consec_year_pcts(2005, 2008))
display(q)

# COMMAND ----------

# Katie
# Showing period 4 index_nsa per state for 2008. 
query = spark.sql("""
SELECT yr, period, place_name, index_nsa
FROM housing
WHERE period = '4' AND level = 'State' AND yr = '2008' AND hpi_flavor = 'purchase-only'
GROUP BY yr, period, place_name, index_nsa
ORDER BY index_nsa;
""") 
display(query)

# COMMAND ----------

# Katie
# Showing period 4 index_nsa per state for 2009. 
query = spark.sql("""
SELECT yr, period, place_name, index_nsa,
FROM housing
WHERE period = '4' AND level = 'State' AND yr = '2009' AND hpi_flavor = 'purchase-only'
ORDER BY index_nsa;
""") 
display(query)

# COMMAND ----------

# Katie
# Showing period 4 index_nsa per state for 2008 and 2010. 
query = spark.sql("""
SELECT yr, period, place_name, index_nsa
FROM housing
WHERE period = '4' AND level = 'State' AND (yr = '2008' OR yr = '2010') AND hpi_flavor = 'purchase-only'
GROUP BY place_name, yr, period, index_nsa
ORDER BY place_name, yr;
""") 
display(query)

# COMMAND ----------

# Katie
# Showing period 4 index_sa per state for 2008 and 2010. 
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE period = '4' AND level = 'State' AND (yr = '2008' OR yr = '2010') AND hpi_flavor = 'purchase-only' AND index_sa IS NOT NULL
GROUP BY place_name, yr, period, index_sa
ORDER BY place_name, yr;
""") 
display(query)

# COMMAND ----------

# Showing period 4 index_sa per state for 2008 and 2010. 
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE period = '4' AND level = 'State' AND (yr = '2008' OR yr = '2010') AND hpi_flavor = 'purchase-only' AND index_sa IS NOT NULL
GROUP BY place_name, yr, period, index_sa
ORDER BY index_sa, yr;
""") 
display(query)

# COMMAND ----------

# Showing period 4 index_sa per state for 2008 and 2010. 
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE period = '4' AND level = 'State' AND (yr = '2008' OR yr = '2010') AND hpi_flavor = 'purchase-only' AND index_sa IS NOT NULL
GROUP BY place_name, yr, period, index_sa
ORDER BY index_sa, yr;
""") 
display(query)

# COMMAND ----------

# Katie
# Showing period 4 index_sa per state for 2008 and 2010. 
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE period = '4' AND level = 'State' AND (yr = '2008' OR yr = '2010') AND hpi_flavor = 'purchase-only' AND index_sa IS NOT NULL
GROUP BY place_name, yr, period, index_sa
ORDER BY index_sa, yr;
""") 
display(query)

# COMMAND ----------

q = spark.sql(compare_consec_year_pcts(2005, 2013))
display(q)

# COMMAND ----------

# Katie
# Percent difference 2008 and 2010 using a function (made by Gavin).
query = spark.sql(compare_two_year_pcts(2008, 2010)) 
display(query)

# COMMAND ----------

# Katie
# Seasonaly Adjusted HPI from 2000 to 2022.
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE index_sa IS NOT NULL AND level = 'State' AND hpi_flavor = 'purchase-only' AND period = '4' AND yr BETWEEN 2000 and 2022
""") 
# Fed interest rate 2000-2022
query2 = spark.sql("""
SELECT yr, fed_interest_rate
FROM housing
WHERE period = '4' AND level = 'State' AND hpi_flavor = 'purchase-only' AND yr BETWEEN 2000 and 2022
""") 
display(query)
display(query2)

# COMMAND ----------

# Comparing seasonally adjusted index between the lowest (Arizona) and highest (North Dakota) states.
query = spark.sql("""
SELECT yr, period, place_name, index_sa
FROM housing
WHERE index_sa IS NOT NULL AND level = 'State' AND hpi_flavor = 'purchase-only' AND period = '4' AND yr BETWEEN 2000 and 2022 AND (place_name = 'Arizona' OR place_name = 'North Dakota')
""") 
display (query)

# COMMAND ----------

# Gavin

def compare_consec_year_pcts_test(start_year, end_year, period=4):
    return f"""
SELECT place_name, yr, pct_diff as pct_diff_to_prior_year FROM
    (SELECT yr, place_name, 100*(index_nsa - prev_nsa) / prev_nsa as pct_diff
    FROM
    (SELECT yr, place_name, index_nsa, LEAD(index_nsa, -1) OVER (PARTITION BY place_name ORDER BY yr) AS prev_nsa
    FROM housing
    WHERE period = {period} AND level = 'State' AND yr BETWEEN {start_year} AND {end_year} AND hpi_flavor = 'purchase-only'));
"""

query = spark.sql(compare_consec_year_pcts(2005, 2006)) 
display(query)

# COMMAND ----------

qu = spark.sql("""SELECT yr, period, place_name, index_nsa FROM HOUSING where period = 4 and level ='State' and yr BETWEEN 2005 and 2006 AND hpi_flavor = 'purchase-only' ORDER BY place_name, yr""")
display(qu)

# COMMAND ----------

# Showing period 4 index_nsa per state for 2008 and 2010. Gavin
query = spark.sql("""
SELECT yr, place_name, 100*(LEAD(index_nsa) OVER (PARTITION BY place_name ORDER BY yr) - index_nsa)/index_nsa as pct_diff
    FROM housing
    WHERE period = 4 AND level = 'State' AND (yr = 2020 OR yr = 2021) AND hpi_flavor = 'purchase-only'
""") 
display(query)

# COMMAND ----------

# CHRISTINE
testh = spark.sql(""" 
SELECT index_sa, place_name,yr, period
FROM housing 
WHERE period = '4' AND level = 'State' and (yr = '2019' or yr ='2021') and index_sa IS NOT NULL
order by yr desc
""")

display (testh)

# COMMAND ----------

#CHRISTINE
query = spark.sql(compare_two_year_pcts(2007, 2008)) 
display(query)


# COMMAND ----------

#CHRISTINE
query = spark.sql(compare_two_year_pcts(2019, 2021)) 
display(query)


# COMMAND ----------

#christine
q = spark.sql(compare_consec_year_pcts(2005, 2009))
display(q)

# COMMAND ----------

#christine
q = spark.sql(compare_consec_year_pcts(2017, 2022))
display(q)

# COMMAND ----------

testh = spark.sql("""
SELECT place_name, yr, pct_diff as pct_diff_to_prior_year FROM
    (SELECT yr, place_name, 100*(index_nsa - prev_nsa) / prev_nsa as pct_diff
    FROM
        (SELECT yr, place_name, index_nsa, LEAD(index_nsa, -1) OVER (PARTITION BY place_name ORDER BY yr) AS prev_nsa
    FROM housing
    WHERE (place_name = 'Utah' OR place_name = 'Idaho' OR place_name = 'Arizona' or place_name = 'Montana' OR place_name = 'Florida' OR place_name = 'District of Columbia' OR place_name = 'Lousiana' OR place_name = 'North Dakota' OR place_name = 'West Virginia' OR place_name = 'Alaska' OR place_name = '' ) and  period = 4 AND level = 'State' AND yr BETWEEN 2017 AND 2022 AND hpi_flavor = 'purchase-only'));
""")
display (testh)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Storage Container
# MAGIC 
# MAGIC Storage account key is exposed here. In actual production, we'd set up a specific key for the container with Azure Key Vault: https://learn.microsoft.com/en-us/azure/key-vault/secrets/overview-storage-keys

# COMMAND ----------

outputs = {}
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
