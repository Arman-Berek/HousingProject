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

df_importimport seaborn as sns
import matplotlib.pyplot as plt

matrix = df_import.corr().round(2)
sns.heatmap(matrix, annot=True)
plt.show()

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import PolynomialFeatures
from sklearn import linear_model
import numpy as np

df_import = df_import.rename(columns={'avg(Close)':'Close'})
df_import = df_import[df_import['hpi_flavor'].str.contains('all-transactions')]
df_import = df_import[df_import['level']  == 'USA or Census Division']


#X = df_import[['Close', 'fed_interest_rate','year']]
X = df_import[['Close', 'fed_interest_rate','year']].values
y = df_import['index_nsa'].values


x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=.3, random_state=0)




# COMMAND ----------

df_import.head()

# COMMAND ----------

from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt

def create_polynomial_regression(degree):
    poly = PolynomialFeatures(degree=degree, include_bias = False)
    x_poly = poly.fit_transform(x_train)
    poly_reg = LinearRegression()
    poly_reg.fit(x_poly, y_train)
    y_train_predicted = poly_reg.predict(x_poly)
    y_test_predict = poly_reg.predict(poly.fit_transform(x_test))
    
    rmse_train = np.sqrt(mean_squared_error(y_train, y_train_predicted))
    r2_train = r2_score(y_train, y_train_predicted)
    
    rmse_test = np.sqrt(mean_squared_error(y_test, y_test_predict))
    r2_test = r2_score(y_test,y_test_predict)
    
    print("Model performance for training set")
    print("----------------------------------")
    print("RMSE of training set is {}".format(rmse_train))
    print("R2 score of training set is {}".format(r2_train))
    
    print("Model performance for test set")
    print("----------------------------------")
    print("RMSE of test set is {}".format(rmse_test))
    print("R2 score of test set is {}".format(r2_test))
    

# COMMAND ----------

create_polynomial_regression(4)

# COMMAND ----------

pip install prophet

# COMMAND ----------

from prophet import Prophet
import pandas as pd
from pandas.tseries.offsets import *

df_import['time'] = pd.to_datetime(df_import['year'], format="%Y")
df_import['time'] = df_import.apply(lambda x:(x['time'] + BQuarterBegin(x['period'])), axis=1)

df_import = df_import.rename(columns={'time':'ds','index_nsa':'y'})

# COMMAND ----------

nat_level = df_import[df_import['level']  == 'USA or Census Division']
nat_level = nat_level[nat_level['year'] <= 2018]
nat_level = nat_level[nat_level['place_name'].str.contains('tates$')]
nat_level = nat_level[nat_level['hpi_flavor'].str.contains('all-transactions')]

# COMMAND ----------

df_prophet = nat_level[['ds','y']]
m = Prophet()
m.fit(df_prophet)

# COMMAND ----------

future = m.make_future_dataframe(periods=12, freq='Q')
future.tail()

# COMMAND ----------

df_import.columns

# COMMAND ----------

df_import = df_import[(df_import['year'] == 2020) | (df_import['year'] == 2021)]
df_import = df_import.rename(columns={'y':'actual_value'})

df_import[['actual_value','year']].tail(6)

# COMMAND ----------

forecast = m.predict(future)
forecast = forecast.rename(columns={'yhat':'predicted_index','yhat_lower':'predicted_lower_bounds','yhat_upper':'predicted_upper_bounds'})
#forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
forecast[['ds','predicted_index','predicted_lower_bounds','predicted_upper_bounds']].tail()

# COMMAND ----------

from prophet.plot import plot_plotly, plot_components_plotly

plot_plotly(m, forecast)

# COMMAND ----------


