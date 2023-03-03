# Databricks notebook source
# create dataframes as pandas
import pandas as pd
hpi_df = pd.read_csv("HPI_master.csv")

# test that dfs loaded
display(hpi_df.shape) # 121462 rows, 10 columns
display(hpi_df.head(10))