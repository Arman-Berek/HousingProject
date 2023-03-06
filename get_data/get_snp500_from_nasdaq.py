# S&P
import requests
import pandas as pd


# Variables
def get_snp500_quarterly():
    '''
    Get S&P 500 Quarterly from NASDAQ API at close
    https://data.nasdaq.com/data/MULTPL/SP500_REAL_PRICE_MONTH-sp-500-real-price-by-month
    

    Returns
    -------
    df_sp_quarterly : pandas.DataFrame
        dataframe .

    '''
    # https://data.nasdaq.com/data/MULTPL/SP500_REAL_PRICE_MONTH-sp-500-real-price-by-month

    # NASDAQ python call
    #quandl.get("MULTPL/SP500_REAL_PRICE_MONTH", authtoken="fJueoQJtK4zU-raf1asE")

    # CSV
    # url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_REAL_PRICE_MONTH.csv?api_key=fJueoQJtK4zU-raf1asE"

    dt_start = '1975-01-01'
    api_key = "fJueoQJtK4zU-raf1asE"
    url_base = "https://data.nasdaq.com/api/v3/datasets/MULTPL"
    stock = "SP500_REAL_PRICE_MONTH"
    
    params = {
        "api_key" : api_key
        }
    
    # url
    api_url = "{}/{}.csv".format(url_base, stock)
    
    # Get Request
    response = requests.get(api_url, params=params)
    print(response)
    url = response.url
    df = pd.read_csv(url)
    
    ## CLEAN
    # convert to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # filter df
    df = df.loc[df['Date'] >= dt_start]
    df = df.sort_values(['Date'], ascending=True)
    
    # add quarters
    df['Period'] = df['Date'].dt.quarter
    df['Year'] = df['Date'].dt.year
    
    # Group by quarters
    df_sp_quarterly = df.loc[:, ['Period', 'Year', 'Value']].groupby(['Period', 'Year']).mean().reset_index()
    df_sp_quarterly = df_sp_quarterly.sort_values(['Year', 'Period'], ascending=True).reset_index(drop=True)
    
    
    df_sp_quarterly = df_sp_quarterly.rename(columns={'Value' : 'SNP500_Value'})
    
    return df_sp_quarterly


#df_sp_quarterly = get_snp500_quarterly()

df_snp_quarterly_pandas = get_snp500_quarterly()
'''
# create dataframes as PySpark
sp500_quarterly_agg = spark.createDataFrame(df_snp_quarterly_pandas)
# test that dfs loaded
display((sp500_quarterly_agg.count(), len(sp500_quarterly_agg.columns))) # 192 x 4
display(sp500_quarterly_agg.tail(5))
'''
