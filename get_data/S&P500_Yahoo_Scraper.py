#!/usr/bin/env python
# coding: utf-8

# In[34]:


import time
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import csv


# In[2]:


driver = webdriver.Chrome(executable_path='CHROMEDRIVER_PATH')


# In[3]:


url = "https://finance.yahoo.com/quote/%5EGSPC/history?period1=157766400&period2=1677801600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"


# In[4]:


driver.get(url)


# In[5]:


for i in range(0,130):
    driver.execute_script("window.scrollBy(0,5000)")
    time.sleep(2)
    print(i)
webpage = driver.page_source
soup = BeautifulSoup(webpage, "html.parser")


# In[6]:


for data in soup.find_all("td"):
    print(data)


# In[7]:


sp_data_res = soup.find_all("td")
sp_data = []
for td in sp_data_res:
    sp_data.append(td.text)


# In[8]:


len(sp_data)


# In[10]:


sp_data[85029]


# In[11]:


#I want to pop everyting after 85028
for i in range(len(sp_data)):
    if len(sp_data) < 85030:
        break
    else:
        sp_data.pop()


# In[12]:


len(sp_data)


# In[15]:


sp_data[85028]


# In[16]:


Date = []
Open = []
High = []
Low = []
Close = []
Adj_Close = []
Volume = []


# In[17]:


for n in range(0,len(sp_data),7):
    Date.append(sp_data[n])    


# In[22]:


for n in range(1,len(sp_data),7): 
    Open.append(sp_data[n])


# In[23]:


for n in range(2,len(sp_data),7): 
    High.append(sp_data[n])


# In[24]:


for n in range(3,len(sp_data),7): 
    Low.append(sp_data[n])


# In[25]:


for n in range(4,len(sp_data),7): 
    Close.append(sp_data[n])


# In[26]:


for n in range(5,len(sp_data),7): 
    Adj_Close.append(sp_data[n])


# In[27]:


for n in range(5,len(sp_data),7): 
    Volume.append(sp_data[n])


# In[36]:


df1 = pd.DataFrame({"Open":Open, "Close":Close, "High": High, "Low":Low, "Adj_Close":Adj_Close, "Volume": Volume, "Time":Date})


# In[37]:


df1


# In[38]:


#dataframe to CSV
df1.to_csv('S&P500_1975_Present.csv', index = False)


# In[ ]:




