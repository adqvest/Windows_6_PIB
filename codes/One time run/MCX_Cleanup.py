# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 10:56:04 2022

@author: Abhishek Shankar
"""



import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import adqvest_db
import JobLogNew as log
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

engine = adqvest_db.db_conn()

connection = engine.connect()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


table_name = 'MCX_DAILY_VOLUME_DATA'
new_table_name = table_name.replace("_DATA",'_CLEAN_DATA')


min_date = pd.read_sql('Select min(Relevant_Date) as RD from AdqvestDB.'+table_name,con=engine).iloc[0,0]
max_date = pd.read_sql('Select max(Relevant_Date) as RD from AdqvestDB.'+table_name,con=engine).iloc[0,0]


all_dates = []
while min_date<=max_date:

  all_dates.append(min_date)

  min_date+=days


#%%
for dates in all_dates:
  print(dates)
  data = pd.read_sql("Select * from AdqvestDB."+table_name+" where Relevant_Date = '"+str(dates)+"'",con=engine)
  data['Unit']  = data['Volume_In_Thousands'].str.split(" ").str[1].str.strip()
  data = data[['Date', 'Instrument_Name', 'Symbol', 'Expiry_Date', 'Option_Type',
       'Strike_Price', 'Open', 'High', 'Low', 'Close', 'Previous_Close',
       'Volume_Lots', 'Volume_In_Thousands', 'Unit', 'Traded_Volume_K', 'Value_Lacs',
       'Open_Interest_Lots', 'Relevant_Date', 'Runtime']]
  data.to_sql(new_table_name, if_exists="append", index=False, con=engine)
