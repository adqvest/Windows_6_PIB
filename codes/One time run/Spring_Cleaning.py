# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 11:40:18 2021

@author: abhis
"""


#import scrapy
import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
import json
import requests
import os
import calendar
import sqlalchemy
import pandas as pd
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
import random
from dateutil import parser
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
from dateutil import parser
import sys
from time import sleep
import requests, zipfile, io
import time

#os.chdir('/home/ubuntu/AdQvestDir')

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
os.chdir(r'C:\Users\Administrator\AdQvestDir')
conndetail = pd.read_csv('Amazon_AdQvest_properties.txt',delim_whitespace=True)

hostdet = conndetail.loc[conndetail['Env'] == 'Host']
port = conndetail.loc[conndetail['Env'] == 'port']
DBname = conndetail.loc[conndetail['Env'] == 'DBname']
host = list(hostdet.iloc[:,1])
port = list(port.iloc[:,1])
dbname = list(DBname.iloc[:,1])
Connectionstring = 'mysql+pymysql://' + host[0] + ':' + port[0] + '/' + dbname[0]
engine = sqlalchemy.create_engine(Connectionstring)

connection = engine.connect()

#%%
os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run')
query = "SELECT table_name AS 'Table', ROUND(((data_length + index_length) / 1024 / 1024/1024), 9) AS `Size (GB)`, CREATE_TIME, date(CREATE_TIME), date(update_time)  FROM information_schema.TABLES WHERE table_schema = 'AdqvestDB' ORDER BY `Table`,date(CREATE_TIME) desc"

df = pd.read_sql(query,con=engine)

df = df[['Table', 'Size (GB)', 'CREATE_TIME', 'date(CREATE_TIME)']]

df.columns = ['Table', 'Size (GB)', 'CREATE_TIME', 'Date']


output = pd.DataFrame()
dates = []
for i,row in df.iterrows():

    table = row['Table']
    print(table)
    try:

        query = "Select max(Relevant_Date) as RD from AdqvestDB."+table

        date = pd.read_sql(query,con=engine)

        if date.empty:
            date = None
            dates.append(date)
        else:
            date = date['RD'][0]
            dates.append(date)
            print(table,i,date)
    except:

        date = None
        dates.append(date)


output['Max_Relevant_Date'] = dates
output.to_csv("AMAZONDB_TABLES.csv",index=False)
