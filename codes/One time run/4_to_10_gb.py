# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 23:47:27 2022

@author: Abdulmuizz
"""

import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar

import os
import requests
import json
from bs4 import BeautifulSoup

import random

import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
from time import sleep
import time
import sys
# import warnings
# warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
import numpy as np
# import adqvest_db
# import adqvest_db_amz
# import JobLogNew as log

#DB Connection
properties = pd.read_csv(r"C:\Users\Administrator\AdQvestDir\Amazon_AdQvest_properties.txt",delim_whitespace=True)
#properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
connection  = engine.connect()



#DB Connection
# properties = pd.read_csv(r"C:\Users\Abdulmuizz\Desktop\ADQVest\AmazonDB\Amazon_AdQvest_properties.txt",delim_whitespace=True)
# #properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

# host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
# port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
# db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

# con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
# engine_amz     = sqlalchemy.create_engine(con_string,encoding='utf-8')
# # connection  = engine.connect()


#os.chdir('/home/ubuntu/AdQvestDir')
# engine = adqvest_db.db_conn()
# engine_amz = adqvest_db_amz.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
Day       = datetime.timedelta(1)
yesterday = today - Day

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'TEST'

no_of_ping = 0

all_table = pd.read_sql("SELECT Table_Name as Table_Name, ROUND(((data_length + index_length) / 1024 / 1024/ 1024), 4) AS Size_GB, date(CREATE_TIME) as Create_Date FROM information_schema.TABLES WHERE table_schema = 'AdqvestDB' ORDER BY table_name ",engine)


all_table_1 = all_table[(all_table['Size_GB'] > 4) & (all_table['Size_GB'] < 10)]


final_df_1 = pd.DataFrame()
iteration = 1
for i in all_table_1['Table_Name']:
    each_table = all_table_1[all_table_1['Table_Name'] == i]
    print(str(iteration) + ': '+ i)
    try:
        table_rel = pd.read_sql('Select max(Relevant_Date) as Relevant_Date from ' + i, engine)
        each_table['Max_Rel_Date']  = table_rel['Relevant_Date'][0]
    except:
        each_table['Max_Rel_Date']  = None #datetime.date(1970,1,1)
        print('Exception Max_Rel_Date: '+ str(iteration))

    try:
        table_rel = pd.read_sql('Select min(Relevant_Date) as Relevant_Date from ' + i, engine)
        each_table['Min_Rel_Date']  = table_rel['Relevant_Date'][0]
    except:
        each_table['Min_Rel_Date']  = None #datetime.date(1970,1,1)
        print('Exception Min_Rel_Date: '+ str(iteration))

    try:
        table_run = pd.read_sql('Select max(Runtime) as Runtime from ' + i, engine)
        each_table['Last_Runtime']  = table_run['Runtime'][0]
    except:
        each_table['Last_Runtime']  = None
        print('Exception Runtime: '+ str(iteration))


    final_df_1 = pd.concat([final_df_1,each_table])
    iteration = iteration + 1


final_df_1.to_excel(r"C:\Users\Administrator\AdQvestDir\Amazon_last_rel_10_gb.xlsx", index = False)
