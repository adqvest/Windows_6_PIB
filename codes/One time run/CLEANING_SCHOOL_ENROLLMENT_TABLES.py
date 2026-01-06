# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 09:59:16 2021

@author: Abhishek Shankar
"""




import sqlalchemy
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
import re
import calendar
warnings.filterwarnings('ignore')
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# In[22]:


os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'SCHOOL_ENROLLMENT_CLEANING'
scheduler = ''
no_of_ping = 0


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        query = 'Select * from AdqvestDB.SCHOOL_ENROLMENT_BY_MANAGEMENT_YEARLY_RAW_DATA'
        mgmt = pd.read_sql(query,con=engine)

        df = mgmt.copy()


        cleandf = pd.DataFrame()

        for i,row in df.iterrows():
            print(i)
            a = row.T.reset_index()
            p1 = a.head(10)
            p2 = a.tail(6).T
            headers = p2.iloc[0]
            new_df  = pd.DataFrame(p2.values[1:], columns=headers)
            p1.columns = ['Variable','Value']
            p1['Value'] = pd.to_numeric(p1['Value'],errors='coerce')
            final = pd.concat([p1,new_df],axis=1)
            cols = list(final.columns)[2:]
            final[cols] = final[cols].ffill()
            final = final.drop(final[final['Variable'].str.lower().str.contains('total')].index,axis=0)
        #    cleandf = pd.concat([final,cleandf])
            final.to_sql(name='SCHOOL_ENROLMENT_BY_MANAGEMENT_YEARLY_DATA',con=engine,if_exists='append',index=False)


        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
