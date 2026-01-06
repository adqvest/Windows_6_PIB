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
table_name = 'NAUKRI_CLEANING'
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


        tables =  ['NAUKRI_TOTAL_JOBS_DATA',
                'NAUKRI_TOTAL_JOBS_INDUSTRY_DATA',
                'NAUKRI_TOTAL_JOBS_STATE_DATA']

        for tbls in tables:

           query = 'Select Distinct Relevant_Date as RD from AdqvestDB.'+tbls+' group by Relevant_Date'
           all_dates = list(pd.read_sql(query,con=engine)['RD'])
           for dates in all_dates:
               df = pd.read_sql("Select * from TempBackUp."+tbls+" where Relevant_Date = '"+str(dates)+"'",con=engine)

               if(tbls=='NAUKRI_TOTAL_JOBS_INDUSTRY_DATA'):

                 df = df.drop_duplicates(['State', 'City', 'Industries', 'Number', 'Relevant_Date'])
                 df.to_sql(tbls+"_NEW", con=engine, if_exists='append', index=False)

               elif(tbls=='NAUKRI_TOTAL_JOBS_DATA'):

                 df = df.drop_duplicates(['State', 'City', 'Number', 'Relevant_Date'])
                 df.to_sql(tbls+"_NEW", con=engine, if_exists='append', index=False)

               else:

                 df = df.drop_duplicates(['State', 'City', 'Number', 'Relevant_Date'])
                 df.to_sql(tbls+"_NEW", con=engine, if_exists='append', index=False)

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
