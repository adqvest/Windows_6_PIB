# -*- coding: utf-8 -*-
"""
Created on Mon Dec 20 13:09:09 2021

@author: Abhishek Shankar
"""




# -*- coding: utf-8 -*-
"""
Created on Mon Dec 20 10:22:17 2021

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


#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
#df = pd.read_excel("C:\Adqvest\Consumer\Master Database.xlsx")
#df = pd.read_csv(r"C:\Adqvest\Naukri")

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'EXIM_TAGGING_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        codes = pd.read_sql("Select * from AdqvestDB.EXIM_HS_CODE_COMMODITY_PAIR_STATIC where `Type`='HSN';",con=engine)

        data = pd.read_sql("Select * from AdqvestDB.EXPORTS_DATA_COUNTRY_WISE_MONTHLY limit 100;",con=engine)

        df1 = codes.copy()[['Code','Description']]
        df2 = data.copy()
        df2=df2.rename(columns = {'HSCode':'Code'})

        df1['Code'] = df1['Code'].astype(int)
        df2['Code'] = df2['Code'].astype(int)



        query1 = 'Select Distinct Relevant_Date as RD from AdqvestDB.EXPORTS_DATA_COUNTRY_WISE_MONTHLY group by Relevant_Date;'
        query2 = 'Select Distinct Relevant_Date as RD from AdqvestDB.EXPORTS_DATA_HSN_MONTHLY group by Relevant_Date;'

        tables = ['EXPORTS_DATA_COUNTRY_WISE_MONTHLY','EXPORTS_DATA_HSN_MONTHLY']
        queries = [query1,query2]
        for query,tbls in zip(queries,tables):

          all_dates = pd.read_sql(query,con=engine)

          final_df = pd.DataFrame()
          na = []
          all_dates = sorted(list(all_dates['RD']))
          for dt1 in all_dates:

            q = "Select * from AdqvestDB."+tbls+" where Relevant_Date = '"+str(dt1)+"';"
        #  q = "Select * from AdqvestDB.EXPORTS_DATA_HSN_MONTHLY where Relevant_Date = '"+str(dt1)+"';"
            #q = "Select * from AdqvestDB.EXPORTS_DATA_COUNTRY_WISE_MONTHLY where Relevant_Date = '"+str(dt1)+"';"

            old = pd.read_sql(q,con=engine)
        #  old['HSCode_Old'] = old['HSCode']
            old = old.rename(columns = {'HSCode':'Code'})
            old['Code'] = pd.to_numeric(old['Code'],errors='coerce')
        #  old['Code'] = np.where(old['Code'].astype(str).str.len()>6,pd.to_numeric(old['Code'].astype(str).str[0:6],errors='coerce'),pd.to_numeric(old['Code'],errors='coerce'))
            merged = df1.merge(old,on='Code',how='right')
        #  if int(dt1.strftime("%Y"))>=2020:
        #      merged['Code'] = old['HSCode_Old']#pd.to_numeric(old['Code'].astype(str).str[:-2],errors='coerce')
        #  else:
        #      merged['Code'] = pd.to_numeric(old['Code'].astype(str).str[:-2],errors='coerce')

        #  if merged['Description'].isnull().count():
        #    raise Exception("some data not getting merged")
            val = merged[merged['Description'].isnull()==True]['Code'].count()
            merged['Description'] = np.where(merged['Description'].isnull(),merged['Commodity'],merged['Description'])
            merged = merged.rename(columns = {'Code':'HSCode'})
            merged = merged.rename(columns = {'Description':'Commodity_New'})
            merged = merged.rename(columns = {'Commodity':'Commodity_Old'})
        #  merged['HSCode_New'] = merged['HSCode_New'].astype(str)
        #  merged['HSCode_New'] = merged['HSCode_New'].astype(str).replace('\.0', '', regex=True)
            merged =  merged[[x for x in  merged.columns if '_new' in x.lower()]+ [x for x in  merged.columns if '_old' in x.lower()] + [x for x in  merged.columns if (('_new' not in x.lower()) and ('_old' not in x.lower()))]]
            if (val/len(merged))*100 >2:
                na.append(dt1)
        #    raise Exception("some data not getting merged")

            merged.to_sql(name=tbls+"_Temp_Abhi",con=engine,if_exists='append',index=False)


            print(dt1,"Done with Error of ", (val/len(merged))*100)

        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
