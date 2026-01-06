# -*- coding: utf-8 -*-
"""
Created on Fri May  6 10:54:11 2022

@author: Abhishek Shankar
"""


# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 14:40:52 2022

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
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function/')
#sys.path.insert(0, r'C:\Adqvest')
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
def location_cleaner(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    words = ['rto','apmc','sdm','arto','dto','rla','srto','uo','rta','sta','apmcs','krishi upaj mandi samiti','krishi upaj samiti']
    for w in words:
      text = re.sub(r'(?<!\S)' + w + '+(?!\S)', "", text, flags=re.IGNORECASE)
    text = text.replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace("'",' ').replace(';','').replace('"','')
    text = text.replace('/','').replace('+','')
    text = re.sub(r'[?|$|.|!]',r'',text)
    text = text.replace('  ',' ')
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r"'\w+", " ", text)
    text = re.sub(r'  +', ' ', text).strip()
    return text
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
    table_name = 'SCHOOLS_DISTRICT_TAGGER'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
#%%
        table1 = "SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA"
        table2 = "SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA"

        new_table1 = "SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA_STAMPED"#"AGMARKNET_NAM_TRADE_CLEAN_DAILY_DATA"
        new_table2 = "SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA_STAMPED"#"ENAM_MANDIS_TRADE_CLEAN_DAILY_DATA"
        lookup = pd.read_sql("Select * from AdqvestDB.DISTRICT_LOOKUP_TABLE where `Table` = 'UDISE'",con=engine)



        min_date1 = pd.read_sql("Select Distinct Relevant_Date as RD from AdqvestDB."+table1,con=engine).iloc[:,0].to_list()
#        max_date = pd.read_sql("Select max(Relevant_Date) from AdqvestDB."+table1,con=engine).iloc[0,0]
        #min_date2 = pd.read_sql("Select Distinct Relevant_Date as RD from AdqvestDB."+table2,con=engine).iloc[:,0].to_list()
#        max_date = pd.read_sql("Select max(Relevant_Date) from AdqvestDB."+table1,con=engine).iloc[0,0]

#        all_dates = min_date#[]
#        all_dates = min_date#[]
        all_dates = min_date1#list(set(min_date1).intersection(set(min_date2)))
#        while min_date<=max_date:
#
#          all_dates.append(min_date)
#
#          min_date+=days
        if all_dates!=[]:
          for date1 in all_dates:
              print("################")

              print(date1)

              '''

              SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA


              '''
              df = pd.read_sql("Select * from AdqvestDB."+table1+" where Relevant_Date = '"+str(date1)+"';",con=engine)
#              df = df1.copy()
              df['Search_Term'] = df['District'] + " " +df['State']
              df['Search_Term'] = df['Search_Term'].apply(lambda x : location_cleaner(x))
              c1 = df.shape[0]
              df = pd.merge(df.applymap(lambda s: s.lower() if type(s) == str else s), lookup[['Tagged_District','Search_Term','LGD_State']].applymap(lambda s: s.lower() if type(s) == str else s).drop_duplicates(['Search_Term']), how='left', on='Search_Term')
              c2 = df.shape[0]
              if c1!=c2:
                 raise Exception("Merge Error")
              df['Tagged_State'] = df['LGD_State']
              df = df[['Variable', 'Class', 'Type', 'Gender', 'Value', 'Category', 'District',
                     'State','Tagged_State','Tagged_District', 'Timeperiod', 'Relevant_Date', 'Runtime']]
              df = df.applymap(lambda s: s.title() if type(s) == str else s)
              check = df.copy()

              if check['Tagged_District'].isnull().any():
                raise Exception("New Seacrh Term Has Come up Check Training Data")
              #df.to_sql(name =new_table1,if_exists="append",index = False,con = engine)
              print("Uploaded UDISE",str(date1))
              final = df.copy()
              del df
#              final = final[[x for x in final.columns if x.lower() not in ['relevant_date','runtime','relevant_quarter']]+[x for x in final.columns if x.lower() in ['relevant_date','runtime','relevant_quarter']]]
              final = final.applymap(lambda s: s.title() if type(s) == str else s)
              final = final.applymap(lambda s: s.strip() if type(s) == str else s)
              final['Category'] = final['Category'].apply(lambda x : x.upper() if len(x) <4 else x)
              final.to_sql(name =new_table2,if_exists="append",index = False,con = engine)

        else:
          print("Clean Data Present")

        #%%

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
