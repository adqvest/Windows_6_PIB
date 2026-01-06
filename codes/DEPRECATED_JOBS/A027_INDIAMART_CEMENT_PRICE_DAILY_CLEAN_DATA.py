import pandas as pd
from bs4 import BeautifulSoup
import sqlalchemy
import datetime as datetime
import warnings
warnings.filterwarnings("ignore")
import os
import requests
import re
import numpy as np
from pytz import timezone
import time
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'INDIAMART_CEMENT_PRICE_DAILY_DATA_CLEAN_TABLE_STOPPED'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        brands = []
        city = ['Mumbai', 'Delhi', 'Kolkata', 'Chennai', 'Hyderabad', 'Bengaluru', 'Pune', 'Jaipur', 'Patna',
                'Guwahati', 'Goa', 'Chandigarh', 'Kochi', 'Bhubaneswar', 'Lucknow']
       
        for i in range(len(city)):
            r = requests.get('https://dir.indiamart.com/' + city[i].lower() + '/construction-cement.html')
            print('https://dir.indiamart.com/' + city[i].lower() + '/construction-cement.html')
            print(r)
            soup = BeautifulSoup(r.content)
            for row in soup.findAll("li", attrs={'class': 'colps'}):
                val = row.text.strip()
                if val not in brands:
                    brands.append(val)
        print(brands)
        max_rel_date=pd.read_sql('select max(Relevant_Date) as Max from INDIAMART_CEMENT_PRICE_DAILY_DATA_CLEAN_TABLE_STOPPED',engine)['Max'][0]
        df_raw = pd.read_sql("select * from INDIAMART_CEMENT_PRICE_DAILY_DATA_STOPPED where Relevant_Date", engine)
        df=df_raw[df_raw['Relevant_Date']>max_rel_date]
        print(df)
        if df.empty:
            print('no new data')
        else:
            for i in range(df.shape[0]):
                print(df['Brand'].iloc[i])

                ratio = process.extract(df['Brand'].iloc[i], brands, limit=1, scorer=fuzz.token_set_ratio)
                similarity = ratio[0][1]
                if similarity > 85:
                    df['Brand'].iloc[i] = ratio[0][0]
                else:
                    df['Brand'].iloc[i] = df['Brand'].iloc[i]
            match = re.compile(r', Pack.*')
            df['Brand'] = df['Brand'].apply(lambda x: re.sub(match, '', x))
            df['Price'] = df['Price'].apply(lambda x: x.strip('?'))
            print(df.head())
            df.to_sql('INDIAMART_CEMENT_PRICE_DAILY_DATA_CLEAN_TABLE_STOPPED', con=engine, if_exists='append', index=False)
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
