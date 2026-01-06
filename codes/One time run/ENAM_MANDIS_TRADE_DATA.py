# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 16:53:16 2021

@author: Abhishek Shankar
"""
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import time
import re
import os
import igraph as ig
import timeit
import json
import time
import itertools
from clickhouse_driver import Client
import unidecode
import os
import datetime
import unidecode
import timeit
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import requests
import JobLogNew as log
from dateutil import parser
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
from clickhouse_driver import Client
from dateutil.relativedelta import relativedelta
client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com',
                user='default',
                password='@Dqu&TP@ssw0rd',
                database='AdqvestDB',
               port=9000)


# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


def ldm(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def fdm(any_day):

    days = int(ldm(any_day).strftime("%d"))
    fd = ldm(any_day) - datetime.timedelta(days = days-1)

    return fd


engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'ENAM_MANDIS_TRADE_DATA'

no_of_ping = 0
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        start = datetime.date(2020,3,1)
        finish = yesterday.date()
        date_iters = []
        while start<=finish:

          a = start
          print(a)
          date_iters.append(a)
          start = start+datetime.timedelta(1)


        for dates1 in date_iters:

              time.sleep(1)
              r = requests.get("https://enam.gov.in/web/dashboard/trade-data")

              headers = {
                     "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
                     }

              headers['cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])


              url = 'https://enam.gov.in/web/Ajax_ctrl/trade_data_list'


              data = {"language": "en",
                      "stateName": "-- All --",
                      "apmcName": "-- Select APMCs --",
                      "commodityName": "-- Select Commodity --",
                      'fromDate': str(dates1),
                      "toDate": str(dates1)}
              time.sleep(2)
              r = requests.post(url,data=data,headers=headers)

              data = json.loads(r.text)
              data = pd.DataFrame(data['data'])

              data = data[[ 'state', 'apmc', 'commodity', 'min_price', 'modal_price',
               'max_price', 'commodity_arrivals', 'commodity_traded','Commodity_Uom', 'created_at']]

              data.columns = ['State','Apmcs','Commodity','Price_In_Rs_Min_Price','Price_In_Rs_Modal_Price','Price_In_Rs_Max_Price','Commodity_Arrivals','Commodity_Traded','Unit','Date']


              data[['Price_In_Rs_Min_Price', 'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price', 'Commodity_Arrivals', 'Commodity_Traded']] = data[['Price_In_Rs_Min_Price', 'Price_In_Rs_Modal_Price', 'Price_In_Rs_Max_Price', 'Commodity_Arrivals', 'Commodity_Traded']].apply(pd.to_numeric)
              data['Date'] = data['Date'].apply(lambda x:parser.parse(x).date())
              data['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
              data.rename(columns = {"Date":"Relevant_Date"},inplace = True)
              data.to_sql('ENAM_MANDIS_TRADE_DATA_BACKUP', con=engine, if_exists='append', index=False)
              print("UPLOADED : ", str(dates1))
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
