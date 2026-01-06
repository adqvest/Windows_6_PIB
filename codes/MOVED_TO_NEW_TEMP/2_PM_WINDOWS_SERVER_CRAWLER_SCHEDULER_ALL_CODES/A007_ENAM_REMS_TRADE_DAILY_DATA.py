# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 01:11:02 2023
@author: Santonu
"""
import pandas as pd
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import time
#import igraph as ig
import json
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#%%
import ClickHouse_db
import adqvest_db
import requests
import JobLogNew as log
from dateutil import parser
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
from adqvest_robotstxt import Robots

from datetime import datetime as strptime
from dateutil.relativedelta import relativedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from clickhouse_driver import Client
robot = Robots(__file__)
engine = adqvest_db.db_conn()
connection = engine.connect()

#%%
# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def convert_date_format(input_date,output_format='%d-%b-%Y',input_format='%Y-%m-%d'):
    input_datetime = strptime.strptime(str(input_date), input_format)
    output_date = input_datetime.strftime(output_format)

    return output_date.upper()

def get_date_range(table_name,apmc_name):

    query="""select max(Relevant_Date) as RD from """ + table_name + """ where Apmcs='""" + apmc_name + """';"""
    max_date = pd.read_sql(query, con=engine)
    if (max_date['RD'].isnull().all()):
       start_date = pd.to_datetime('2018-01-01', format='%Y-%m-%d').date()
    else:
        start_date = max_date['RD'][0]

    start_date = datetime.date(start_date.year, start_date.month, start_date.day) + datetime.timedelta(1)
    end_date = datetime.date(yesterday.year, yesterday.month, yesterday.day)
    if((end_date-start_date).days>30):
            start_date=end_date-datetime.timedelta(30)
            print(start_date)

    if start_date>end_date:
        time_range=[]
    else:
        time_range = pd.date_range(str(start_date), str(end_date), freq='D')

    return time_range

def Upload_Data(table_name,data,apmc_name,db: list):
    query="""select max(Relevant_Date) as Max from """ + table_name + """ where Apmcs='""" + apmc_name + """';"""
    db_max_date = pd.read_sql(query,engine)
    if (db_max_date['Max'].isnull().all()):
       pass
    else:
        db_max_date = db_max_date["Max"][0]
        data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print("Data uploded in MySQL")
        print(data.info())

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def get_request_session(url):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.verify = False
    r=session.get(url)
    return r

def get_market_list(url):
    r=get_request_session(url)
    data = json.loads(r.text)
    data = pd.DataFrame(data['marketList'])
    return data
#%%
## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'ENAM_REMS_TRADE_DAILY_DATA'

no_of_ping = 0
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
#%%
#        
        data=get_market_list('https://mis.remsl.in/UMPInterOpService/MastersUpdate/getMarkets')
        data=data[['marketCode','marketShortName']]
        if data.empty==False:
            apmcs=zip(data.marketCode,data.marketShortName)
            for mc,apmc_name in apmcs:
                if mc not in ['KDLG','MCK9','RRB','FPO','YRGT']:
                    print(apmc_name)
                    time_range=get_date_range("ENAM_REMS_TRADE_DAILY_DATA",apmc_name)
                    if len(time_range)>0:
                        k = 0
                        for dates1 in time_range:
                            dates1=dates1.date()
                            print(f'Working on  {apmc_name}')
                            print(f'Working on  {dates1}')

                            url='https://mis.remsl.in/UMPInterOpService/TxnUpdate/getArrivals/'+mc+'/'+convert_date_format(dates1)
                            r=get_request_session(url)
                            # r=requests.get(url,verify=False)
                            robot.add_link(url)
                            
                            print(r.text)
                            data = json.loads(r.text)
                            if len(data['arrivals'])==0:
                                print(f'No data for--------------> {apmc_name}')
                                continue
                           
                            data = pd.DataFrame(data['arrivals'])
                            data['State']='KARNATAKA'
                            data['Apmcs']=apmc_name
                            data=data.rename(columns={"commodity":"Commodity",
                                                    "maxPrice":"Max_Price_Rs",
                                                    "minPrice":"Min_Price_Rs",
                                                    "modalPrice":"Modal_Price_Rs",
                                                    "sold":"Sold_quantity",
                                                    "totalArrival":"Total_Arrival_quantity",
                                                    "uom":"Unit"})
        
                            data['Relevant_Date'] = dates1
                            data['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                            data = data[data['Commodity'].notna()]
                            data["Commodity"]=data["Commodity"].replace({'BEGALGRAM':'BENGALGRAM',
                                                                         'MUSTERED':'MUSTARD',
                                                                         'SOAPNUT':'SOAP NUT'})

                            data=drop_duplicates(data)
                          
                            Upload_Data("ENAM_REMS_TRADE_DAILY_DATA",data,apmc_name,['MySQL'])
        else:
            print('Apms Code is Missing')    
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')