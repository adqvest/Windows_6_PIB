# done by kama 02/05/2024
import pandas as pd
import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import re
import sqlalchemy
import numpy as np
import calendar
from selenium import webdriver
from pandas.core.common import flatten
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import calendar
import ClickHouse_db
import MySql_To_Clickhouse as MySql_CH
from pandas.tseries.offsets import MonthEnd
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
def get_month_end(month, year):
    month_num = list(calendar.month_name).index(month.capitalize())
    num_days = calendar.monthrange(int(year), month_num)[1]
    month_end_date = datetime.datetime(int(year), month_num, num_days)
    month_end_date_str = month_end_date.strftime("%Y-%m-%d")
    
    return month_end_date_str
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MCX_PROVISIONAL_SETTLEMENT_GAURANTEE_FUND_SGF_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        url='https://www.mcxccl.com/risk-management/settlement-guarantee-fund/settlement-guarantee-fund-archive'
        robot.add_link(url)
        headers={'User-Agent':
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
        r = requests.get(url,headers=headers,verify=False,timeout=60)
        time.sleep(5)
        main_soup = BeautifulSoup(r.content)
        l1 = main_soup.findAll("a")
        # print(l1)
        links=[]
        text=[]
        links=[link.get("href") for link in l1 if "SGF" in link.text]
        date=[(pd.to_datetime(t.text.split('of')[2].strip())+MonthEnd()).date() for t in l1 if "SGF" in t.text]
        max_date=pd.read_sql('select max(Relevant_Date) as date from MCX_PROVISIONAL_SETTLEMENT_GAURANTEE_FUND_SGF_MONTHLY_DATA',engine)['date'][0]
        print(max_date)

        links_df = pd.DataFrame({
                                'Links': links,
                                'Dates': date
                                })
        links_df=links_df[links_df['Dates']>max_date]
        
        if not links_df.empty:
            data_final=pd.DataFrame()
            for i in range(len(links_df)):
                tables = camelot.read_pdf(links_df['Links'][i])
                df=tables[0].df
                df.columns=['Particulars','MCXCCL_Contribution_INR_Cr','MCX_Contribution_INR_Cr','Member_Contribution_INR_Cr','Other_Contribution_INR_Cr','Total_INR_Cr']
                df=df.iloc[1:,:]
                print(links_df['Dates'][i])
                df['Total_INR_Cr']=float(df['MCXCCL_Contribution_INR_Cr'])+float(df['MCX_Contribution_INR_Cr'])+float(df['Other_Contribution_INR_Cr'])
                df.replace('-', np.nan, inplace=True)
                df['Relevant_Date']=pd.to_datetime(links_df['Dates'][i]).date()
                df['Runtime']=pd.to_datetime('now')
                data_final=data_final.append(df)
            print(data_final)
            max_date_data=max(data_final['Relevant_Date'])
            print(max_date_data)
            if max_date_data>max_date:
                print('new data came')
                data_final=data_final[data_final['Relevant_Date']>max_date]
                data_final.to_sql('MCX_PROVISIONAL_SETTLEMENT_GAURANTEE_FUND_SGF_MONTHLY_DATA', index=False, if_exists='append', con=engine)
        
            else:
                print('Data is up to date')
        else:
            print('Data is upto date')
        MySql_CH.ch_truncate_and_insert('MCX_PROVISIONAL_SETTLEMENT_GAURANTEE_FUND_SGF_MONTHLY_DATA')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')


