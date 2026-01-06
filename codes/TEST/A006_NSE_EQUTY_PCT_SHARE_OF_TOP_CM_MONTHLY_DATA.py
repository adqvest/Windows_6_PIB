import datetime as datetime
import os
import re
import sys
import warnings
import pandas as pd
from pytz import timezone
from requests_html import HTMLSession
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from pandas.tseries.offsets import MonthEnd

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine     = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # session = HTMLSession()
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        driver = webdriver.Chrome(executable_path=driver_path)
        url = "https://www1.nseindia.com/products/content/equities/equities/eq_topbrokersyearwise.htm"
        r = driver.get(url)
        no_of_ping += 1
        df = pd.read_html(driver.page_source)
        df = df[0]
        member_index = df[df['Unnamed: 0'] == 'Members'].index[0]
        df = df[member_index+1:]
        df.columns = ['Relevant_Date', 'Top_5', 'Top_10', 'Top_25', 'Top_50', 'Top_100']
        df.reset_index(drop=True,inplace=True)

        df = df[:10]  #take latest 10 months data
        df['Comments'] = ''
        df['Relevant_Date'] = df['Relevant_Date'].apply(lambda d:datetime.datetime.strptime(d,'%b-%Y'))
        df['Relevant_Date'] = df['Relevant_Date'].apply(lambda d:(pd.to_datetime(d) - MonthEnd(0)).date())
        df['Runtime']       = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        df['Last_Updated']  = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

        query = "Select max(Relevant_Date) as Relevant_Date from NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA"
        max_date = pd.read_sql(query, con=engine)['Relevant_Date'][0]
        print(df)
        df = df[df['Relevant_Date'] > max_date]
        df.to_sql(name='NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA',con=engine,if_exists='append',index=False)
        print('To SQL | Rows: ', len(df))
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except :
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
