# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 11:56:50 2021

@author: abhis
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 10:56:37 2021

@author: abhis
"""


import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import os
import calendar
#os.chdir(r"C:\Adqvest\Mercadolibre")
import sqlalchemy
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from dateutil import parser
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from dateutil import parser
from random import randint
import sys
from time import sleep
import requests, zipfile, io
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()

con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days



from dateutil.relativedelta import relativedelta

def last_day_of_month(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)

def get_month_day_range(date):
    """
    For a date 'date' returns the start and end date for the month of 'date'.
    Month with 31 days:
    >>> date = datetime.date(2011, 7, 27)
    >>> get_month_day_range(date)
    (datetime.date(2011, 7, 1), datetime.date(2011, 7, 31))
    Month with 28 days:
    >>> date = datetime.date(2011, 2, 15)
    >>> get_month_day_range(date)
    (datetime.date(2011, 2, 1), datetime.date(2011, 2, 28))
    """
    last_day = date + relativedelta(day=1, months=+1, days=-1)
    first_day = date + relativedelta(day=1)
    return [first_day, last_day]


def monthdelta(date, delta):
    m, y = (date.month-delta) % 12, date.year + ((date.month)-delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and (not y%100==0 or y%400 == 0) else 28,
        31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)

def monthdeltaforward(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and (not y%100==0 or y%400 == 0) else 28,
        31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)

def get_driver():
    download_file_path = 'C:\\Users\\Administrator\\AdQvestDir\\codes\\One time run\\NSE_FILES'
    prefs = {
                "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True
                }
    option = webdriver.ChromeOptions()

    option.add_experimental_option('prefs', prefs)

    #option = Options()
    driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=option)

    return driver
#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_CLIENT_FUNDING_HISTORICAL_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

        start_date = today.date() - datetime.timedelta(2590)
        end_date = today.date() - datetime.timedelta(790)

        iterated_dates = []
        while start_date <= end_date:

            ranges = get_month_day_range(start_date)
            iterated_dates.append(ranges)

            start_date = monthdeltaforward(start_date,1)

        for date_ranges in iterated_dates:
            driver = get_driver()
            try:
                Start_Date = date_ranges[0]

                End_Date = date_ranges[1]#yesterday.date()

                Day = datetime.timedelta(1)

                output = pd.DataFrame()

                while(Start_Date <= End_Date):

                    date = Start_Date.strftime("%d-%m-%Y")

                    try:
                        date = Start_Date.strftime("%d-%m-%Y")
                        #webdriver.Chrome(executable_path=r"C:\Adqvest\Selenium Extension\chromedriver.exe",options = options)

                        #    try:
                        driver.get("https://www1.nseindia.com/ArchieveSearch?h_filetype=eqcli&date="+date+"&section=EQ")

                        time.sleep(1)
                        html = driver.page_source

                        driver.find_elements_by_xpath("//a[@href]")[0].click()
                        time.sleep(2)
                        file = []
                        for files in os.listdir('C:\\Users\\Administrator\\AdQvestDir\\codes\\One time run\\NSE_FILES\\'):
                            if (files.endswith(".csv")):
                                file.append(files)
                                print(files,date)

                        df = pd.read_csv("C:\\Users\\Administrator\\AdQvestDir\\codes\\One time run\\NSE_FILES\\"+file[0])

                        NSE_CL_DATA = df

                        #driver.quit()

                        if "Submission Date" not in list(df.columns):
                            df['Submission Date'] = None

                        cols = ['Member Code', 'Member Name',
                               'Amount Funded For Temporary Margin INR Lakhs',
                               'Amount Funded For Institutional Clients INR Lakhs',
                               'Amount Funded For Non-Institutional Clients INR Lakhs',
                               'Amount Funded Under Margin Trading INR Lakhs',
                               'Total Amount Funded INR Lakhs', 'Total No of Clients Funded',
                               'Submission Date String']

                        cols = [x.replace(" ","_") for x in cols]

                        NSE_CL_DATA.columns = cols
                        try:
                            NSE_CL_DATA['Submission_Date'] = NSE_CL_DATA['Submission_Date_String'].apply(lambda x : parser.parse(x).date())
                        except:
                            NSE_CL_DATA['Submission_Date'] = None
                        NSE_CL_DATA['Relevant_Date'] = Start_Date
                        NSE_CL_DATA['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df = NSE_CL_DATA



                        os.chdir("C:\\Users\\Administrator\\AdQvestDir\\codes\\One time run\\NSE_FILES")
                        for files in file:
                            os.remove(files)

                        output = pd.concat([output,df])
                        del df
                        time.sleep(0.5)
                        Start_Date = Start_Date + Day
                    except:
                        print(date,Start_Date.strftime('%A'))
                        #driver.quit()
                        Start_Date = Start_Date + Day
                        continue

                output = output.drop_duplicates(cols)
                if len(list(output['Relevant_Date'].unique())) == 1:
                    output['Relevant_Date'] = output['Relevant_Date'].apply(lambda x : get_month_day_range(x)[1])
                else:
                    pass

                output.to_sql(name='NSE_CLIENT_FUNDING_DATA',con=engine,if_exists='append',index=False)
                print("Data Uploaded", output['Relevant_Date'].unique())

                driver.quit()
            except:
                driver.quit()
                driver = get_driver()
                continue
        try:
            driver.quit()
        except:
            pass
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
            driver.quit()
        except:
            pass

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
