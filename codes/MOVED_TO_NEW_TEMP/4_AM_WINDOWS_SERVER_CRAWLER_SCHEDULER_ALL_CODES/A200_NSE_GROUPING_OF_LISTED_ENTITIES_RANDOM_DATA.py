''' @author : Joe '''
import sqlalchemy
import calendar
import os
import requests
import json
import random
import re
import ast
import csv
import time
import sys
import PyPDF2
import urllib
import numpy as np
import pandas as pd
from time import sleep
from pandas.io import sql
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader,PdfFileWriter
from dateutil.relativedelta import relativedelta
from urllib.request import Request, urlopen
from requests_html import HTMLSession
from pytz import timezone
import glob
import camelot
import warnings
from urllib.parse import urlparse
warnings.filterwarnings('ignore')
import numpy as np
import datetime as datetime
from pandas.tseries.offsets import MonthEnd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_GROUPING_OF_LISTED_ENTITIES_RANDOM_DATA_N_NSE_INDIA_RELATED_EQUITY_SECURITIES_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '4_AM_WINDOWS_SERVER_CRAWLER_SCHEDULER_ALL_CODES'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def S3_upload(filename,bucket_folder):
            ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
            ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
            os.chdir(r'C:\Users\Administrator\AdQvestDir\NSE_GROUPING')
            BUCKET_NAME = 'adqvests3bucket'
            s3 = boto3.resource(
                 's3',
                 aws_access_key_id=ACCESS_KEY_ID,
                 aws_secret_access_key=ACCESS_SECRET_KEY,
                 config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
            data_s3 =  open(filename, 'rb')
            s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
            data_s3.close()

        max_date   = pd.read_sql('select max(Relevant_Date) as Max from NSE_GROUPING_OF_LISTED_ENTITIES_RANDOM_DATA', engine)['Max'][0]
        check_date = max_date + datetime.timedelta(1)

        chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_directory = r'C:\Users\Administrator\AdQvestDir\NSE_GROUPING'
        prefs = {
            "download.default_directory": download_directory,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }

        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', prefs)
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        options.add_experimental_option('prefs', prefs)

        driver = webdriver.Chrome(executable_path=chrome_driver_path, options = options)

        while today.date() >= max_date:
            if today.date() == check_date:
                break
            else:
                date = check_date.strftime('%d%m%Y')
                print(check_date)
                driver = webdriver.Chrome(executable_path=chrome_driver_path, options = options)
                driver.get(f'https://nsearchives.nseindia.com/corporate/compliance/Grouping_of_Listed_Entities_{date}.xlsx')
                robot.add_link(f'https://nsearchives.nseindia.com/corporate/compliance/Grouping_of_Listed_Entities_{date}.xlsx')
                time.sleep(7)
                if "The file you are trying to access doesn't appear to exist" in BeautifulSoup(driver.page_source).text:
                    print('No file for this date')
                else:
                    print('New file')
                    os.chdir(r'C:\Users\Administrator\AdQvestDir\NSE_GROUPING')
                    file = f"Grouping_of_Listed_Entities_{date}.xlsx"
                    print(file)
                    s3_folder='NSE/Grouping_Of_Listed_Entities'
                    S3_upload(file,s3_folder)
                    raw_dt = file.split('_')[-1].split('.xl')[0]
                    date = pd.to_datetime(raw_dt,format = "%d%m%Y").date()
                    print(date)

                    xls = pd.ExcelFile(file)
                    for sheet in xls.sheet_names:
                        if 'grouping' in sheet.lower():
                            print('Grouping')
                            grp_df = pd.read_excel(file, sheet)
                            grp_df.rename(columns = {'Serial No.' :  'Sr_No'},inplace=True)
                            grp_df.columns = [col.replace(' ', '_') for col in grp_df.columns]
                            grp_df['Relevant_Date'] = date
                            grp_df['Runtime'] = datetime.datetime.now()
                            grp_df.to_sql(name ="NSE_GROUPING_OF_LISTED_ENTITIES_RANDOM_DATA",index = False,con =engine,if_exists = "append")
                #             break
                        elif 'india related' in sheet.lower():
                            print('india')
                            sec_df = pd.read_excel(file, sheet)
                            sec_df = sec_df.dropna(how = 'all')
                            sec_df = sec_df.dropna(how = 'all', axis = 1)
                            sec_df = sec_df.rename(columns = {'Name of Company' : 'Company_Name', 'Names of Subsidiaries/Associates/JV' : 'Entity_Name', 'Subsidiary/Associates/JV' : 'Entity_Type'})
                            sec_df['Company_Name'] = sec_df['Company_Name'].ffill()
                            sec_df['Relevant_Date'] = date
                            sec_df['Runtime'] = datetime.datetime.now()
                            sec_df.to_sql(name ="NSE_INDIA_RELATED_EQUITY_SECURITIES_RANDOM_DATA",index = False,con =engine,if_exists = "append")
                    xls.close()
                    # xls.close()
                    os.remove(file)
                
            #         break
                check_date += datetime.timedelta(1)

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')