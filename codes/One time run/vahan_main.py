
import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
import vahan_function as vf

import re
import ast
import datetime as datetime
from pytz import timezone
import requests

import numpy as np
import time

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLog as log
import adqvest_db





################
os.chdir('C:/Users/Administrator/vahan/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'VAHAN_CATEGORY_YEAR_WISE_DATA'

scheduler = ''
no_of_ping = 0


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    if(run_by=='Adqvest_Bot'):
        log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
    else:
        log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

    main_limit = 0
    while True:
        try:
            today = datetime.datetime.now(india_time)

            date_vahan = (today - datetime.timedelta(130)).strftime('%Y-%m-%d')

            vf.main_selenium()


            break

        except:
            main_limit += 1
            print(main_limit)
            print(sys.exc_info())
            error_msg = str(sys.exc_info()[1])
            if(error_msg=='come out completly'):
                error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                error_msg = "Program ran successfully but adjust_data function or enter log end time failed"
                log.job_error_log(table_name,job_start_time,error_type,error_msg)
                break


            if(main_limit>40):
                error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                error_msg = str(sys.exc_info()[1])
                log.job_error_log(table_name,job_start_time,error_type,error_msg)
                break

            print('new error')

            time.sleep(30)
            continue
if(__name__=='__main__'):
    run_program(run_by='manual')
