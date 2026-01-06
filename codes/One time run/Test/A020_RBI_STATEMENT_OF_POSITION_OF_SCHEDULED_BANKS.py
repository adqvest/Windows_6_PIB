import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
import json
import requests
import os
import sqlalchemy
import pandas as pd

import random
import time
import numpy as np
import math
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from tqdm import tqdm
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'test_code'
no_of_ping = 0

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    global job_start_time
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def something():
            count = 0
            while count < 10:
                print("yes")

                # relevant_date = job_start_time.strftime("%Y-%m-%d")
                # job_end_time       = datetime.datetime.now(india_time)
                # job_execution_time = (job_end_time - job_start_time).total_seconds()
                # job_end_time       = '"'+job_end_time.strftime("%Y-%m-%d %H:%M:%S")+'"'
                # job_start_time_temp = job_start_time.strftime("%Y-%m-%d %H:%M:%S")
                # query      = 'Update LOG_TABLE_NEW_TEST set No_Of_Ping=' + str(no_of_ping) + ', End_Time = ' + job_end_time + ', Execution_Time_Seconds = "' + str(job_execution_time) + '", Runtime = ' + job_end_time + ' where Start_Time = "' + job_start_time_temp + '" and Table_Name = "'+table_name+'"'
                # connection = engine.connect()
                # connection.execute(query)
                # connection.execute('commit')
                # connection.close()
                count += 1
                time.sleep(5)
                log.job_end_log(table_name,job_start_time,no_of_ping)
            print(a)
        something()

        #log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
