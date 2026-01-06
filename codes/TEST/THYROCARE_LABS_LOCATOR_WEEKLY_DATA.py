import warnings
warnings.filterwarnings('ignore')
import datetime
import time

import sys
import os
import re
import numpy as np
import pandas as pd
from pytz import timezone
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir')
from State_Function import state_rewrite


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = "HEALTHCARE_FACILITIES_LOCATOR_WEEKLY_DATA"

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date = pd.read_sql('SELECT MAX(Relevant_Date) as max from HEALTHCARE_FACILITIES_LOCATOR_WEEKLY_DATA WHERE Company = "Thyrocare Technologies Limited"', con=engine)['max'][0]
        if today.date() - max_rel_date >= datetime.timedelta(7):

            url = 'https://www.thyrocare.com/location'
            address = []
            pincode = []
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                page = browser.new_page()
                page.goto(url, wait_until='networkidle')

                time.sleep(2)
                soup2 = BeautifulSoup(page.content())
                data_soup = soup2.findAll('div', class_ = 'col-md-12 card-tst-head')
                for i in range(len(data_soup)):
                    address.append(data_soup[i].find('input')['value'])
                    pincode.append(data_soup[i].findAll('p')[1].text.split(': ')[-1])
                time.sleep(2)
            df = pd.DataFrame()
            df['Address'] = address
            df['Pincode'] = pincode
            df['State'] = np.nan
            for i in range(len(df.Pincode)):
                try:
                    state = state_rewrite.state(df.Pincode[i]).split('|')[-1].title()
                    if state == '':
                        df['State'][i] = np.nan
                    else:
                        df['State'][i] = state
                except:
                    df['State'][i] = np.nan
            df['Company'] = 'Thyrocare Technologies Limited'
            df['Category'] = 'Healthcare Facilities'
            df['Sub_Category_1'] = 'Diagnostic Laboratory'
            df['Relevant_Date'] = today.date()
            df['Runtime'] = today
            df.to_sql('HEALTHCARE_FACILITIES_LOCATOR_WEEKLY_DATA', if_exists='append', index=False, con=engine)
        else:
            print('No new data')

        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if (__name__ == '__main__'):
    run_program(run_by='manual')
