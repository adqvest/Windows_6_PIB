import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
import json
import requests
import os
import sqlalchemy
import pandas as pd
import selenium

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
import re
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()


    #DB Connection
    properties = pd.read_csv('AdQvest_properties.txt',delim_whitespace=True)

    host    = list(properties.loc[properties['Env'] == 'Host'].iloc[:,1])[0]
    port    = list(properties.loc[properties['Env'] == 'port'].iloc[:,1])[0]
    db_name = list(properties.loc[properties['Env'] == 'DBname'].iloc[:,1])[0]

    con_string = 'mysql+pymysql://' + host + ':' + port + '/' + db_name + '?charset=utf8'
    engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA'

    scheduler = 'One_time_run'
    no_of_ping = 0
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        # user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36'
        # options.add_argument('user-agent={0}'.format(user_agent))


        url = "https://www.naukri.com/jobs-by-location"
        driver_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        driver=webdriver.Chrome(executable_path=driver_path)

        driver = webdriver.Chrome(executable_path =driver_path,
                                   chrome_options=options)
        # url = "https://www.naukri.com/jobs-by-location"

        no_of_ping += 1
        driver.get(url)
        driver.maximize_window()
        final_df = pd.DataFrame()
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        all_columns = soup.findAll('div', class_='column')

        zipped = []
        for data in all_columns:
            for state_data in data.findAll('div', class_='section_white_title'):
                city_data = state_data.findAll('a')

                for i in range(len(city_data)):
                    if((i==0) & (('union terr' in city_data[0].text.lower())==False)):

                        state = city_data[i].text.split(' in ')[1].strip()
                        city = city_data[i].text.split(' in ')[1].strip()
                        link = city_data[i]['href']
                        zipped.append([state, city, link])
                    elif(('union terr' in city_data[0].text.lower())):
                        if(i==0):
                            pass
                        else:
                            state = city_data[0].text.split(' in ')[1].strip()
                            city = city_data[i].text.split(' in ')[1].strip()
                            link = city_data[i]['href']
                            zipped.append([state, city, link])


        data = []
        for i in range(len(zipped)):
            state = zipped[i][0]
            # city = zipped[i][1]
            url = zipped[i][2]
            print(url)

            no_of_ping += 1
            limit = 0
            while True:
              try :
                time.sleep(2)
                driver.get(url)
                time.sleep(20)
                # jobs=driver.find_element_by_class_name("sortAndH1Cont")
                soup = BeautifulSoup(driver.page_source, 'lxml')
                jobs = soup.find('span', class_ = 'fleft count-string mr-5 fs12')
                # jobs=driver.find_element_by_xpath('//span[@class="fleft count-string mr-5 fs12"]')
                time.sleep(2)
                job=re.findall(r'-?\d+\.?\d*',jobs.text.replace(',','')).pop()

                break
              except:
                driver.get('data:,')
                limit += 1
                if(limit>3):
                   print("Page N "+state)
                   break
                time.sleep(2)

            num =float(job)
            print(num)
            # data.append([state, city, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])
            data.append([state, num, today.date(), pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))])


        NAUKRI_TOTAL_JOBS_STATE_DATA = pd.DataFrame(data, columns=['State', 'Number', 'Relevant_Date', 'Runtime'])
        NAUKRI_TOTAL_JOBS_STATE_DATA = NAUKRI_TOTAL_JOBS_STATE_DATA.drop_duplicates(['State', 'Number', 'Relevant_Date'])

        max_relevant_date = pd.read_sql("select max(Relevant_Date) as Max from AdqvestDB.NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA",engine)["Max"][0]
        if max_relevant_date == today.date():
            print('Data Collected')
        else:
             NAUKRI_TOTAL_JOBS_STATE_DATA.to_sql(name='NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA',con=engine,if_exists='append',index=False)
             click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA")
             click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
             query = 'select * from AdqvestDB.NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
             df = pd.read_sql(query,engine)
             client.execute("INSERT INTO NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA VALUES", df.values.tolist())

             # log.check_data("NAUKRI_TOTAL_JOBS_STATE_WISE_DAILY_DATA", NAUKRI_TOTAL_JOBS_STATE_DATA.shape[0],thresh = 0.7)
        log.job_end_log(table_name, job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
