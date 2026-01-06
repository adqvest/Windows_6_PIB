
import re
import sys
import ast
import csv
import time
import glob
import json
import random
import zipfile
import calendar
import warnings
import requests
import sqlalchemy
import os
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
warnings.filterwarnings('ignore')
import adqvest_db
# import adqvest_s3
# import boto3
# import ClickHouse_db

import numpy as np
import pandas as pd
import datetime as datetime

from time import sleep
from pandas.io import sql
from pytz import timezone
from dateutil import parser
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.select import Select
import requests.packages.urllib3.util.ssl_
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'

import JobLogNew as log
import Cleaner as cleaner
import adqvest_db
import ClickHouse_db
pd.options.display.max_columns = None
pd.options.display.max_rows = None


os.chdir('C:/Users/Administrator/AdQvestDir')
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

#####   Date Time #####

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

run_time = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

##### Job Log Details #####

job_start_time = datetime.datetime.now(india_time)
table_name = 'NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA'
scheduler = '12_PM_WINDOWS_SERVER_CRAWLER_SCHEDULER_ALL_CODES'
no_of_ping = 0

############ data collection starts ############

def find_value1(links):
    try:
        values=links.find('h1')
        value=str(values).split('>')
        value=value[1].split('<')
        value=value[0]
        value=value.title()
        value=value.strip()
    except:
        value=None
    return value

def find_value2(links):
    values=links.find('small')
    values=str(values)
    try:
       values=re.findall(r'(\d+)', values)
       values=values[len(values)-1]
    except:
       values=re.findall(r'(\d+)', links.find('small').text)
       values=values[len(values)-1]
    
    return values

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_date= pd.read_sql("select max(Relevant_Date) as Max from NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA", engine)
        last_rel_date = max_date["Max"][0]
        today=pd.to_datetime('now')
        if(today.date()-last_rel_date >= datetime.timedelta(1)):
            print('Collecting new data')
            url='https://www.nykaafashion.com/mega-menu-shop?path=All%20Brands'

            driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

            prefs = {
                # "download.default_directory": download_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
                }
            count=0
            while (count<5):
               
                options = webdriver.ChromeOptions()
                options.add_argument("--disable-infobars")
                options.add_argument("start-maximized")
                options.add_argument("--disable-extensions")
                options.add_argument('--incognito')
                options.add_argument("--disable-notifications")
                options.add_argument('--ignore-certificate-errors')
                options.add_argument("--use-fake-ui-for-media-stream")
                options.add_argument("--use-fake-device-for-media-stream")


                options.add_experimental_option('prefs', prefs)
                driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
                driver.maximize_window()
                driver.get("https://www.nykaafashion.com/all-brands?root=topnav_1")

                time.sleep(10)
                elem1 = driver.find_element(By.XPATH,'/html/body/div[1]/div/div[1]/div/div/div/div[1]/div/div[1]')
                time.sleep(20)
                elem1.click()
                time.sleep(10)
                html_source = driver.page_source

                html_soup: BeautifulSoup = BeautifulSoup(html_source, 'html.parser')
                elements=html_soup.find_all('a')
                driver.quit()
                if len(elements)!=0:
                                 break

                if (count==5 and len(brand_page3)==0):
                    log.job_end_log(table_name,job_start_time,no_of_ping)
                    
                print(count)
                count+=1

            link_3=[]
            for ele in elements:
                link_3.append(ele.get('href'))

            headers = {
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56',
                        }

            nykaa_fashion_final=pd.DataFrame()
            nykaa_fashion=pd.DataFrame()
            Brands=[]
            Sku_count=[]
            count=0
            for link in link_3:
                if link !=None:
                    if link.lower().find('play.google')==-1:
                        act_link='https://www.nykaafashion.com'+link
                        print(act_link)
                        r=requests.get(act_link,headers=headers)
                        soup = BeautifulSoup(r.content)
                        links = soup.find('header',attrs = {"class":"css-tcvlxq"})
                        if links != None:
                            print(links)
                            brand=find_value1(links)
                            print(brand)
                            sku_count=find_value2(links)
                            print(sku_count)
                            Brands.append(brand)
                            Sku_count.append(sku_count)
                            count+=1
                            print('Done for '+str(count)+' of brands')
                            time.sleep(2)
                        else:
                            print(links)
                            print('This is not expected link')
                    else:
                        print('Done')
                        break
                else:
                    print('None type link-------->',link)

            nykaa_fashion['Brand']=Brands
            nykaa_fashion['Sku_count']=Sku_count
            nykaa_fashion['Relevant_Date']=pd.to_datetime('now').date() 
            nykaa_fashion['Runtime']=pd.to_datetime('now')

            values=['Women','Men','Kids','Home','Gadgets &Amp; Tech Accessories']

            nykaa_fashion = nykaa_fashion[~nykaa_fashion['Brand'].isin(values)]

            nykaa_fashion.to_sql(name='NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA', if_exists="append",index=False, con=engine)
        else:
            print('Data already there')

        ################################ click_house data upload #################################
        
        click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        print(click_max_date)
        # click_min_date='2015-03-31'
        query = 'select * from AdqvestDB.NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
        df = pd.read_sql(query,con=engine)
        client.execute("INSERT INTO NYKAA_FASHION_BRAND_WISE_SKU_COUNT_DAILY_DATA VALUES",df.values.tolist())
        print('Data pushed into clickhouse')
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by = 'manual')
