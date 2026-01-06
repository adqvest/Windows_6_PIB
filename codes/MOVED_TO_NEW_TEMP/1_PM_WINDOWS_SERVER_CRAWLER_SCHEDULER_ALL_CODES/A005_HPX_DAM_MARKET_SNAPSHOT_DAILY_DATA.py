"""
@author: Rahul

"""

import requests
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions
import lxml
import pymysql
import sys
import sqlalchemy
from urllib.request import urlopen as uReq
from pandas.io import sql
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import adqvest_db
import JobLogNew as log
import ClickHouse_db

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()

    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days


    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HPX_DAM_MARKET_SNAPSHOT_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date = pd.read_sql('SELECT MAX(Relevant_Date) as max_date FROM HPX_DAM_MARKET_SNAPSHOT_DAILY_DATA', con=engine)['max_date'][0]
        print(max_rel_date)

        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\\Users\\Administrator\\Junk"


        st_date = max_rel_date + days
        final_df = pd.DataFrame()
        prefs = {
            # "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
        driver.maximize_window()

        driver.get("https://www.hpxindia.com/MarketDepth/DAM/DAM_market_snapshot.html")
        robot.add_link("https://www.hpxindia.com/MarketDepth/DAM/DAM_market_snapshot.html")
        driver.implicitly_wait(10)
        driver.find_element("xpath",'//*[@id="3"]').click()
        time.sleep(2)
        sel = Select(driver.find_element("xpath",'//*[@id="ddldelper"]'))
        time.sleep(2)
        sel.select_by_visible_text("Select Range")
        while st_date < today.date():
            time.sleep(2)
            driver.find_element('xpath', '//input[@id="startdate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            time.sleep(2)
            driver.find_element('xpath', '//input[@id="enddate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            time.sleep(2)
            elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
            elem.click()
            time.sleep(5)
        
            tables = pd.read_html(driver.page_source)
            df = tables[5]
            if df.empty == False:
                df.columns=['Date','PURCHASE_BID_MWH','SELL_BID_MWH','MCV_MWH','CLEARED_VOLUME_MWH','VOLUME_LOSS_REAL_TIME_CURTAILMENT_MWH','FINAL_SCHEDULED_VOLUME_MWH','MCP_RS_PER_MWH']
                df = df.dropna(how = 'all', axis = 1)
                df['Relevant_Date'] = pd.to_datetime(df.Date, dayfirst=True).dt.date
                df['Runtime']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                final_df = pd.concat([final_df, df])
                            
                final_df.columns = ['Date','PURCHASE_BID_MWH','SELL_BID_MWH','MCV_MWH','CLEARED_VOLUME_MWH','VOLUME_LOSS_REAL_TIME_CURTAILMENT_MWH','FINAL_SCHEDULED_VOLUME_MWH','MCP_RS_PER_MWH', 'Relevant_Date', 'Runtime']
                final_df = final_df.drop_duplicates()
                final_df = final_df.drop('Date', axis = 1)
                final_df.to_sql(name='HPX_DAM_MARKET_SNAPSHOT_DAILY_DATA',con=engine,if_exists='append',index=False)
            else:
                print('No New Data')
            st_date = st_date + days


        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
             driver.quit()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
