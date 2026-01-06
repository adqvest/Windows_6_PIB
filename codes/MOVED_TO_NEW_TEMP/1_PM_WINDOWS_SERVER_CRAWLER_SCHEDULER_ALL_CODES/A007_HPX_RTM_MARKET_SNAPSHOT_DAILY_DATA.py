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
    table_name = 'HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date = pd.read_sql('SELECT MAX(Relevant_Date) as max_date FROM HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA', con=engine)['max_date'][0]
        print(max_rel_date)

        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\\Users\\Administrator\\Junk"


        prefs = {
            "download.default_directory": download_file_path,
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

        driver.get("https://www.hpxindia.com/MarketDepth/RTM/rtm_marketsnapshot.html")
        robot.add_link("https://www.hpxindia.com/MarketDepth/RTM/rtm_marketsnapshot.html")
        driver.implicitly_wait(10)
        driver.find_element("xpath",'//*[@id="3"]').click()
        time.sleep(2)
        sel = Select(driver.find_element("xpath",'//*[@id="ddldelper"]'))
        time.sleep(2)
        sel.select_by_visible_text("Last 10 days")
        time.sleep(2)
        elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
        elem.click()
        time.sleep(2)
        elem2= driver.find_element("xpath",'//*[@id="hideNext3"]')
        elem2.click()

        soup = BeautifulSoup(driver.page_source, 'html')
        dates=soup.findAll('td',{'class':'tdcolumntop tdcolumn_center','ng-repeat':'column in row'})
        dates_list=[]
        for date in dates[-5:]:
            text=date.text.strip()
            text=datetime.datetime.strptime(text, '%d/%m/%Y').date()
            print(text)
            dates_list.append(text)

        values_soup=soup.findAll('td',{'class':'tdcolumntop tdcolumn_right','ng-repeat':'column in row'})
        values_list=[]
        for i in values_soup:
            value=i.get_text().strip()
            values_list.append(value)
        df_values = pd.DataFrame()
        for n in range(0, len(values_list), 7):
            df_values = pd.concat([df_values, pd.DataFrame(values_list[n:n+7]).T])

        df_values.columns=['PURCHASE_BID_MWH', 'SELL_BID_MWH', 'MCV_MWH', 'CLEARED_VOLUME_MWH',
           'VOLUME_LOSS_REAL_TIME_CURTAILMENT_MWH', 'FINAL_SCHEDULED_VOLUME_MWH',
           'MCP_INR_PER_MWH']

        df_values['Relevant_Date']=dates_list
        df_values['Runtime']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df_final=df_values[df_values['Relevant_Date']>max_rel_date]
        print(df_final)
        today=today.date()
        df_final=df_final[df_final['Relevant_Date']<today]
        if df_final.shape[0]==0:
            print("No new data came")
        else:
            df_final.to_sql(name='HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA',con=engine,if_exists='append',index=False)
        # click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA")
        # print(click_max_date)
        # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        # query = 'select * from AdqvestDB.HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA WHERE Relevant_Date > "' + click_max_date + '"'
        # df = pd.read_sql(query,engine)
        # client.execute("INSERT INTO HPX_RTM_MARKET_SNAPSHOT_DAILY_DATA VALUES",df.values.tolist())
        # print("Data inserted successfully!")

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
