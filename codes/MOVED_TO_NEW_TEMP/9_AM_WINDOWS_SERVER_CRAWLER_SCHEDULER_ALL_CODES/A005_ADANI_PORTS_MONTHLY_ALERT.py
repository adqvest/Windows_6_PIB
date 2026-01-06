import datetime as datetime
import os
import re
import sys
import time
import warnings
import pandas as pd
import requests
from pytz import timezone

import requests
from bs4 import BeautifulSoup
from dateutil import parser
warnings.filterwarnings('ignore')
import numpy as np
import camelot
from dateutil.relativedelta import *
import boto3
from botocore.config import Config
from calendar import monthrange
from requests_html import HTMLSession

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
import time
from dateutil.relativedelta import relativedelta

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

from adqvest_robotstxt import Robots
robot = Robots(__file__)

import adqvest_db
import JobLogNew as log
import adqvest_s3
import ClickHouse_db

def end_date(date):
    end_dt = datetime.datetime(date.year, date.month, monthrange(date.year, date.month)[1]).date()
    return end_dt

def get_data(from_date,to_date):
    year_from = str(from_date.strftime('%Y'))
    month_from = str(from_date.strftime('%b'))
    day_from = str(from_date.strftime('%d')).lstrip('0')

    year_to = str(to_date.strftime('%Y'))
    month_to = str(to_date.strftime('%b'))
    day_to = str(to_date.strftime('%d')).lstrip('0')

    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"


    prefs = {
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

    driver.get("https://www.bseindia.com/stock-share-price/adani-ports-and-special-economic-zone-ltd/adaniports/532921/corp-announcements/")
    robot.add_link("https://www.bseindia.com/stock-share-price/adani-ports-and-special-economic-zone-ltd/adaniports/532921/corp-announcements/")
    driver.implicitly_wait(10)

    driver.execute_script("window.scrollTo(0, window.scrollY + 200)")
    #----inputing from_date and to_date
    date=driver.find_element(By.XPATH,"//*[@id='txtFromDt']")
    time.sleep(5)
    date.click()

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_from)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_from)
    time.sleep(2)


    from_day = driver.find_element("xpath","//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_from+"']")
    from_day.click()
    time.sleep(5)

    #---to date--

    date=driver.find_element(By.XPATH,"//*[@id='txtToDt']")
    time.sleep(5)
    date.click()


    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_to)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_to)
    time.sleep(2)


    to_day = driver.find_element("xpath","//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_to+"']")
    to_day.click()
    time.sleep(5)



    elem = driver.find_element("xpath",'//*[@class="btn btn-default"]')
    elem.click()
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html')


    return driver

def process_data(soup):
    soup=BeautifulSoup(soup)

    page2=[i.text for i in soup.find_all('td',class_="tdcolumngrey ng-binding") if 'Announcement under Regulation 30 (LODR)-Press Release / Media Release'.lower() in i.text.lower()]
    page3=[i.text for i in soup.find_all('td',class_="tdcolumngrey ng-binding") if 'Monthly Business Updates'.lower() in i.text.lower()]
    page_4=[i.text for i in soup.find_all('span',class_="ng-binding") if 'Operational Performance update'.lower() in i.text.lower()]
    
    if len(page2)>0:
        raise Exception("Adani Ports new data has come")
    elif len(page3)>0:
        raise Exception("Adani Ports new data has come")

    elif len(page_4)>0:
        raise Exception("Adani Ports new data has come")
        


    # text_data=[]
    # links_data=[]
    # dates_data=[]

    # dates=soup.find_all('b',{'class':'ng-binding'})
    # if dates!=[]:

    #     dates=soup.find_all('b',{'class':'ng-binding'})[2:-3]
    #     for i in range(0,len(dates),3):
    #         print(dates[i].get_text())
    #         dates_data.append(dates[i].get_text().strip())

    #     text=soup.find_all('a',{'class':'ng-binding'})[1:]
    #     # print(text)
    #     for i in text:
    #         text_data.append(i.get_text())


    #     link=soup.find_all('a',{'class':'tablebluelink'})[1:]

    #     for i in link:
    #         links_data.append(i['href'])


    #     pdf=[]

    #     for text,link, date in zip(text_data,links_data, dates_data):
    #         if 'Announcement under Regulation 30 (LODR)-Press Release / Media Release' in text:
    #             company=text.split("-")[0]
    #             print(company)
    #             pdf.append(link.split('/')[-1])
    #             date=str(parser.parse(date,dayfirst=True).date())
    #             print(date)
    #             break
    #         elif 'APSEZL - Operational Performance Update' in text:


    #             pdf.append(link.split('/')[-1])
    #             date=str(parser.parse(date,dayfirst=True).date())
    #             print(date)

    #             break



    # if len(pdf)!=0:
    #     raise Exception("Adani Ports new data has come")

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
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
    table_name = 'BSE_PUBLISHED_RANDOM_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date=pd.read_sql("SELECT max(Relevant_Date) as Max FROM AdqvestDB.BSE_PUBLISHED_RANDOM_DATA where Category='Throughput'",engine)['Max'][0]
        max_rel_date=end_date(max_rel_date)+relativedelta(months=1)
        if max_rel_date>today.date():
            pass
        else:
            
            from_date=max_rel_date
            to_date=today.date()

            driver=get_data(from_date,to_date)
            process_data(driver.page_source)

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
