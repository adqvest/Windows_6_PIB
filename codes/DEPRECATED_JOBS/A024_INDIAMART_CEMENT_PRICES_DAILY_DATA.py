import pandas as pd
from bs4 import BeautifulSoup
import sqlalchemy
import datetime as datetime
import warnings
warnings.filterwarnings("ignore")
import os
import requests
import re
import numpy as np
from pytz import timezone
import time
import sys

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from selenium.common.exceptions import ElementNotVisibleException, WebDriverException, NoSuchElementException
import glob
from requests_html import AsyncHTMLSession
from requests_html import HTMLSession
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'INDIAMART_CEMENT_PRICE_DAILY_DATA_STOPPED'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        city = ['Mumbai', 'Delhi', 'Kolkata', 'Chennai', 'Hyderabad', 'Bengaluru', 'Pune', 'Jaipur', 'Patna', 'Guwahati','Goa', 'Chandigarh', 'Kochi', 'Bhubaneswar', 'Lucknow']
        item = np.arange(1, 40)
        item = [str(i) for i in item]

        for i in range(len(city)):

            option = Options()
            option.add_argument("--disable-infobars")
            option.add_argument("start-maximized")
            option.add_argument("--disable-extensions")
            option.add_argument("--disable-notifications")
            option.add_argument('--ignore-certificate-errors')
            # option.add_argument('--no-sandbox')
            option.add_argument('--incognito')
            # option.add_argument('--disable-dev-shm-usage')

            chrome_driver_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=chrome_driver_path,options = option)
            driver.implicitly_wait(30)

            driver.get('https://dir.indiamart.com/' + city[i].lower() + '/construction-cement.html')
            print('https://dir.indiamart.com/' + city[i].lower() + '/construction-cement.html')
            no_of_ping+=1
            time.sleep(5)
            item = np.arange(1, 40)
            item = [str(i) for i in item]

            title = []
            price = []
            details = []
            uom = []
            for row in item:
                try:
                    par_ele = driver.find_element(By.XPATH, '//*[@id="LST' + row + '"]')
                    title.append(par_ele.find_element(By.XPATH, './div[1]/div[1]/div[2]/span/span').text)
                    print(title)
                except NoSuchElementException:
                    title.append('NA')
                    pass
                try:
                    price.append(
                        driver.find_element(By.XPATH, '//*[@id="' + row + 'prcenq"]/span').text.split('/')[0].strip('Rs '))
                except NoSuchElementException:
                    price.append('NA')
                    pass
                try:
                    details.append(par_ele.find_element(By.XPATH, './div[1]/div[1]/div[2]/div').text)
                except NoSuchElementException:
                    details.append('NA')
                    pass
                try:
                    uom.append(driver.find_element(By.XPATH, '//*[@id="' + row + 'prcenq"]/span/span[2]').text)
                except NoSuchElementException:
                    uom.append('NA')
                    pass

                title_df = pd.DataFrame({'Title': title, 'Price': price, 'Details': details, 'UOM': uom})
            # title_df=title_df.apply(lambda x: x.replace('NA',np.nan))
            # title_df=title_df.dropna(how='all')

            title_df['Brand'] = title_df['Details'].apply(lambda x: x.split('\n'))
            title_df['Brand'] = [i.group(1) if i else None for i in
                                 title_df['Brand'].apply(lambda x: re.match('Brand:\s(.*)', x[0]))]
            title_df['Brand'] = [
                title_df['Title'].iloc[i] if title_df['Brand'].iloc[i] is None else title_df['Brand'].iloc[i] for i in
                range(title_df['Title'].shape[0])]

            title_df['Grade'] = title_df['Details'].apply(lambda x: x.split('\n'))
            r = re.compile('Cement Grade:(.*)')
            title_df['Grade'] = [i[0] if i else None for i in title_df['Grade'].apply(lambda x: list(filter(r.match, x)))]
            title_df['Grade'] = [i.split(':')[1] if i is not None else None for i in title_df['Grade']]

            title_df['Type'] = title_df['Details'].apply(lambda x: x.split('\n'))
            r = re.compile('Type:(.*)')
            title_df['Type'] = [i[0] if i else None for i in title_df['Type'].apply(lambda x: list(filter(r.match, x)))]
            title_df['Type'] = [i.split(':')[1] if i is not None else None for i in title_df['Type']]

            title_df['Grade1'] = [
                title_df['Grade'].iloc[i] if title_df['Type'].iloc[i] is None else title_df['Type'].iloc[i] for i in
                range(title_df['Details'].shape[0])]

            # title_df['Price']=title_df['Price'].apply(lambda x : x.split('/')[0].strip('Get Latest Price'))
            # title_df['Price']=title_df['Price'].apply(lambda x:x.strip('Rs '))

            title_df.drop(['Grade', 'Details', 'Title'], axis=1, inplace=True)
            title_df = title_df.apply(lambda x: x.replace('NA', np.nan))
            title_df = title_df.dropna(subset=['Brand', 'Price'])
            title_df = title_df.apply(lambda x: x.str.title())
            title_df = title_df.rename(columns={'Grade1': 'Grade'})
            title_df['Source'] = 'IndiaMart'
            title_df['Relevant_Date'] = today.date()
            title_df['Location'] = city[i]
            title_df['Runtime'] = datetime.datetime.now()
            title_df = title_df[['Source', 'Brand', 'Grade', 'Location', 'Price', 'UOM', 'Relevant_Date', 'Runtime']]
            title_df = title_df.reset_index(drop=True)
            driver.quit()
            print(title_df.head())
            engine = adqvest_db.db_conn()
            title_df.to_sql("INDIAMART_CEMENT_PRICE_DAILY_DATA_STOPPED", con=engine, if_exists='append', index=False)
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
