

import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import os
from pytz import timezone
import datetime
import re
import sys
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.common.by import By

import warnings
from selenium import webdriver
warnings.filterwarnings('ignore')
import timeit

from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import time

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
from selenium.webdriver.common.keys import Keys

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
engine = adqvest_db.db_conn()


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir(r'C:\Users\sasai\jupyter notebook\ADQVest Capital\Important DB Conn')
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BSE ANNUAL REPORT'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:

        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)



        data_security_code=pd.read_sql("select ISIN,Security_Code,Security_Name, sum(Face_Value) from BSE_LIST_OF_SCRIPS_DAILY_DATA where Relevant_Date>='2024-05-01' and Status='Active' and (Instrument='Equity' or Instrument='Equity T+1') group by ISIN  order by sum(Face_Value) desc limit 2500",engine)

        data_security_code['ISIN']=data_security_code['ISIN'].astype(str)
        data_security_code['Security_Code']=data_security_code['Security_Code'].astype(int)
        data_security_code['Security_Code']=data_security_code['Security_Code'].astype(str)
        data_security_code['Security_Code']=data_security_code['Security_Code'].str.strip()

        data_security_code['Status']=''

        companies=data_security_code['Security_Code'][600:1500]
        company_names=data_security_code['Security_Name'][600:1500]
       
        company_names=company_names.reset_index(drop=True)
        df_final_pdf=pd.DataFrame()
        loop=0
        for i,company in enumerate(companies):
            company=company.split("(")[0].strip()
            print(company)
            
            chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

            download_file_path = r"C:\Users\Administrator\AdQvestDir\BSE REPORTS"

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

            driver.get("https://www.bseindia.com/corporates/ann.html")

            time.sleep(2)
            driver.refresh()
            wait = WebDriverWait(driver, 10)  # Wait up to 10 seconds
            input_field = driver.find_element(By.XPATH, "//*[@id='getquotesearch']")
            input_field.clear()
            time.sleep(2)
            print("Search input field found")
            input_field.send_keys(company[:3]) 
            time.sleep(2)
            input_field.send_keys(company[3:5])
            time.sleep(10)
            input_field.send_keys(company[5:]) 
            time.sleep(10)
            wait = WebDriverWait(driver, 10)
            input_field.send_keys(Keys.ENTER)
            time.sleep(5)
            # Wait for the search results to appear
            wait = WebDriverWait(driver, 10)
            search_results_ul = driver.find_element(By.XPATH, "//*[@id='ulSearchQuote']")
            wait = WebDriverWait(driver, 10)
            # Get all list items (li) within the unordered list
            
            # Wait for the page to load
            time.sleep(3)

            # Find the dropdown caret icon and click on it
            driver.execute_script("window.scrollBy(0, 100);")
            try:
                time.sleep(2)
                try:
                    caret_icon = driver.find_element(By.XPATH, "/html/body/div[1]/div[4]/div[5]/div[7]/aside/div/div[2]/div[1]/h1/a")
                    driver.execute_script("arguments[0].scrollIntoView(true);", caret_icon)
                    caret_icon.click()
                except:
                    time.sleep(2)
                    caret_icon = driver.find_element(By.XPATH, "/html/body/div[1]/div[4]/div[5]/div[7]/aside/div/div[2]/div[1]/h1/a")
                    driver.execute_script("arguments[0].scrollIntoView(true);", caret_icon)
                    caret_icon.click()

                # Wait for the dropdown menu to appear
                time.sleep(2)
                driver.execute_script("window.scrollBy(0, 250);")
                time.sleep(2)
                # Find the 'Annual Reports' link and click on it
                annual_reports_link = driver.find_element(By.XPATH, "/html/body/div[1]/div[4]/div[5]/div[7]/aside/div/div[2]/div[2]/div/div[2]/a")
                annual_reports_link.click()

                # Wait for the page to load
                time.sleep(3)

                # Now you can extract the source of the page or perform any other actions you need
                page_source = driver.page_source
                tables=pd.read_html(page_source)
                df=tables[len(tables)-1]
                links=[]
                text=[]
                time.sleep(2)
                main_soup = BeautifulSoup(page_source,'html')
                l1 = main_soup.findAll("a")
                for tag in l1:
                    try:
                        if ("Attach" in tag.get("href")) or ("AnnualReport" in tag.get("href")) or ("HIS_ANN_" in tag.get("href")):
                            link = tag.get("href")
                            links.append(link)
                            text.append(tag.text)
                    except:
                        continue
                driver.quit()
                time.sleep(3)
                df['Links']=links
                df['Company']=company_names[i]
                df['Code']=company
                df['Filing Date Time'] = pd.to_datetime(df['Filing Date Time'], format='%d-%m-%Y %I:%M:%S %p', errors='coerce')
                df['Relevant_Date'] = df.apply(
                    lambda row: row['Filing Date Time'].date() if not pd.isnull(row['Filing Date Time']) else pd.to_datetime(f"{row['Year']}-03-31").date(),
                    axis=1
                )
                df['Company'] = df['Company'].str.replace(r'\(\d+\)', '')
                df['Company'] = df['Company'].str.replace('.', '')
                df['Company'] = df['Company'].str.replace('$', '')
                df['Company'] = df['Company'].str.replace('-', '')
                df['Runtime'] = pd.to_datetime('now')
                print(df.head())
                df.to_sql('BSE_ANNUAL_REPORT_Temp_Kama2',index=False, if_exists='append', con=engine)
                data_security_code['Status']=np.where(data_security_code['Security_Code']==company,'Done',data_security_code['Status'])
                loop+=1
                print('DONE FOR FOLLOWING COMPANIES-------------------->',loop)
                time.sleep(5)
            except:
                driver.quit()
                time.sleep(3)
                data_security_code['Status']=np.where(data_security_code['Security_Code']==company,'Not Done',data_security_code['Status'])
            data_security_code.to_csv('C:\\Users\\Administrator\\AdQvestDir\\BSE REPORTS\\bse_security_code.csv')
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        # error_msg = str(sys.exc_info()[1])
        error_msg = 1
        print(error_msg)
        
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if (__name__ == '__main__'):
    run_program(run_by='manual')



