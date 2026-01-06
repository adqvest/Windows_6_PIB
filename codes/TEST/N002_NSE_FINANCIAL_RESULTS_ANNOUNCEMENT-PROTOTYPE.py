
import numpy as np
import pandas as pd
# import adqvest_db
import sys
import os
from pytz import timezone
import datetime as dt

import re

import time
from time import sleep
# from dateutil import parser

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.options.display.max_columns = None
pd.options.display.max_rows = None

import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import ClickHouse_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()

    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = dt.datetime.now(india_time)
    days       = dt.timedelta(1)
    yesterday  = today - days

    run_time = pd.to_datetime(today.strftime("%Y/%m/%d %H:%M:%S"))
    ## job log details
    job_start_time = dt.datetime.now(india_time)
    table_name = 'NSE_FINANCIAL_RESULTS_ANNOUNCEMENT_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
        


        # In[3]:


        keys_to_send='investor presentation'
        keys_to_send[:6]


        # In[50]:


        def get_investor_presentation(driver):
            wait = WebDriverWait(driver, 10)
            input_field = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[11]/div[1]/div/section/div/div/div[2]/div/div[3]/div[1]/div[1]/div[2]/div/span/input')))
            input_field.clear()
            time.sleep(5)
            keys_to_send='investor presentation'
            input_field.send_keys(keys_to_send[:6])  # Replace COMPANY_NAME with the actual company name
            time.sleep(2)
            input_field.send_keys(keys_to_send[6:12])  # Replace COMPANY_NAME with the actual company name
            time.sleep(10)
            input_field.send_keys(keys_to_send[12:])  # Replace COMPANY_NAME with the actual company name
            time.sleep(10)
            input_field.send_keys(Keys.RETURN) 
            input_field.send_keys(Keys.ENTER)
            time.sleep(2)
            element = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[11]/div[1]/div/section/div/div/div[2]/div/div[3]/div[1]/div[1]/div[2]/div/span/div/div/div')))
            time.sleep(10)
            element.click()
            time.sleep(20)
            return driver


        # In[34]:


        def click_year_button_to(driver,wait,target_year):
            current_year_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[19]/div/div/div[2]')))
            current_year = int(current_year_element.text.strip()) 
            print(current_year)
            while current_year > int(target_year):
                
                left_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[19]/div/div/div[1]/i')))
                left_button.click()
                current_year_element = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[19]/div/div/div[2]')))
                current_year = int(current_year_element.text.strip())
            time.sleep(2)
            


        # In[35]:


        def click_year_button_from(driver,wait,target_year):
            current_year_element = wait.until(EC.presence_of_element_located((By.XPATH, '//div[@data-role="period" and @role="button"]')))
            current_year = int(current_year_element.text.strip()) 
            print(current_year)
            while current_year > int(target_year):
                
                left_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[18]/div/div/div[1]/i')))
                left_button.click()
                current_year_element = wait.until(EC.presence_of_element_located((By.XPATH, '//div[@data-role="period" and @role="button"]')))
                current_year = int(current_year_element.text.strip())
            time.sleep(2)


        # In[36]:


        def get_month_th(driver, wait, month_name):

            months_chunks = [['jan', 'feb', 'mar', 'apr'], ['may', 'jun', 'jul', 'aug'], ['sep', 'oct', 'nov', 'dec']]
            for idx, chunk in enumerate(months_chunks, start=1):
                if month_name.lower() in chunk:
                    return (idx - 1) * 4 + chunk.index(month_name.lower()) + 1
            return 1


        # In[37]:


        def get_month_tr_th(drive,wait, month_name):
            tr_index = 1 if month_name.lower() in ['jan', 'feb', 'mar', 'apr'] else (2 if month_name.lower() in ['may', 'jun', 'jul', 'aug'] else 3)
            th_index = get_month_th(driver, wait, month_name)
            return tr_index,th_index


        # In[38]:


        def determine_range(to_date):
            return (to_date) // 7 + 1

        def get_date_tr(to_date):
            range_index = determine_range(to_date)
            print(f'The date {to_date} falls into range {range_index}.')
            return range_index
        def get_date_th(day):
            if day < 7:
                return day + 1  
            elif day < 14:
                return day - 6  
            elif day < 21:
                return day - 13 
            elif day < 28:
                return day - 20  
            else:
                return day - 27


        # In[39]:


        def click_month_button(driver,type_date,wait, month_name):
            tr_index,th_index=get_month_tr_th(driver,wait, month_name)
            if type_date=='from':
                month_button_xpath = f'/html/body/div[18]/div/table/tbody/tr[{tr_index}]/td[{th_index}]'
                print(month_button_xpath)
                time.sleep(2)
                wait.until(EC.element_to_be_clickable((By.XPATH, month_button_xpath))).click()
            else:
                month_button_xpath = f'/html/body/div[19]/div/table/tbody/tr[{tr_index}]/td[{th_index}]'
                print(month_button_xpath)
                time.sleep(2)
                wait.until(EC.element_to_be_clickable((By.XPATH, month_button_xpath))).click()


        # In[40]:


        def click_date_button(driver,type_date,wait,date):
            if type_date=='from':
                date_button = wait.until(EC.element_to_be_clickable((By.XPATH, f'//td[@class="current-month gj-cursor-pointer" and @day="{date}"]/div[@role="button"]')))
                date_button.click()
                time.sleep(2)
            else:
                time.sleep(3)
                complete_xpath = f'/html/body/div[19]/div/table/tbody/tr/td[@day="{date}"]'

                # Find the element using the complete XPath
                day_30_element = driver.find_element(By.XPATH, complete_xpath)
                time.sleep(2)
                day_30_element.click()


        # In[45]:


        def driver_pass_dates(driver):
            wait = WebDriverWait(driver, 10)
            custom_dates = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[11]/div[1]/div/section/div/div/div[2]/div/div[3]/div[1]/div[2]/div/div/div/div/ul/li[7]/a')))
            custom_dates.click()
            time.sleep(15)
            datepicker_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[11]/div[1]/div/section/div/div/div[2]/div/div[3]/div[1]/div[3]/div/div/div/div/div/div/div[1]/div/input')))
            datepicker_button.click()
            time.sleep(3)
            year_range_element_from = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[18]/div/div/div[2]')))
            year_range_element_from.click()
            target_year=2019
            month_name='Jan'
            date=1
            click_year_button_from(driver,wait,target_year)
            click_month_button(driver,'from',wait, month_name)
            click_date_button(driver,'from',wait,date)
            datepicker_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[11]/div[1]/div/section/div/div/div[2]/div/div[3]/div[1]/div[3]/div/div/div/div/div/div/div[2]/div/input')))
            datepicker_button.click()
            time.sleep(3)
            year_range_element_to=wait.until(EC.element_to_be_clickable((By.XPATH,'/html/body/div[19]/div/div/div[2]')))
            year_range_element_to.click()
            target_year=2019
            to_month='Mar'
            to_date=31
            click_year_button_to(driver,wait,target_year)
            time.sleep(5)
            click_month_button(driver,'to',wait, to_month)
            time.sleep(5)
            click_date_button(driver,'to',wait,to_date)
            

            return driver
            

        failed_company = []
        # Setup WebDriver
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        driver = webdriver.Chrome(executable_path=driver_path)
        driver.maximize_window()

        try:
            driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
            wait = WebDriverWait(driver, 10)
            time.sleep(10)
            driver.quit()
            driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=driver_path)
            driver.maximize_window()
            driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
            wait = WebDriverWait(driver, 10)
            time.sleep(10)
            driver=get_investor_presentation(driver)
        except:
            driver.quit()
            driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=driver_path)
            driver.maximize_window()
            driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
            wait = WebDriverWait(driver, 10)
            time.sleep(10)
            driver=get_investor_presentation(driver)
        driver=driver_pass_dates(driver)
        wait = WebDriverWait(driver, 10)
        time.sleep(5)
        go_button=wait.until(EC.element_to_be_clickable((By.XPATH,'//*[@id="Announcements_equity"]/div[3]/div/div/div/div/div/div/div[3]/button')))
        go_button.click()
        time.sleep(2)

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if (__name__ == '__main__'):
    run_program(run_by='manual')
# In[ ]:




