
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
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

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

import undetected_chromedriver as uc
# from selenium_stealth import stealth
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
from datetime import datetime
from collections import OrderedDict

import adqvest_db
engine = adqvest_db.db_conn()
import JobLogNew as log

india_time = timezone('Asia/Kolkata')
today      = datetime.now(india_time)
# days       = datetime.datetime.timedelta(1)
# yesterday = today - days
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
#    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.now(india_time)
    table_name = "BSE_ANNOUNCEMENT_INVESTOR_PRESENTATION_COMPANY_WISE_YEARLY_DATA"
    scheduler = ''
    no_of_ping = 0
    engine = adqvest_db.db_conn()

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def select_from_date(year, month , day, driver):
            dropdown = Select(driver.find_element(By.CLASS_NAME, 'ui-datepicker-year'))
            print(year, month)
            year_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-year'))
            )
            Select(year_dropdown).select_by_visible_text(year)
            time.sleep(2)
            month_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-month'))
            )
            Select(month_dropdown).select_by_value(str(int(month) - 1))
            time.sleep(2)
            day_xpath = f"//table/tbody/tr/td/a[text()='{int(day)}']"
            day_element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, day_xpath))
            )
            day_element.click()
            print(f"Selected day: {str(int(day))}")


        def select_to_date(year, month , day, driver):
            dropdown = Select(driver.find_element(By.CLASS_NAME, 'ui-datepicker-year'))
            print(year, month)
            year_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-year'))
            )
            Select(year_dropdown).select_by_visible_text(year)
            time.sleep(2)
            month_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-month'))
            )
            Select(month_dropdown).select_by_value(str(int(month) - 1))
            time.sleep(2)
            day_xpath = f"//table/tbody/tr/td/a[text()='{int(day)}']"
            day_element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, day_xpath))
            )
            day_element.click()
            print(f"Selected day: {str(int(day))}")

        def extract_date_from_url(url):
            try:
                # Extract the year and month parts from the URL
                date_part = url.split('/xml-data/corpfiling/CorpAttachment/')[1].split('/')[0:2]
                year, month = date_part
                
                # Create a date string in the format 'YYYY-MM-01'
                date_str = f"{year}-{month}-01"
                
                # Return the formatted date
                return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                return None


        def get_text_link_dictionary(driver):
            result_dict = {}
            time.sleep(3)
            tbodies = driver.find_elements(By.TAG_NAME, 'tbody')
            time.sleep(3)
            len(tbodies)
            for tbody in tbodies:
                try:
                    headline_element = tbody.find_element(By.CSS_SELECTOR, '[ng-bind-html="cann.HEADLINE"]')
                    headline_text = headline_element.text
                    time_element = tbody.find_element(By.CSS_SELECTOR, '[ng-if="cann.TimeDiff"]')
                    time_text = time_element.text
                    try:
                        if 'presentation' in headline_text.lower():
                            print('Presentation is present')
                            link_element = tbody.find_element(By.CLASS_NAME, 'tablebluelink')
                            href_link = link_element.get_attribute('href')
                    except Exception as e:
                        href_link = None
                    final_url = None
                    if href_link!=None:
                        if href_link and 'xml' in href_link:
                            final_url = href_link
                        elif download_url and 'xml' in download_url:
                            final_url = download_url
                        if final_url:
                            result_dict[headline_text] = [final_url, time_text]
                except:
                    continue
                return result_dict
        def click_next():
            try:
                # Wait until the Next button is clickable
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, 'idnext'))
                )
                
                time.sleep(3)
                next_button.click()
                return True 
            except Exception as e:
                return False  

        def go_to_first_page(driver):
            while True:
                current_page_info = driver.find_element(By.XPATH, '//b[contains(text(), "Current Page Number")]').text
                if '1 out of' in current_page_info:
                    break
                else:
                    # Go to the previous page
                    prev_page_button = driver.find_element(By.ID, 'idprev')  # Replace with the actual prev page button ID or XPath
                    prev_page_button.click()
                    time.sleep(3)
        def get_disseminated_time(entry):
            details = entry[1]  # The second item in the list contains the details
            disseminated_time_str = details.split('Exchange Disseminated Time ')[1].split(' Time Taken')[0].strip()
            disseminated_time = datetime.strptime(disseminated_time_str, '%d-%m-%Y %H:%M:%S')
            return disseminated_time

        df1=pd.read_sql("select ISIN_No as ISIN,Security_Id,Security_Code,Security_Name,Company_Name,Status from BSE_COMPANY_LISTING_DAILY_DATA",engine)

        df1['ISIN']=df1['ISIN'].astype(str)
        df1['Security_Code']=df1['Security_Code'].astype(int)
        df1['Security_Code']=df1['Security_Code'].astype(str)
        df1['Security_Code']=df1['Security_Code'].str.strip()
        df1['Status']=''

        today = datetime.today().date()
        date_ranges = OrderedDict()

        for year in range(2020, today.year):
            from_date = f"{year}-01-01"
            to_date = f"{year}-12-31"
            date_ranges[from_date] = to_date

        current_year_from_date = f"{today.year}-01-01"
        date_ranges[current_year_from_date] = str(today)
        sorted_date_ranges=date_ranges



        for i in range(12,20):
            print(df1['Security_Code'][i])
            company_code=df1['Security_Code'][i]
            driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=driver_path)
            driver.maximize_window()

            driver.get("https://www.bseindia.com/corporates/ann.html")
            wait = WebDriverWait(driver, 10) 
            time.sleep(5)
            driver.refresh()
            dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ddlPeriod"))
            )

            # Create a Select object
            select = Select(dropdown)

            # Select the "Company Update" option by visible text
            select.select_by_visible_text("Company Update")
            time.sleep(3)
            input_field = driver.find_element(By.XPATH, '//*[@id="scripsearchtxtbx"]')
            input_field.clear()
            time.sleep(2)
            print("Search input field found")
            input_field.send_keys(company_code[:2])  # Replace COMPANY_NAME with the actual company name
            time.sleep(2)
            input_field.send_keys(company_code[2:5])  # Replace COMPANY_NAME with the actual company name
            time.sleep(5)
            input_field.send_keys(company_code[5:])  # Replace COMPANY_NAME with the actual company name
            time.sleep(5)
            wait = WebDriverWait(driver, 5)
            input_field.send_keys(Keys.ENTER)
            time.sleep(5)
            for from_date, to_date in sorted_date_ranges.items():
                
                time.sleep(5)
                driver.find_element(By.XPATH,'//*[@id="txtFromDt"]').click()
                time.sleep(5)
                year, month , day = from_date.split('-')[0], from_date.split('-')[1], from_date.split('-')[2]
                select_from_date(year, month , day, driver)
                driver.find_element(By.XPATH,'//*[@id="txtToDt"]').click()
                time.sleep(5)
                year, month , day = to_date.split('-')[0], to_date.split('-')[1], to_date.split('-')[2]
                select_to_date(year, month , day, driver)
                driver.find_element(By.XPATH,'//*[@id="btnSubmit"]').click()
                time.sleep(5)
                try:
                    driver.page_source
                    go_to_first_page(driver)
                    result_dict = {}
                    result_dict=get_text_link_dictionary(driver)
                    print(result_dict)
                    while click_next():
                        print("Clicked Next button.")
                        new_results = get_text_link_dictionary(driver)
                        if new_results:
                            result_dict.update(new_results)
                        else:
                            print("No more results found.")
            #                 result_dict={}
                        print(result_dict)
                        time.sleep(5)
                    if result_dict:
                        print('data is present')
                        records = []
                        for description, value in result_dict.items():
                            link = value[0]
                            disseminated_time = get_disseminated_time(value)
                            records.append([description, link, disseminated_time])

                        df = pd.DataFrame(records, columns=['Description', 'Link', 'Broadcast'])
                        df['Security_Code']=company_code
                        df['Company_Name']=df1['Company_Name'][i]
                        df=df[['Security_Code','Company_Name','Description','Link', 'Broadcast']]
                        df['Relevant_Date']=pd.to_datetime('now').date()
                        df['Runtime']=pd.to_datetime('now')
                        df.drop_duplicates(inplace=True)
                        print(df)
                        df.to_sql('BSE_ANNOUNCEMENT_INVESTOR_PRESENTATION_COMPANY_WISE_YEARLY_DATA', con=engine, if_exists='append', index=False)
                        time.sleep(3)
                        query='update BSE_COMPANY_LISTING_DAILY_DATA_STATUS set Investor_Status="DONE" where Security_Code="'+str(company_code)+'"'
                        engine = adqvest_db.db_conn()
                        connection = engine.connect()
                        connection.execute(query)
                        print('DONE FOR FOLLOWING COMPANY COUNT------------>',i)
                        print('DONE FOR COMPANY---------------->',df1['Company_Name'][i])
                    else:
                        continue
                except:
                    print('Code is not valid----> ',company_code)
                    query='update BSE_COMPANY_LISTING_DAILY_DATA_STATUS set Investor_Status="'+str(df1['Status'][i])+'" where Security_Code="'+str(company_code)+'"'
                    engine = adqvest_db.db_conn()
                    connection = engine.connect()
                    connection.execute(query)
                    print('DONE FOR FOLLOWING COMPANY COUNT------------>',i)
                    print('DONE FOR COMPANY---------------->',df1['Company_Name'][i])
                    break       
            driver.quit() 
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)        
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')


