
import pandas as pd
import sqlalchemy
import numpy as np
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import time
import os
import re
import sqlalchemy
import camelot
import numpy as np
from selenium import webdriver
from pandas.core.common import flatten
import calendar
import pandas as pd
import numpy as np
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
import numpy as np

import warnings
warnings.filterwarnings('ignore')

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
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC



def run_program(run_by='Adqvest_Bot', py_file_name=None):
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
    table_name = 'DOSEL_NAS_DIST_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AdQvestDir\codes\One time run"
        # chrome_driver = r"D:\Adqvest\chrome_path\chromedriver.exe"
        # download_file_path = r"D:\Adqvest\Junk"
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
            }

        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
        driver.maximize_window()
        driver.get("https://nas.gov.in/report-card/nas-2021")
        # robot.add_link("https://www.hpxindia.com/MarketDepth/RECMarket/recdata.html")
        # driver.implicitly_wait(10)
        time.sleep(3)
        html_content = driver.page_source

        soup = BeautifulSoup(html_content, 'html.parser')
        print('trying to get data')
        list_buttons = soup.find_all('a', class_='national_state_list')
        state_names = []
        df= pd.DataFrame()
        #class 3
        for button in list_buttons:
            if button.has_attr('onclick'):
                driver.execute_script(button['onclick'])
                time.sleep(2) 

                state_name = button.get_text(strip=True)
                state_name = state_name.replace('radio_button_checked', '').replace('radio_button_unchecked', '').strip()
                state_names.append(state_name)
                html_content2 = driver.page_source
                soup2 = BeautifulSoup(html_content2, 'html.parser')
                district_links = soup.find_all('a', class_='districts')
                
                count=0
                for district in district_links:
                    final_df=pd.DataFrame()
                    if district.has_attr('onclick'):
                        driver.execute_script(district['onclick'])
                        time.sleep(1)
                        district_name = district.get_text(strip=True)
                        driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                        try:
                            class_3_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "class3-tab")))
                            class_3_tab.click()
                            time.sleep(2)
                            learning_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                            learning_tab_button.click()
                            time.sleep(2)
                        except:
                            time.sleep(2)
                            print('except')
                            for sc in range(2):
                                driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                            time.sleep(1)
                            class_3_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "class3-tab")))
                            class_3_tab.click()
                            time.sleep(2)
                            learning_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                            learning_tab_button.click()
                            time.sleep(1)
                        html_content1 = driver.page_source
                        soup1 = BeautifulSoup(html_content1, 'html.parser')
                        span_element = soup1.find('span', class_='nav-link t')
                        if span_element:
                            text = span_element.get_text(strip=True)
                            state_name = text.split(' > ')[0]
                        #for mathematics
                        time.sleep(3)
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_language_district_class3")
                            district_value = district_value_element.text
                        data = {'Subject': ['Language'], 'Overall_Pct': [district_value]}
                        print(district_value)
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        time.sleep(3)
                        #for mathematics
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_math_district_class3")
                            district_value = district_value_element.text
                        data = {'Subject': ['Mathematics'], 'Overall_Pct': [district_value]}
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        #for mathematics
                        time.sleep(3)
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_evs_district_class3")
                            district_value = district_value_element.text
                        data = {'Subject': ['EVS'], 'Overall_Pct': [district_value]}
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        
                        final_df['State_Name'] = final_df['State_Name'].str.replace('(', '').str.strip()
                        final_df['Class']='Class 3'
                        final_df['Relevant_Date']='2021-03-31'
                        final_df['Relevant_Date']=pd.to_datetime(final_df['Relevant_Date'])
                        final_df['Runtime']=pd.to_datetime('now')
                        final_df.columns=['Subject','Overall_Pct','State_Name','District_Name','Class','Relevant_Date','Runtime']
                        final_df=final_df[['Class','State_Name','District_Name','Subject','Overall_Pct','Relevant_Date','Runtime']]
                        final_df.reset_index(drop=True,inplace=True)
                        engine = adqvest_db.db_conn()
                        connection = engine.connect()
                        connection.execute("Delete from DOSEL_NAS_DIST_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME where State_Name='"+state_name+"' and District_Name='"+district_name+"' and Class='"+class_name[i]+"'")
                        connection.execute('commit')
                        time.sleep(3)
                        final_df.to_sql(name='DOSEL_NAS_DIST_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME',con=engine,if_exists='append',index=False)
                        # final_df=final_df.append(df)
                        
            break

        for button in list_buttons:
            if button.has_attr('onclick'):
                driver.execute_script(button['onclick'])
                time.sleep(2) 

                state_name = button.get_text(strip=True)
                state_name = state_name.replace('radio_button_checked', '').replace('radio_button_unchecked', '').strip()
                state_names.append(state_name)
                html_content2 = driver.page_source
                soup2 = BeautifulSoup(html_content2, 'html.parser')
                district_links = soup.find_all('a', class_='districts')
                
                count=0
                for district in district_links:
                    final_df=pd.DataFrame()
                    if district.has_attr('onclick'):
                        driver.execute_script(district['onclick'])
                        time.sleep(1)
                        district_name = district.get_text(strip=True)
                        driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                        try:
                            class_5_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "class5-tab")))
                            class_5_tab.click()
                            time.sleep(2)
                            learning_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                            learning_tab_button.click()
                            time.sleep(2)
                        except:
                            time.sleep(2)
                            print('except')
                            for sc in range(2):
                                driver.execute_script("window.scrollTo(0, -document.body.scrollHeight);")
                            time.sleep(1)
                            class_5_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "class5-tab")))
                            class_5_tab.click()
                            time.sleep(2)
                            learning_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                            learning_tab_button.click()
                            time.sleep(1)
                        html_content1 = driver.page_source
                        soup1 = BeautifulSoup(html_content1, 'html.parser')
                        span_element = soup1.find('span', class_='nav-link t')
                        if span_element:
                            text = span_element.get_text(strip=True)
                            state_name = text.split(' > ')[0]
                        #for mathematics
                        time.sleep(3)
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_language_district_class5")
                            district_value = district_value_element.text
                        data = {'Subject': ['Language'], 'Overall_Pct': [district_value]}
                        print(district_value)
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        time.sleep(3)
                        #for mathematics
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_math_district_class5")
                            district_value = district_value_element.text
                        data = {'Subject': ['Mathematics'], 'Overall_Pct': [district_value]}
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        #for mathematics
                        time.sleep(3)
                        for i in range(3):
                            district_value_element = driver.find_element(By.ID, "performance_evs_district_class5")
                            district_value = district_value_element.text
                        data = {'Subject': ['EVS'], 'Overall_Pct': [district_value]}
                        time.sleep(1)
                        df = pd.DataFrame(data)
                        df['State_Name']=state_name
                        df['District_Name']=district_name
                        final_df=final_df.append(df)
                        
                        final_df['State_Name'] = final_df['State_Name'].str.replace('(', '').str.strip()
                        final_df['Class']='Class 5'
                        final_df['Relevant_Date']='2021-03-31'
                        final_df['Relevant_Date']=pd.to_datetime(final_df['Relevant_Date'])
                        final_df['Runtime']=pd.to_datetime('now')
                        final_df.columns=['Subject','Overall_Pct','State_Name','District_Name','Class','Relevant_Date','Runtime']
                        final_df=final_df[['Class','State_Name','District_Name','Subject','Overall_Pct','Relevant_Date','Runtime']]
                        final_df.reset_index(drop=True,inplace=True)
                        engine = adqvest_db.db_conn()
                        connection = engine.connect()
                        connection.execute("Delete from DOSEL_NAS_DIST_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME where State_Name='"+state_name+"' and District_Name='"+district_name+"' and Class='"+class_name[i]+"'")
                        connection.execute('commit')
                        time.sleep(3)
                        final_df.to_sql(name='DOSEL_NAS_DIST_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME',con=engine,if_exists='append',index=False)
                        # final_df=final_df.append(df)
                        
            break

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')


