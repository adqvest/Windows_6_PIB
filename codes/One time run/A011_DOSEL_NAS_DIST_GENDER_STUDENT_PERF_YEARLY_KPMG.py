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
from selenium.webdriver.common.action_chains import ActionChains

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
    table_name = 'DOSEL_NAS_DIST_LOCATION_STUDENT_PERF_YEARLY_KPMG'
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

        list_buttons = soup.find_all('a', class_='national_state_list')
        state_names = []
        df= pd.DataFrame()

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
                final_df=pd.DataFrame()
                count=0
                for district in district_links:
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
                        try:
                            element_to_hover_over = driver.find_element("xpath", '//*[@id="GenderLanguageBarGraph_class3"]')
                            time.sleep(1)
                            # Move to the left part of the element
                            ActionChains(driver).move_to_element_with_offset(element_to_hover_over, -200, 0).perform()
                            time.sleep(2)

                            element_to_hover_over = driver.find_element("xpath", '//*[@id="GenderEvsBarGraph_class3"]')

                            # Move to the left part of the element
                            ActionChains(driver).move_to_element_with_offset(element_to_hover_over, -200, 0).perform()
                            html_content = driver.page_source

                            element_to_hover_over = driver.find_element("xpath", '//*[@id="GenderMathBarGraph_class3"]')

                            # Move to the left part of the element
            #                     ActionChains(driver).move_to_element_with_offset(element_to_hover_over, -200, 0).perform()
            #                     time.sleep(3)
            #                     soup = BeautifulSoup(html_content, 'html.parser')
            #                     data = soup.find_all('div',attrs = {"class":"highcharts-label highcharts-tooltip highcharts-color-undefined"}) 
                            element_to_hover_over = driver.find_element("xpath", '//*[@id="perAccordioncollapseOne"]/div/div[3]/div[1]/div/div')
                            time.sleep(2)
                            ActionChains(driver).move_to_element_with_offset(element_to_hover_over, 400, 0).perform()
                            time.sleep(2)
                            html_content = driver.page_source

                            soup = BeautifulSoup(html_content, 'html.parser')
                            data = soup.find_all('div',attrs = {"class":"highcharts-label highcharts-tooltip highcharts-color-undefined"})  
                            table_data = []
                            for text in data:
                                text=text.text
                                parts = text.split('%')
                                row_data = {
                                    'Gender': 'Boys',
                                    'Pct': int(parts[0].split(': ')[1])
                                }
                                table_data.append(row_data)
                                row_data = {
                                    'Gender': 'Girls',
                                    'Pct': int(parts[1].split(': ')[1])
                                }
                                table_data.append(row_data)

                            # Creating a DataFrame
                            df = pd.DataFrame(table_data)
                            df['Subject'] = ['Language', 'Language', 'EVS', 'EVS', 'Maths', 'Maths']
                            df['State_Name']=state_name
                            df['District_Name']=district_name
                            df['State_Name'] = df['State_Name'].str.replace('(', '').str.strip()
                            df['Class']='Class 3'
                            df['Relevant_Date']='2021-03-31'
                            df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'])
                            df['Runtime']=pd.to_datetime(today)
                            df.columns=['Gender','Pct','Subject','State_Name','District_Name','Class','Relevant_Date','Runtime']
                            df=df[['Class','State_Name','District_Name','Subject','Gender','Pct','Relevant_Date','Runtime']]
                            df.reset_index(drop=True,inplace=True)
                            df = df.pivot_table(index=['Class','State_Name', 'District_Name','Subject','Relevant_Date','Runtime'], columns=['Gender' ], values='Pct', aggfunc='sum')
                            df.reset_index(inplace=True)
                            df = df.rename(columns={'Boys': 'Boys_Pct'})
                            df = df.rename(columns={'Girls': 'Girls_Pct'})  
                            df=df[['Class','State_Name','District_Name','Subject','Boys_Pct','Girls_Pct','Relevant_Date','Runtime']] 
                            print(df)
                            df.to_sql(name='DOSEL_NAS_DIST_GENDER_STUDENT_PERF_YEARLY_KPMG',con=engine,if_exists='append',index=False)
                            
                            # df=df.append(df)
                            time.sleep(5)
                            
                        except:
                            print('No data in site')
                    final_df=final_df.append(df)
                    # final_df['State_Name'] = final_df['State_Name'].str.replace('(', '').str.strip()
                    count=count+1
                    print('Done for District and now the count is------>',count)
                          
                break

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')