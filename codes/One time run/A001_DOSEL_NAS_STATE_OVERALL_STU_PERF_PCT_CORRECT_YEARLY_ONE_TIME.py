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
    table_name = 'DOSEL_NAS_STATE_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME'
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
        time.sleep(3)



        def get_data(driver,hover_val_x,hover_val_y,tag_val):
            for i in range(3):
                time.sleep(2)
                #/html/body/section/div[2]/main/div/div[2]/div[3]/div[3]/div/div/div[15]/div/div/div[1]/div/div/div[1]/div/div/figure/div
                #/html/body/section/div[2]/main/div/div[2]/div[3]/div[3]/div/div/div[15]/div/div/div[1]/div/div/div[2]/div/div/figure/div
                element_to_hover_over = driver.find_element("xpath", '/html/body/section/div[2]/main/div/div[2]/div[3]/div[3]/div/div/div['+str(tag_val)+']/div/div/div[1]/div/div/div[1]/div/div/figure/div')
                time.sleep(2)
                ActionChains(driver).move_to_element_with_offset(element_to_hover_over,hover_val_x[0],  hover_val_y[0]).perform()
                time.sleep(2)
            print('language---->',hover_val_x[0])
            for i in range(3):
                element_to_hover_over = driver.find_element("xpath", '/html/body/section/div[2]/main/div/div[2]/div[3]/div[3]/div/div/div['+str(tag_val)+']/div/div/div[1]/div/div/div[2]/div/div/figure/div')
                time.sleep(5)
                ActionChains(driver).move_to_element_with_offset(element_to_hover_over, hover_val_x[1], hover_val_y[1]).perform()
                html_content = driver.page_source
            for i in range(3):
                element_to_hover_over = driver.find_element("xpath", '/html/body/section/div[2]/main/div/div[2]/div[3]/div[3]/div/div/div['+str(tag_val)+']/div/div/div[1]/div/div/div[3]/div/div/figure/div')
                time.sleep(5)
                ActionChains(driver).move_to_element_with_offset(element_to_hover_over, hover_val_x[2], hover_val_y[2]).perform()
            html_content = driver.page_source

            soup = BeautifulSoup(html_content, 'html.parser')
            data = soup.find_all('div',attrs = {"class":"highcharts-label highcharts-tooltip highcharts-color-0"}) 
            print(data)
            table_data = []
            for text in data:
                text=text.text
                parts = text.split('%')
                row_data = {
                    'Pct': int(parts[0].split(': ')[1])
                }
                table_data.append(row_data)
            df = pd.DataFrame(table_data)
            print(df.shape)
            return df


        html_content = driver.page_source

        soup = BeautifulSoup(html_content, 'html.parser')

        list_buttons = soup.find_all('a', class_='national_state_list')
        state_names = []
        df= pd.DataFrame()

        #//*[@id="class3-tab"]
        class_tabs=['class3-tab','class5-tab']
        class_name=['Class 3','Class 5']
        tag_val=[15,16]
        for i in range(len(class_name)):
            loop=1
            count=0
            for button in list_buttons:
                if button.has_attr('onclick'):
                    driver.execute_script(button['onclick'])
                    time.sleep(2)
                    class_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, class_tabs[i])))
                    class_tab.click()
                    performance_tab_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "performance-tab")))
                    performance_tab_button.click()
                    html_content1 = driver.page_source
                    soup1 = BeautifulSoup(html_content1, 'html.parser')
                    span_element = soup1.find('span', class_='nav-link t')
                    tag_val_cls=tag_val[i]
                    if span_element:
                        text = span_element.get_text(strip=True)
                        state_name = text
                    try:
                        hover_val_x=[0,10,0]
                        hover_val_y=[0,10,0]
                        df= get_data(driver,hover_val_x,hover_val_y,tag_val_cls)
                        if len(df)<2:
                            hover_val_x=[0,10,10]
                            hover_val_y=[0,10,10]
                            df= get_data(driver,hover_val_x,hover_val_y,tag_val_cls)
                        if len(df)<2:
                            hover_val_x=[0,10,0]
                            hover_val_y=[0,10,0]
                            df= get_data(driver,hover_val_x,hover_val_y,tag_val_cls)
                        df['Subject'] = ['Language', 'Maths','EVS']
                        df['State_Name']=state_name
                        df['Class']=class_name[i]
                        df['State_Name'] = df['State_Name'].str.replace(r'[()]', '')
                        df['State_Name'] = df['State_Name'].str.replace(r'[()]', '')
                        df['Relevant_Date']='2021-03-31'
                        df['Relevant_Date']=pd.to_datetime(df['Relevant_Date'])
                        df['Runtime']=pd.to_datetime('now')
                        state_name=state_name.replace(r'[()]', '')
                        df.columns=['Overall_Pct','Subject','State_Name','Class','Relevant_Date','Runtime']
                        df=df[['Class','State_Name','Subject','Overall_Pct','Relevant_Date','Runtime']]
                        df.reset_index(drop=True,inplace=True)
                        df.reset_index(inplace=True)
                        df=df[['Class','State_Name','Subject','Overall_Pct','Relevant_Date','Runtime']] 
                        print(df)
                        engine = adqvest_db.db_conn()
                        connection = engine.connect()
                        connection.execute("Delete from DOSEL_NAS_STATE_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME where State_Name='"+state_name+"' and Class='"+class_name[i]+"'")
                        connection.execute('commit')
                        time.sleep(3)
                        df.to_sql(name='DOSEL_NAS_STATE_OVERALL_STU_PERF_PCT_CORRECT_YEARLY_ONE_TIME',con=engine,if_exists='append',index=False)
                        
                        count=count+1
                        print('Done for District and now the count is------>',count)  
                        time.sleep(5)
                    except:
                        print('No data in site')
            print('DONE FOR CLASS----------->',class_name[i])
            print('--------------------------------RUNNING CODE FOR NEXT CLASS---------------------------')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
                    





