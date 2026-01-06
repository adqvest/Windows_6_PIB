# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 16:39:47 2024

@author: GOKUL
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os 
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from playwright.sync_api import sync_playwright
import datetime
from pytz import timezone
import requests
import glob
import boto3
import numpy as np
import re
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn() 

    
def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    print(pdf_list)

    matching = [s for s in pdf_list if key_word in s]
    print('Matching')
    print(matching)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=10000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto("https://www.ilovepdf.com/",timeout=30000*5)

        page.locator("//*[contains(text(),'Login')]").click()
        email = page.locator("//*[@id='loginEmail']")
        email.fill("kartmrinal101@outlook.com")
        password = page.locator("//*[@id='inputPasswordAuth']")
        password.fill("zugsik-zuqzuH-jyvno4")
        page.locator("//*[@id='loginBtn']").click()
        page.get_by_title("PDF to Excel").click()

        for i in matching:
            with page.expect_file_chooser() as fc_info:
                page.get_by_text("Select PDF file").click()
                file_chooser = fc_info.value
                file_chooser.set_files(i)
                if OCR_doc==True:
                    page.get_by_text("Continue without OCR").click()
                    
                page.locator("//*[@id='processTask']").click()
                with page.expect_download() as download_info:
                    page.get_by_text("Download EXCEL").click()
                download = download_info.value
                print(download.path())
                file_name = download.suggested_filename
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()    

def read_link(link,file_name,s3_folder):
    os.chdir(r"C:\Users\Administrator\Junk_NAS_State_Wise_Perf_Avg")
    path=os.getcwd()
    print(link)
    if not link.startswith('http://') and not link.startswith('https://'):
        link = 'https://nas.gov.in/' + link

    print(link)    
    try:
        r = requests.get(link, timeout = 60)
        with open(file_name, 'wb') as f:
            f.write(r.content)
        files = glob.glob(os.path.join(path, "*.pdf"))
        print(files)
        file=files[0]   
        return file
    except Exception as e:
        print(f"Error fetching link: {link}")
        print(e)
        return None    

def clean_string(s):
    return re.sub(r'[^a-zA-Z0-9\s]', '', s)

def get_desired_table(link, search_str):
    xls = pd.ExcelFile(link)
    sheet_names = xls.sheet_names
    print(sheet_names)
    
    for sheet_name in sheet_names:
        df = pd.read_excel(link, sheet_name=sheet_name, header=None)
        df = df.replace(np.nan, '')
        print('Done', sheet_name)
        # print('Cell Value:', df.iat[0, 0])
    
        if not df.empty and clean_string(search_str.lower()) in clean_string(df.iat[0, 0].lower()):
            print('Sheet Found')
            return df
    
    print('Sheet not found for search string:', search_str)
    return None 

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
    table_name = 'DOSEL_NAS_DISTRICT_WISE_PERFORMANCE_AVERAGE_YEARLY_DATA_KPMG,DOSEL_NAS_STATE_WISE_PERFORMANCE_AVERAGE_YEARLY_DATA_KPMG'
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
        download_file_path = r"C:\Users\Administrator\Junk_NAS_State_Wise_Perf_Avg"
        for filename in os.listdir(download_file_path):
            file_path = os.path.join(download_file_path, filename)
            os.remove(file_path)
        
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

        url = "https://nas.gov.in/report-card/nas-2021"
        driver.get(url)
        time.sleep(3)
        
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        list_buttons = soup.find_all('a', class_='national_state_list')
        time.sleep(1)
                    
        all_district_dataframes = []
        all_state_dataframes = []
        
        for state in list_buttons:
            if state.has_attr('onclick'):
                driver.execute_script(state['onclick'])
                state_name = state.get_text(strip=True)
                state_name = state_name.replace('radio_button_checked', '').replace('radio_button_unchecked', '').strip()
                print(state_name)
                
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, 'html.parser')
                link_with_id = soup.find('a', {'id': 'report-link'})
                if link_with_id:
                    link = link_with_id.get('href')
                    temp_link = link_with_id['href']
                    filename = 'NAS_'+state_name+'.pdf'
                    print(link)
                    print(temp_link)
                    print(filename)
                    
                    read_link(link,filename,'Junk_NAS_State_Wise_Perf_Avg')
                    pdf_to_excel(r"C:\Users\Administrator\Junk_NAS_State_Wise_Perf_Avg",filename)
                    files = glob.glob(os.path.join(download_file_path, "*.xlsx"))
                    print(files)
                    file=files[0] 
                    
                    serach_str = 'Overall Achievement Score'        
                    dis_avg=get_desired_table(file,serach_str)
                    dis_avg.columns=['Num','District','Mean_Pct','Blank', 'Perf_of_Children_Bel_Basic','Perf_of_Children_Adv_Prof']
                    rows_to_delete = [0,1,2]
                    dis_avg.drop(rows_to_delete, inplace=True)
                    columns_to_delete = ['Num', 'Blank']
                    dis_avg.drop(columns=columns_to_delete, inplace=True)
                    dis_avg.reset_index(drop=True,inplace=True)
                    dis_avg['State'] = state_name
                    st_avg = dis_avg.iloc[[-1]]
                    st_avg.columns=['State','Mean_Pct','Perf_of_Children_Bel_Basic','Perf_of_Children_Adv_Prof']
                    
                    rows_to_delete = [3]
                    dis_avg.drop(rows_to_delete, inplace=True)
                    st_avg['State'] = state_name
                    st_avg.reset_index(drop=True,inplace=True)
                
                else:
                    print("No 'a' element with the specified ID found.")
                    
            all_state_dataframes.append(st_avg)   
            all_district_dataframes.append(dis_avg)
        final_df_dis = pd.concat(all_district_dataframes, ignore_index=True) 
        final_df_dis['Relevant_Date'] = '2022-03-31'
        final_df_dis['Runtime'] = datetime.datetime.now()
        final_df_dis['Relevant_Date'] = pd.to_datetime(final_df_dis['Relevant_Date']).dt.date
        
        final_df_st = pd.concat(all_state_dataframes, ignore_index=True) 
        final_df_st['Relevant_Date'] = '2022-03-31'
        final_df_st['Runtime'] = datetime.datetime.now()
        final_df_st['Relevant_Date'] = pd.to_datetime(final_df_st['Relevant_Date']).dt.date 

        print(final_df_st)
        print("-------------------")
        print(final_df_dis)

        final_df_dis.to_sql(name='DOSEL_NAS_DISTRICT_WISE_PERFORMANCE_AVERAGE_YEARLY_DATA_KPMG',con=engine,if_exists='append',index=False)
        final_df_st.to_sql(name='DOSEL_NAS_STATE_WISE_PERFORMANCE_AVERAGE_YEARLY_DATA_KPMG',con=engine,if_exists='append',index=False)

    
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')