# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 10:12:07 2024

@author: Santonu
"""
import datetime
import os
import re
import sys
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from playwright.sync_api import sync_playwright
from pytz import timezone
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
from selenium.webdriver.support.ui import Select
import pdfplumber
from fiscalyear import *
import fiscalyear
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime
warnings.filterwarnings('ignore')
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
# import state_rewrite

#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
job_start_time = datetime.datetime.now(india_time)
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
#%%

def get_request_session(url):
    
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r=session.get(url)
    return r
def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    # print(pdf_list)
    matching = [s for s in pdf_list if key_word in s]
    print('Matching')
    # print(matching)
                   
    
    from playwright.sync_api import sync_playwright
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
                # Wait for the download to start
                download = download_info.value
                # Wait for the download process to complete
                # print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    os.chdir('C:/Users/Administrator/KPMG/PM_POSHAN')
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
         's3',
         aws_access_key_id=ACCESS_KEY_ID,
         aws_secret_access_key=ACCESS_SECRET_KEY,
         config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
    data_s3 =  open(filename, 'rb')
    s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
    data_s3.close()
    

def read_link(link,file_name,s3_folder):
    os.chdir('C:/Users/Administrator/KPMG/PM_POSHAN')
    path=os.getcwd()
    # r=requests.get(link,headers =headers,verify=True)
    # r.raise_for_status()
    r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.xls"))
    # print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    return file



def get_financial_year(dt,prev_fiscal=False):
    fiscalyear.setup_fiscal_calendar(start_month=4)
    if dt.month<4:
        yr=dt.year
    else:
        yr=dt.year+1
        
    if prev_fiscal==True:
        yr=yr-1
        
    fy = FiscalYear(yr)
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    fy_year=str(fy.start.year)+'-'+str(fy.end.year)[-2:]
    return fy_year

def extract_table_using_plumber(pdf_file,search_str):
    
    df_list=[]
    page_list=[]
    with pdfplumber.open(pdf_file) as pdf:
         for i in range(len(pdf.pages)):
             # print(i)
             i=6
             page_text=pdf.pages[i].extract_text()
             if search_str.lower() in page_text.lower():
                 page_list.append(i)
                 break
             
    with pdfplumber.open(pdf_file) as pdf:                    
         for i in range(len(pdf.pages)):
             if i in page_list:
                 table = pdf.pages[i].extract_tables()
                 df = pd.DataFrame(table[0],columns=table[0][0])
    
    return df

def s3_buccket_download(folder_name,file_extension,download_path):
    
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    os.chdir(download_path)
 
    s3_client =boto3.client('s3')
    s3_bucket_name='adqvests3bucket'
    s3 = boto3.resource('s3',
                        aws_access_key_id= ACCESS_KEY_ID,
                        aws_secret_access_key=ACCESS_SECRET_KEY)

    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    file_list=[i.key for i in my_bucket.objects.filter(Prefix =folder_name) if f'{file_extension}' in i.key ]
    print(f"Total item in bucket : {len(file_list)}")
    
    for file in file_list:
        print(file)
        try:
          downloded_file=s3.Bucket(s3_bucket_name).download_file(file,f'{file.split("/")[-1]}')
        except:
            pass

    print("Done")
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "KPMG_PM_POSHAN_DIST_COVERAGE_temp_santonu"
    scheduler = ''
    no_of_ping=0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
#%%            
        os.chdir(r'C:\Users\Administrator\KPMG\PM_POSHAN')
        download=os.getcwd()
        delete_pdf=os.listdir(r"C:\Users\Administrator\KPMG\PM_POSHAN")
        for file in delete_pdf:
                os.remove(file)
                
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from KPMG_PM_POSHAN_DIST_COVERAGE_temp_santonu", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()+relativedelta(years=2)
        print(type(max_rel_date))
        financial_yr=get_financial_year(max_rel_date)
        print(financial_yr)
        #%%
        link=f"https://pmposhan.education.gov.in/PAB%20{financial_yr.split('-')[0]}-20{financial_yr.split('-')[1]}.html"
        robot.add_link(link)
        r=get_request_session(link)
        soup=BeautifulSoup(r.content)

        #2021-2022------2019-2020
        state_link={i.text.replace('\n',' ').strip().lower():i.find_all('a',href=True)[2]['href'] for i in soup.find_all('tr') if ((i.find('a',href=True)!=None) and ('edcil-tsg' not in i.text.replace('\n',' ').strip().lower()) and ('nic' not in i.text.replace('\n',' ').strip().lower()))}
        state_link={k.split('pdf')[0].split('.')[1]+financial_yr:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        state_link={'_'.join(k.replace('\n','_').strip().split()).upper().replace('_;',''):'https://pmposhan.education.gov.in/'+v for k,v in state_link.items()}
        
        #2018-2019------2016-2017
        # state_link={i.text.replace('\n',' ').strip().lower():i.find_all('a',href=True)[2]['href'] for i in soup.find_all('tr') if ((i.find('a',href=True)!=None) and ('edcil-tsg' not in i.text.replace('\n',' ').strip().lower()) and ('nic' not in i.text.replace('\n',' ').strip().lower()))}
        # state_link={re.findall('[A-Za-z]+', k.split('pdf')[0].replace('\n',''))[0]:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        # state_link={k+'_'+financial_yr.split('-')[0]+'-'+financial_yr.split('-')[1][-2:]:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        # state_link={'_'.join(k.replace('\n','_').strip().split()).upper().replace('_;',''):'https://pmposhan.education.gov.in/'+v for k,v in state_link.items()}
        
        if len(state_link)>0:
            for st,lnk in sorted(state_link.items(), key=lambda item: item[1], reverse=False):
                    print(st)
                    print(lnk)
                    robot.add_link(lnk)
                    os.chdir('C:/Users/Administrator/KPMG/PM_POSHAN')
                    filename=f"{'_'.join(st.split('_')[:-1])}_PM_POSHAN_{st.split('_')[-1]}.xls"
                    s3_folder=f"KPMG/PM_POSHAN_FACTS_SHEET/{st.split('_')[-1]}"
                    xl_lnk=read_link(lnk,filename,s3_folder)


        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')