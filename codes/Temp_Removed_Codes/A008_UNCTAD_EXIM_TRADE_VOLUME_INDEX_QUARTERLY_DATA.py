''' @author : Joe '''
import sqlalchemy
import pandas as pd
import numpy as np
from pandas.io import sql
import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
from time import sleep
import random
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import zipfile
from zipfile import ZipFile 
import sys
import time
import glob
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import warnings
import requests
warnings.filterwarnings('ignore')
from dateutil import parser
from pandas.tseries.offsets import MonthEnd
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import JobLogNew as log
import ClickHouse_db
import cv2
import time
import boto3
from botocore.config import Config
from adqvest_robotstxt import Robots
robot = Robots(__file__)

import MySql_To_Clickhouse as MySql_CH
#%%
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

#%%

imp_exp={'01':'Import_Vol_Index',
         '02':'Export_Vol_Index'}
def convert_qtr_date(k):
    Q_dict={'02':f'y-06-30', '03':f'y-09-30',
            '04':f'y-12-31', '01':f"y-03-31"}
    
    qtr=k.split('Q')[1]
    st=k.split('Q')[0]
    dt=Q_dict[qtr].replace('y',st) 
    return dt 
            
            
            
def get_unctad_trade_data(k):
    k=str(k)
    headers = {
        'authority': 'unctadstat-api.unctad.org',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'ocp-apim-subscription-key': '433468f8d0c4401e9cd359beec6d2bd4',
        'origin': 'https://unctadstat.unctad.org',
        'referer': 'https://unctadstat.unctad.org/',
        'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36 Edg/119.0.0.0',
    }
    
    data =f"$select=Economy/Code,Quarter/Code,M6492,M6490,M4024,M4023&$filter=Flow/Code eq '{k}' and Economy/Code in ('356')&culture=en"
    
    r = requests.post(
        'https://unctadstat-api.unctad.org/datamart-api/US.MerchVolumeQuarterly/767/Facts',
        headers=headers,
        data=data,
    )
    robot.add_link('https://unctadstat-api.unctad.org/datamart-api/US.MerchVolumeQuarterly/767/Facts')
    df=r.json()['value']
    df1=pd.DataFrame.from_dict(df)
    df1['Economy']=df1['Economy'].apply(lambda x:x['Code'])
    df1['Quarter']=df1['Quarter'].apply(lambda x:x['Code'])
    df1['M6490']=df1['M6490'].apply(lambda x:x['Value'])
    df1['Relevant_Date']=df1['Quarter'].apply(lambda x:convert_qtr_date(x))
    df1=df1[['M6490','Relevant_Date']]
    df1=df1.rename(columns={'M6490':imp_exp[k]})
    df1['Runtime']=datetime.datetime.now()
    
    return df1
            
def update_back_data(table_name,org_df):
    q1=f"select distinct Relevant_Date as Relevant_Date  from {table_name} where Relevant_Date< (select max(Relevant_Date) from {table_name});"
    org_dates = pd.read_sql(q1,con=engine)
    
    org_dates['Relevant_Date']=org_dates['Relevant_Date'].apply(lambda x:str(x))
    org_df['Relevant_Date']=org_df['Relevant_Date'].apply(lambda x:str(x))
    
    df_dates=org_df.Relevant_Date.to_list()
    org_dates=org_dates.Relevant_Date.to_list()
    print(org_dates)

    common_dates=set(org_dates) & set(df_dates)
    print(common_dates)
    if len(common_dates)>0:
        for i in common_dates:
            df=org_df[(org_df['Relevant_Date']==i)==True]
            datewise_count = df['Relevant_Date'].value_counts().to_list()[0]
            
            q2=f"select Relevant_Date,count(*) as count from {table_name} where Relevant_Date='{i}' group by Relevant_Date order by Relevant_Date desc;"
            cnt= pd.read_sql(q2,con=engine)
            print('--------------------------------------')

            if datewise_count==cnt['count'][0]:
                print(f"data Deleted for---->{i}")
            
                engine.execute(f"Delete from {table_name} where Relevant_Date='{i}'")
                print(f"data Deleted for---->{i}")
                print(df.info())
                print('----------------------------------------------------------')
                df.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"data Uploded for---->{i}")
                print(df.info())
#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):


    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'UNCTAD_EXIM_TRADE_VOLUME_INDEX_QUARTERLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '2_30_AM_WINDOWS_SERVER_SCHEDULER_ALL_CODES'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def month_for_quarter(date):
            q = date.split(" ")[0]
            y = int(date.split(" ")[1])
            dates = {'Q1':'3', 'Q2':'6',
                     'Q3':'9', 'Q4':'12'}
            date = pd.to_datetime(str(y) + '-' + dates[q], format="%Y-%m") + MonthEnd(1)
            date = date.date()
            print('cur_rel:',date)
            return date

        def get_volume(driver,download_path,vol_type):
            os.chdir(download_path)
            time.sleep(2)
            driver.find_element(By.ID,'basic-addon1').click()
            time.sleep(2)
            if vol_type == 'Exports':
                driver.find_element(By.XPATH,"//span[text()='Exports']").click()
            else:
                driver.find_element(By.XPATH,"//ngb-highlight[text()='Imports']").click()
            time.sleep(2)
            driver.find_element(By.XPATH,"//div[@class='icon download-csv']").click()
            time.sleep(2)
            # driver.find_element(By.XPATH,"//input[@value='tabular']").click()
            # time.sleep(2)
            driver.find_element(By.XPATH,"//button[@class='btn btn-primary'][text()='Download ']").click()
            time.sleep(5)
            for j in os.listdir():
                if j.startswith('US') & j.endswith('zip'):
                    file_name = j
                    break
            with ZipFile(file_name, 'r') as zip:
                for info in zip.infolist():
                    if info.filename.endswith('.csv'):
                        zip.extract(info)
                        csv_file = info.filename
                        break
            data = pd.read_csv(csv_file)
            data = data[data['Economy_Label'].str.lower().str.contains('india')].reset_index(drop=True)
            vol = data['Volume_Index_2005100_Value'][0]
            os.remove(csv_file)
            os.remove(file_name)
            return vol

        def CH_UPLOAD(table_name):
            click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            print('click max:',click_max_date)
            df = pd.read_sql(f'select * from {table_name} WHERE Relevant_Date > "' + click_max_date + '"',engine)
            client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())

        max_date=pd.read_sql(f'select MAX(Relevant_Date) as MAX from {table_name}',engine)['MAX'][0]
        print('max_rel:',max_date)
        dates={'3':'Q2','6': 'Q3',
                       '9':'Q4','12': 'Q1'}
        y = max_date.year
        m = str(max_date.month)
        if m == '12':
            y=str(y+1)
        else:
            y = str(max_date.year)
        nex_qr = f'{dates[m]} {y}'
        print('Searching for ',nex_qr)

        url = 'https://unctadstat.unctad.org/wds/TableViewer/tableView.aspx?ReportId=99'
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path = r"C:\Users\Administrator\Junk"
        os.chdir(download_path)
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', {
        "download.default_directory": download_path, #Change default directory for downloads
        "download.prompt_for_download": False, #To auto download the file
        "download.directory_upgrade": True})
        driver = webdriver.Chrome(driver_path,chrome_options=options)
        driver.maximize_window()

        driver.get(url)
        robot.add_link(url)
        time.sleep(20)
        driver.find_element(By.ID,'design_table').click()
        time.sleep(10)
        driver.find_element(By.XPATH,'/html/body/ngb-modal-window/div/div/div/div[2]/div/div[2]/app-dim-list/treelist/ag-grid-angular/div/div[2]/div[2]/div[1]/div[2]/div/div[1]/div/div[2]/div[2]/input').click()
        time.sleep(5)
        driver.find_element(By.XPATH,'/html/body/ngb-modal-window/div/div/div/div[2]/div/div[2]/app-dim-list/treelist/ag-grid-angular/div/div[2]/div[2]/div[1]/div[2]/div/div[1]/div/div[2]/div[2]/input').click()
        time.sleep(5)
        driver.find_element(By.CLASS_NAME,'form-control').send_keys('Individual economies')
        time.sleep(3)
        driver.find_element(By.ID,'search').click()
        time.sleep(3)
        driver.find_element(By.CLASS_NAME,'ag-group-value').click()
        time.sleep(3)
        driver.find_elements(By.CLASS_NAME,'list-group')[1].click()
        time.sleep(3)
        driver.find_element(By.XPATH,'/html/body/ngb-modal-window/div/div/div/div[2]/div/div[2]/app-dim-list/treelist/ag-grid-angular/div/div[2]/div[2]/div[1]/div[2]/div/div[1]/div/div[2]/div[2]/input').click()
        time.sleep(3)
        driver.find_element(By.CLASS_NAME,'form-control').send_keys(nex_qr)
        time.sleep(3)
        driver.find_element(By.ID,'search').click()
        time.sleep(3)
        try:
            driver.find_element(By.CLASS_NAME,'ag-group-value')
            time.sleep(1)
            new = True
        except:
            new = False
        if new == True:
            driver.find_element(By.CLASS_NAME,'ag-group-value').click()
            time.sleep(2)
            raw_quar = driver.find_element(By.CLASS_NAME,'ag-group-value').text
            print('raw_quar:',raw_quar)

            '''  <<<  quarter  >>>  '''
            cur_date = month_for_quarter(raw_quar)
            print('cur_date:',cur_date)
            if cur_date > max_date:
                print('***  NEW DATA  ***')
                #apply
                driver.find_element(By.XPATH,"/html/body/ngb-modal-window/div/div/div/div[2]/div/div[1]/div[7]/div/button[2]").click()
                time.sleep(2)
                driver.find_element(By.XPATH,"/html/body/app-root/app-dataviewer/div/div[2]/div/div/div[2]/div/div[1]/app-typeahead-ddl/div/input").send_keys('India')
                time.sleep(2)
                driver.find_elements(By.CLASS_NAME,'form-group')[2].click()
                time.sleep(3)
                ''' <<<  Exports  >>> '''
                exp_vol = get_volume(driver,download_path,'Exports')

                ''' <<<  Imports  >>> '''
                imp_vol = get_volume(driver,download_path,'Imports')

                volume_out = pd.DataFrame({'Import_Vol_Index' : imp_vol, 'Export_Vol_Index' : [exp_vol],'Relevant_Date':cur_date, 'Runtime' : datetime.datetime.now()})
                volume_out.to_sql(name=table_name,if_exists='append',index=False,con=engine)
                ######################################################################################################
                # Modified | Santonu |Nov 10| Reason:Revising Old data 
                df_imp=get_unctad_trade_data('01')
                df_exp=get_unctad_trade_data('02')
                
                df_final=pd.merge(df_imp, df_exp[['Export_Vol_Index','Relevant_Date']],on='Relevant_Date',how='inner')
                df_final=df_final[['Import_Vol_Index','Export_Vol_Index','Relevant_Date', 'Runtime']]   
                update_back_data("UNCTAD_EXIM_TRADE_VOLUME_INDEX_QUARTERLY_DATA",df_final)
                MySql_CH.ch_truncate_and_insert('UNCTAD_EXIM_TRADE_VOLUME_INDEX_QUARTERLY_DATA')

                ######################################################################################################
        else:
            print('*** No new data ***')
        # CH_UPLOAD(table_name)

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
    