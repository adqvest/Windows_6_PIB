''' @author : Joe '''
import sqlalchemy
import calendar
import os
import requests
import json
import random
import re
import ast
import csv
import time
import sys
import PyPDF2
import urllib
import numpy as np
import pandas as pd
from time import sleep
from pandas.io import sql
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader,PdfFileWriter
from dateutil.relativedelta import relativedelta
from urllib.request import Request, urlopen
from requests_html import HTMLSession
from pytz import timezone
import glob
import camelot
import warnings
from urllib.parse import urlparse
warnings.filterwarnings('ignore')
import numpy as np
import datetime as datetime
from pandas.tseries.offsets import MonthEnd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import cleancompanies
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NVCT_TRADE_STATISTICS_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '4_AM_WINDOWS_SERVER_CRAWLER_SCHEDULER_ALL_CODES'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        def Selectit(param_id,text,driver):
            select = wait(driver, 10).until(EC.presence_of_element_located((By.NAME, param_id)))
            Select(select).select_by_visible_text(text)
            time.sleep(4)
            return driver

        max_date = pd.read_sql("select max(Relevant_Date) as Max from NVCT_TRADE_STATISTICS_MONTHLY_DATA",engine)["Max"][0]
        status_df = pd.read_sql("select * from NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS",engine)

        if status_df.shape[0]>0:
            print('Collecting for missing records')
            status_df = status_df[(status_df['Status'].isnull())]
            status_df.reset_index(drop=True,inplace=True)
            rel_date=list(set(status_df['Relevant_Date']))[0]
            headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ta;q=0.7',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'}
            url = 'https://www.ncvtmis.gov.in/Pages/ITI/TradeStats.aspx'
            r = requests.get(url,headers=headers, timeout = 100,verify=False)
            soup1 = BeautifulSoup(r.text,'lxml')
            
            'ITI TYPE'
            iti_type = soup1.find_all("select",{"title":"ITI Type"})
            iti_type_id = iti_type[0]['name']
            iti_type_name = iti_type[0].find_all("option")
            iti_type_name = [x.text for x in iti_type_name]
            iti_type_value = iti_type[0].find_all("option")
            iti_type_value = [x['value'] for x in iti_type_value]
            iti_type_name = [x for x in iti_type_name if x!= '-Select-']
            iti_type_value = [x for x in iti_type_value if x!= '-1']

            'STATE'
            states = soup1.find_all("select",{"title":"State"})
            state_id = states[0]['name']
            state_name = states[0].find_all("option")
            state_name = [x.text for x in state_name]
            state_value = states[0].find_all("option")
            state_value = [x['value'] for x in state_value]
            state_value = [x for x in state_value if x!= '-Select-']
            state_name = [x for x in state_name if x!= '-1']
            state_name=[i.strip() for i in state_name]

            'DISTRICT'
            district = soup1.find_all("select",{"title":"District"})
            district_id = district[0]['name']

            chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=chrome_driver_path)
            driver.get(url)
            time.sleep(5)

            for st in range(0,len(status_df)):
                it = status_df['ITI_TYPE'][st]
                state = status_df['State'][st]
                district = status_df['District'][st]
                
                Selectit(iti_type_id,it,driver)
                Selectit(state_id,state,driver)
                Selectit(district_id,district,driver)
                driver.execute_script("document.body.style.zoom='80%'")
                time.sleep(3)
                driver.execute_script("window.scrollTo(0, 50)")

                select = wait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'ctl00$cphBody$btnSearch')))
                driver.execute_script("arguments[0].click();", select)
                time.sleep(5)
                df = pd.read_html(driver.page_source)[2]
                time.sleep(1)
                # print(df)
                try:
                    if 'no record' in df.iloc[1,0].lower():
                        print('No record')
                    else:
                        df = df[df[df.iloc[:,0].str.lower().str.contains('sector name',na=False)==True].index[1]:].reset_index(drop=True)
                        df = df.rename(columns=df.loc[0])

                        df=df[1:].reset_index(drop=True)
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','SeatCountCurrent':'Current_Seat_Count','TraineeCountCurrent':'Current_Trainee_Count','SeatUtilizationCurrent':'Current_Seat_Utilization_Pct','SeatCountPrevious':'Previous_Seat_Count','TraineeCountPrevious':'Previous_Trainee_Count','SeatUtilizationPrevious':'Previous_Seat_Utilization_Pct','SeatCountTotal':'Total_Seat_Count','TraineeCountTotal':'Total_Trainee_Count','SeatUtilizationTotal':'Total_Seat_Utilization_Pct','Trainee CountPrevious(2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','Seat Count Current':'Current_Seat_Count','Trainee Count Current':'Current_Trainee_Count','Seat Utilization Current':'Current_Seat_Utilization_Pct','Seat Count Previous':'Previous_Seat_Count','Trainee Count Previous':'Previous_Trainee_Count','Seat Utilization Previous':'Previous_Seat_Utilization_Pct','Seat Count Total':'Total_Seat_Count','Trainee Count Total':'Total_Trainee_Count','Seat Utilization Total':'Total_Seat_Utilization_Pct','Trainee Count Previous (2nd Year)':'Previous_Trainee_Count_2nd_Year'})

                        df=df[:df[df.iloc[:,1].str.lower().str.contains('total',na=False)==True].index[0]]

                        df['ITI_Type']=it
                        df['State']=state
                        df['District']=district
                        df=df[['Sector_Name','Trade_Name','ITI_Type','State','District','NSQF_Level','Duration','ITI_Count','Current_Seat_Count', 'Current_Trainee_Count',
                               'Current_Seat_Utilization_Pct', 'Previous_Seat_Count',
                               'Previous_Trainee_Count', 'Previous_Seat_Utilization_Pct',
                               'Total_Seat_Count', 'Total_Trainee_Count', 'Total_Seat_Utilization_Pct',
                               'Previous_Trainee_Count_2nd_Year']]
                        df['Relevant_Date']=rel_date
                        df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df.to_sql(name='NVCT_TRADE_STATISTICS_MONTHLY_DATA',if_exists='append',index=False,con=engine)
                except:
                    driver.quit()
                    time.sleep(2)

                    driver = webdriver.Chrome(executable_path=chrome_driver_path)
                    driver.get(url) 
                    time.sleep(5)

                    Selectit(iti_type_id,it,driver)
                    Selectit(state_id,state,driver)
                    Selectit(district_id,district,driver)

                    driver.execute_script("document.body.style.zoom='80%'")
                    time.sleep(3)
                    driver.execute_script("window.scrollTo(0, 50)")

                    select = wait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'ctl00$cphBody$btnSearch')))
                    driver.execute_script("arguments[0].click();", select)
                    time.sleep(5)
                    df = pd.read_html(driver.page_source)[2]
                    time.sleep(1)
                    if 'no record' in df.iloc[1,0].lower():
                        print('No record')
                    else:
                        df = df[df[df.iloc[:,0].str.lower().str.contains('sector name',na=False)==True].index[1]:].reset_index(drop=True)
                        df = df.rename(columns=df.loc[0])
                        df=df[1:].reset_index(drop=True)
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','SeatCountCurrent':'Current_Seat_Count','TraineeCountCurrent':'Current_Trainee_Count','SeatUtilizationCurrent':'Current_Seat_Utilization_Pct','SeatCountPrevious':'Previous_Seat_Count','TraineeCountPrevious':'Previous_Trainee_Count','SeatUtilizationPrevious':'Previous_Seat_Utilization_Pct','SeatCountTotal':'Total_Seat_Count','TraineeCountTotal':'Total_Trainee_Count','SeatUtilizationTotal':'Total_Seat_Utilization_Pct','Trainee CountPrevious(2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','Seat Count Current':'Current_Seat_Count','Trainee Count Current':'Current_Trainee_Count','Seat Utilization Current':'Current_Seat_Utilization_Pct','Seat Count Previous':'Previous_Seat_Count','Trainee Count Previous':'Previous_Trainee_Count','Seat Utilization Previous':'Previous_Seat_Utilization_Pct','Seat Count Total':'Total_Seat_Count','Trainee Count Total':'Total_Trainee_Count','Seat Utilization Total':'Total_Seat_Utilization_Pct','Trainee Count Previous (2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df[:df[df.iloc[:,1].str.lower().str.contains('total',na=False)==True].index[0]]

                        df['ITI_Type']=it
                        df['State']=state
                        df['District']=district
                        df=df[['Sector_Name','Trade_Name','ITI_Type','State','District','NSQF_Level','Duration','ITI_Count','Current_Seat_Count', 'Current_Trainee_Count',
                               'Current_Seat_Utilization_Pct', 'Previous_Seat_Count',
                               'Previous_Trainee_Count', 'Previous_Seat_Utilization_Pct',
                               'Total_Seat_Count', 'Total_Trainee_Count', 'Total_Seat_Utilization_Pct',
                               'Previous_Trainee_Count_2nd_Year']]
                        df['Relevant_Date']=rel_date
                        df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df.to_sql(name='NVCT_TRADE_STATISTICS_MONTHLY_DATA',if_exists='append',index=False,con=engine)

                connection.execute("update NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS set Status = 'Done' where ITI_TYPE='"+it+"' and State = '"+state+"' and District = '"+district+"'")
                connection.execute("commit")
                
                try:
                    driver.find_element(By.XPATH,f"//option[text()='{district}']").click()
                    time.sleep(3)
                except:
                    pass
                driver.find_element(By.XPATH,f"//option[text()='{state}']").click()
                time.sleep(3)
            
            query = "delete from NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS"
            connection.execute(query)
            connection.execute('commit')

        elif ((today.day==26) or (today.date() - max_date >= datetime.timedelta(31))):
            'STATUS TABLE'
            headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ta;q=0.7',
                'Cache-Control': 'max-age=0', 'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"'}
            url = 'https://www.ncvtmis.gov.in/Pages/ITI/TradeStats.aspx'
            r = requests.get(url,headers=headers, timeout = 200, verify=False)
            robot.add_link(url)
            soup1 = BeautifulSoup(r.text,'lxml')

            'ITI TYPE'
            iti_type = soup1.find_all("select",{"title":"ITI Type"})
            iti_type_id = iti_type[0]['name']
            iti_type_name = iti_type[0].find_all("option")
            iti_type_name = [x.text for x in iti_type_name]
            iti_type_value = iti_type[0].find_all("option")
            iti_type_value = [x['value'] for x in iti_type_value]
            iti_type_name = [x for x in iti_type_name if x!= '-Select-']
            iti_type_value = [x for x in iti_type_value if x!= '-1']

            'STATE'
            states = soup1.find_all("select",{"title":"State"})
            state_id = states[0]['name']
            state_name = states[0].find_all("option")
            state_name = [x.text for x in state_name]
            state_value = states[0].find_all("option")
            state_value = [x['value'] for x in state_value]
            state_value = [x for x in state_value if x!= '-Select-']
            state_name = [x for x in state_name if x!= '-1']
            state_name=[i.strip() for i in state_name]

            'DISTRICT'
            district = soup1.find_all("select",{"title":"District"})
            district_id = district[0]['name']

            chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            driver = webdriver.Chrome(executable_path=chrome_driver_path)
            driver.get(url)
            time.sleep(2)
            trade_status=pd.DataFrame(columns=['State','District'])
            for state in state_name[1:]:
                Selectit(state_id,state,driver)
                sel_soup = BeautifulSoup(driver.page_source,'lxml')
                district = sel_soup.find_all("select",{"title":"District"})
                district_name =[x.text for x in district[0].find_all("option") if x.text!= '-Select-']
                district_name=[i.strip() for i in district_name]
                for district in district_name:
                    trade_status.loc[len(trade_status)]=[state,district]
                driver.find_element(By.XPATH,f"//option[text()='{state}']").click()
                time.sleep(2)
                
            trade_copy=trade_status.copy()
            trade_status['ITI_TYPE']='Goverment'
            trade_copy['ITI_TYPE']='Private'
            trade_status=pd.concat([trade_status,trade_copy])
            trade_status=trade_status.sort_values(by=['State','District'])
            trade_status['Status']=None
            trade_status['Relevant_Date']=today.date()
            trade_status['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            trade_status=trade_status[['ITI_TYPE','State','District','Status','Relevant_Date','Runtime']]
            trade_status.to_sql(name='NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS',if_exists='append',index=False,con=engine)

            'MAIN TABLE'
            status_df=pd.read_sql('select * from NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS',con=engine)

            driver = webdriver.Chrome(executable_path=chrome_driver_path)
            driver.get(url) 
            time.sleep(5)
            for st in range(0,len(status_df)):
                it = status_df['ITI_TYPE'][st]
                state = status_df['State'][st]
                district = status_df['District'][st]
                
                Selectit(iti_type_id,it,driver)
                Selectit(state_id,state,driver)
                Selectit(district_id,district,driver)

                driver.execute_script("document.body.style.zoom='80%'")
                time.sleep(3)
                driver.execute_script("window.scrollTo(0, 50)")

                select = wait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'ctl00$cphBody$btnSearch')))
                driver.execute_script("arguments[0].click();", select)
                time.sleep(5)
                df = pd.read_html(driver.page_source)[2]
                time.sleep(1)
                try:
                    if 'no record' in df.iloc[1,0].lower():
                        print('No record')
                    else:
                        df = df[df[df.iloc[:,0].str.lower().str.contains('sector name',na=False)==True].index[1]:].reset_index(drop=True)
                        df = df.rename(columns=df.loc[0])
                        df=df[1:].reset_index(drop=True)
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','SeatCountCurrent':'Current_Seat_Count','TraineeCountCurrent':'Current_Trainee_Count','SeatUtilizationCurrent':'Current_Seat_Utilization_Pct','SeatCountPrevious':'Previous_Seat_Count','TraineeCountPrevious':'Previous_Trainee_Count','SeatUtilizationPrevious':'Previous_Seat_Utilization_Pct','SeatCountTotal':'Total_Seat_Count','TraineeCountTotal':'Total_Trainee_Count','SeatUtilizationTotal':'Total_Seat_Utilization_Pct','Trainee CountPrevious(2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','Seat Count Current':'Current_Seat_Count','Trainee Count Current':'Current_Trainee_Count','Seat Utilization Current':'Current_Seat_Utilization_Pct','Seat Count Previous':'Previous_Seat_Count','Trainee Count Previous':'Previous_Trainee_Count','Seat Utilization Previous':'Previous_Seat_Utilization_Pct','Seat Count Total':'Total_Seat_Count','Trainee Count Total':'Total_Trainee_Count','Seat Utilization Total':'Total_Seat_Utilization_Pct','Trainee Count Previous (2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df[:df[df.iloc[:,1].str.lower().str.contains('total',na=False)==True].index[0]]
                        df['ITI_Type']=it
                        df['State']=state
                        df['District']=district
                        df=df[['Sector_Name','Trade_Name','ITI_Type','State','District','NSQF_Level','Duration','ITI_Count','Current_Seat_Count', 'Current_Trainee_Count',
                               'Current_Seat_Utilization_Pct', 'Previous_Seat_Count',
                               'Previous_Trainee_Count', 'Previous_Seat_Utilization_Pct',
                               'Total_Seat_Count', 'Total_Trainee_Count', 'Total_Seat_Utilization_Pct',
                               'Previous_Trainee_Count_2nd_Year']]
                        df['Relevant_Date']=today.date()
                        df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df.to_sql(name='NVCT_TRADE_STATISTICS_MONTHLY_DATA',if_exists='append',index=False,con=engine)
                except:
                    driver.quit()
                    time.sleep(1)
                    driver = webdriver.Chrome(executable_path=chrome_driver_path)
                    driver.get(url) 
                    time.sleep(5)
                    Selectit(iti_type_id,it,driver)
                    Selectit(state_id,state,driver)
                    Selectit(district_id,district,driver)
                    driver.execute_script("document.body.style.zoom='80%'")
                    time.sleep(3)
                    driver.execute_script("window.scrollTo(0, 50)")
                    select = wait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'ctl00$cphBody$btnSearch')))
                    driver.execute_script("arguments[0].click();", select)
                    time.sleep(5)
                    df = pd.read_html(driver.page_source)[2]
                    time.sleep(1)
                    if 'no record' in df.iloc[1,0].lower():
                        print('No record')
                    else:
                        df = df[df[df.iloc[:,0].str.lower().str.contains('sector name',na=False)==True].index[1]:].reset_index(drop=True)
                        df = df.rename(columns=df.loc[0])
                        df=df[1:].reset_index(drop=True)
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','SeatCountCurrent':'Current_Seat_Count','TraineeCountCurrent':'Current_Trainee_Count','SeatUtilizationCurrent':'Current_Seat_Utilization_Pct','SeatCountPrevious':'Previous_Seat_Count','TraineeCountPrevious':'Previous_Trainee_Count','SeatUtilizationPrevious':'Previous_Seat_Utilization_Pct','SeatCountTotal':'Total_Seat_Count','TraineeCountTotal':'Total_Trainee_Count','SeatUtilizationTotal':'Total_Seat_Utilization_Pct','Trainee CountPrevious(2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        df=df.rename(columns={'Sector Name':'Sector_Name','Trade Name':'Trade_Name','NSQF Level':'NSQF_Level','ITI Count':'ITI_Count','Seat Count Current':'Current_Seat_Count','Trainee Count Current':'Current_Trainee_Count','Seat Utilization Current':'Current_Seat_Utilization_Pct','Seat Count Previous':'Previous_Seat_Count','Trainee Count Previous':'Previous_Trainee_Count','Seat Utilization Previous':'Previous_Seat_Utilization_Pct','Seat Count Total':'Total_Seat_Count','Trainee Count Total':'Total_Trainee_Count','Seat Utilization Total':'Total_Seat_Utilization_Pct','Trainee Count Previous (2nd Year)':'Previous_Trainee_Count_2nd_Year'})
                        
                        df=df[:df[df.iloc[:,1].str.lower().str.contains('total',na=False)==True].index[0]]

                        df['ITI_Type']=it
                        df['State']=state
                        df['District']=district
                        df=df[['Sector_Name','Trade_Name','ITI_Type','State','District','NSQF_Level','Duration','ITI_Count','Current_Seat_Count', 'Current_Trainee_Count',
                               'Current_Seat_Utilization_Pct', 'Previous_Seat_Count',
                               'Previous_Trainee_Count', 'Previous_Seat_Utilization_Pct',
                               'Total_Seat_Count', 'Total_Trainee_Count', 'Total_Seat_Utilization_Pct',
                               'Previous_Trainee_Count_2nd_Year']]
                        df['Relevant_Date']=today.date()
                        df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df.to_sql(name='NVCT_TRADE_STATISTICS_MONTHLY_DATA',if_exists='append',index=False,con=engine)

                connection.execute("update NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS set Status = 'Done' where ITI_TYPE='"+it+"' and State = '"+state+"' and District = '"+district+"'")
                connection.execute("commit")
                
                try:
                    driver.find_element(By.XPATH,f"//option[text()='{district}']").click()
                    time.sleep(2)
                except:
                    pass
                driver.find_element(By.XPATH,f"//option[text()='{state}']").click()
                time.sleep(2)
    
            query = "delete from NVCT_TRADE_STATISTICS_MONTHLY_DATA_STATUS"
            connection.execute(query)
            connection.execute('commit')
        else:
            print('Data has been collected')

        connection.close()
        engine = adqvest_db.db_conn()
        connection = engine.connect()
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
           