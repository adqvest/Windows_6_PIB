import time
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
import time
import re
import itertools
import requests
import sqlalchemy
from pandas.io import sql
import os
import sys
import warnings
warnings.filterwarnings('ignore')
import csv
import calendar
import pdb
import json
from dateutil.relativedelta import relativedelta
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import calendar
import adqvest_db
import ClickHouse_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir(r'C:\Users\sasai\jupyter notebook\ADQVest Capital\Important DB Conn')
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    #os.chdir('/home/ubuntu/AdQvestDir')
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
    table_name = 'EPROCURE_CENTRAL_RANDOM_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:

        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)


        def convert_to_date(x):
            try:
                return datetime.datetime.strptime(x, '%d-%b-%Y %H:%M %p').date()
            except:
                return x


        r = 2
        for n in range(457,10000):
            if r == 1:
                # url = 'https://eprocure.gov.in/cppp/resultoftendersnew/cpppdata/byYzJWc1pXTjBBMTNoMUExM2gxQTEzaDFBMTNoMU1qQXlNdz09QTEzaDFVSFZpYkdsemFHVms='#2023
                url='https://eprocure.gov.in/cppp/resultoftendersnew/cpppdata/byYzJWc1pXTjBBMTNoMUExM2gxQTEzaDFBMTNoMU1qQXlOQT09QTEzaDFVSFZpYkdsemFHVms='
            else:
                i = '?page=' + str(n)
                print('Url:' + str(i))
                page_no = 'Url:' + str(i)
                # url = "https://eprocure.gov.in/cppp/resultoftendersnew/cpppdata/byYzJWc1pXTjBBMTNoMUExM2gxQTEzaDFBMTNoMU1qQXlNdz09QTEzaDFVSFZpYkdsemFHVms=" + str(i)
                url="https://eprocure.gov.in/cppp/resultoftendersnew/cpppdata/byYzJWc1pXTjBBMTNoMUExM2gxQTEzaDFBMTNoMU1qQXlOQT09QTEzaDFVSFZpYkdsemFHVms=" + str(i)
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, slow_mo=1500)
                page = browser.new_page()
                page.goto(url, timeout=30000)

                tend_list = []

                for row_number in range(1, 11): 
                    selector = f"//*[@id='table']/tbody/tr[{row_number}]/td[4]/a"
                    try:
                        page.locator(selector).click()
                        time.sleep(2)  
                        tender_data = pd.read_html(page.content())
                        print(f'entry {row_number} done')
                        tend_list.append(tender_data)

                    except Exception as e:
                        print(f'Error processing entry {row_number}: {e}')
                    finally:
                        page.goto(url, timeout=30000)

                r = 2
            # with sync_playwright() as p:
            #     browser = p.chromium.launch(headless=True,slow_mo=1500)
            #     page = browser.new_page()
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[1]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_1 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[2]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_2 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[3]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_3 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[4]/td[4]/a").click()
            #     time.sleep(1)
            #     print('entry 3 done')
            #     tender_no_4 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[5]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_5 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[6]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_6 = pd.read_html(page.content())
            #     page.goto(url,timeout= 45000)
            #     page.locator("//*[@id='table']/tbody/tr[7]/td[4]/a").click()
            #     time.sleep(1)
            #     print('entry 6 done')
            #     tender_no_7 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[8]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_8 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[9]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_9 = pd.read_html(page.content())
            #     page.goto(url,timeout= 30000)
            #     page.locator("//*[@id='table']/tbody/tr[10]/td[4]/a").click()
            #     time.sleep(1)
            #     tender_no_10 = pd.read_html(page.content())
            #     print('entry 10 done')
            #     r = 2

            # tend_list = [tender_no_1,tender_no_2,tender_no_3,tender_no_4,tender_no_5,tender_no_6,tender_no_7,tender_no_8,tender_no_9,tender_no_10]

            print(tend_list,'this is tender_list')

            df_final = pd.DataFrame()
            
            for i in tend_list:
                df = i[1]
                df2 = df.iloc[:,[3,5]]
                df = df.iloc[:,[0,2]]
                # df = df.dropna()
                drop = df.iloc[:,0][df.iloc[:,0].str.lower().str.contains("please",na = False)].index
                df = df.drop(drop)
                df.columns = ['Variable','Value']
                # print(df)
                # df2 = df2.dropna()
                drop = df2.iloc[:,0][df2.iloc[:,0].str.lower().str.contains("please",na = False)].index
                df2 = df2.drop(drop)
                df2 = pd.DataFrame(df2.iloc[:,:][df2.iloc[:,0].str.lower().str.contains("number|contract",na = False)])
                df2.columns = ['Variable','Value']
                # print(df2)
                final = df.append(df2)
                final['Variable'] = final['Variable'].str.replace('*','').str.replace('(','').str.replace(')','')
                final = final.T
                final.columns = final.loc['Variable']
                final = final.drop(['Variable'])
                final = final.rename_axis(None, axis=1)
                final.reset_index(inplace = True,drop = True)
                # print(final,'final list')
                df_final = df_final.append(final)
            

            df_final['Relevant_Date'] = df_final['Published Date'].apply(convert_to_date)
            df_final = df_final.rename(columns={'Name of the selected bidders':'Name_Of_The_Selected_Bidders','Organisation Name':'Organisation_Name','Tender Ref. No.':'Tender_Ref_No','Tender Description':'Tender_Description','Tender Document':'Tender_Document','Tender Type':'Tender_Type','Published Date':'Published_Date','Contract Date':'Contract_Date','Contract Value ':'Contract_Value','Number of bids received':'Number_Of_Bids_Received','Address of the selected bidders':'Address_of_Selected_Bidders','Date of Completion/Completion Period in Days':'Date_of_Completion'})
            df_final['Published_Date'] = df_final['Published_Date'].apply(convert_to_date)
            df_final['Contract_Date'] = df_final['Contract_Date'].apply(convert_to_date)
            df_final['Date_of_Completion'] = df_final['Date_of_Completion'].apply(convert_to_date)
            today = datetime.datetime.now(india_time)
            df_final['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            df_final = df_final[
                ['Organisation_Name', 'Name_Of_The_Selected_Bidders', 'Tender_Ref_No', 'Tender_Description', 'Tender_Document',
                 'Tender_Type', 'Published_Date', 'Contract_Date', 'Contract_Value', 'Number_Of_Bids_Received',
                 'Address_of_Selected_Bidders', 'Date_of_Completion', 'Relevant_Date', 'Runtime']]
            print(df_final)
            columns = df_final.columns.to_list()
            columns.remove('Runtime')
            unique = df_final.drop_duplicates(subset=columns, keep='first')
            unique.to_sql(name="EPROCURE_CENTRAL_RANDOM_DATA", con=engine, if_exists='append', index=False)
            print('Url:'+str(i)+' done')

    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        # error_msg = str(sys.exc_info()[1])
        error_msg = page_no
        print(error_msg)
        
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if (__name__ == '__main__'):
    run_program(run_by='manual')
