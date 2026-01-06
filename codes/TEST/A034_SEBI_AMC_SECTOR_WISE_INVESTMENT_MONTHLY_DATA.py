# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 13:42:55 2022

@author: Abdulmuizz
"""

import datetime
import os
import re
import sys
import time
import warnings

import pandas as pd
import requests
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from pytz import timezone

warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import calendar
import adqvest_db
import JobLogNew as log
from dateutil.relativedelta import relativedelta
import ClickHouse_db
import boto3
from botocore.config import Config
import adqvest_s3
from adqvest_robotstxt import Robots
robot = Robots(__file__)
client = ClickHouse_db.db_conn()
import MySql_To_Clickhouse as MySql_CH

def date_value(x):
    x = x.strip()
    x = datetime.datetime.strptime(x, '%b%y').date()
    return datetime.date(x.year, x.month, calendar.monthrange(x.year, x.month)[1])

def getSoup(category, start_date):
    pw = sync_playwright().start()
    browser = pw.firefox.launch(headless = False)
    page = browser.new_page()
    
    # All possible combinations
    run_month = start_date.strftime('%b%y').lower()
    run_month_2 = start_date.strftime('%B%y').lower()
    run_month_3 = start_date.strftime('%b%Y').lower()
    run_month_4 = start_date.strftime('%B%Y').lower()
    year = str(start_date.year)
    print(run_month, category)

    robot.add_link(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month}.html')
    page.goto(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month}.html')
    time.sleep(5)
    soup = BeautifulSoup(page.content())
    cond = True
    if soup.find('div', class_='error-page-heading') != None:
        robot.add_link(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_2}.html')
        page.goto(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_2}.html')
        time.sleep(5)
        soup = BeautifulSoup(page.content())
        
        if soup.find('div', class_='error-page-heading') != None:
            robot.add_link(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_3}.html')
            page.goto(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_3}.html')
            time.sleep(5)
            soup = BeautifulSoup(page.content())

            if soup.find('div', class_='error-page-heading') != None:
                robot.add_link(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_4}.html')
                page.goto(f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{category}{run_month_4}.html')
                time.sleep(5)
                soup = BeautifulSoup(page.content())

                if soup.find('div', class_='error-page-heading') != None:
                    print('Data not published')
                    cond = False

    if cond != False:
        soup = BeautifulSoup(page.content(), "html.parser")
        return pw, page, soup, cond
    else:
        return pw, page, soup, cond

def S3_Upload(pw_session, page, start_date):
    global ACCESS_KEY_ID
    global ACCESS_SECRET_KEY
    global BUCKET_NAME
    year = str(start_date.year)
    try:
        soup = BeautifulSoup(page.content())
        file_name = soup.findAll('body')[0].findAll('strong')[0].findAll('a')[0]['href'].replace(' ','%20')
        link = f'https://www.sebi.gov.in/statistics/mutual-fund/deployment/{year}/{file_name}'

        with page.expect_download() as download_info:
            # Perform the action that initiates download
            page.get_by_text("Download").click()
            time.sleep(5)
        download = download_info.value

        download.save_as("C:/Users/Administrator/Junk/" + download.suggested_filename)
        file_name = download.suggested_filename
        robot.add_link(link)

        data =  open("C:/Users/Administrator/Junk/" + file_name, 'rb')
        s3 = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=ACCESS_SECRET_KEY,
            config=Config(signature_version='s3v4',region_name = 'ap-south-1')
        )
        s3.Bucket(BUCKET_NAME).put_object(Key='MF_Deployment_of_Funds/'+file_name, Body=data)
        data.close()

        print('File Uploaded: ',file_name)
        if os.path.exists("C:/Users/Administrator/Junk/" + file_name):
            os.remove("C:/Users/Administrator/Junk/" + file_name)
        else:
            print('passed')
            pass
    except:
        pass
    # pw_session.stop()


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'SEBI_AMC_SECTOR_WISE_INVESTMENT_MONTHLY_DATA_2_TABLES'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    ## S3 Credentials
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    BUCKET_NAME = 'adqvests3bucket'


    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        max_date = pd.read_sql("select max(Relevant_Date) as Max from SEBI_AMC_SECTOR_WISE_INVESTMENT_MONTHLY_DATA",engine)["Max"][0]
        start_date = datetime.date(max_date.year,max_date.month,1)
        end_date = today.date()


        debt_df = pd.DataFrame()
        equity_df = pd.DataFrame()
        reit_df = pd.DataFrame()

        bool1 = True
        while start_date < end_date:

            run_month = start_date.strftime('%b%y').lower()

            # ---------------------------------------- DEBT -------------------------------------------------

            pw_session, page, soup, cond = getSoup('debt', start_date)
            
            if cond == True:
                # S3 Upload
                if bool1 == True:
                    pass
                else:
                    S3_Upload(pw_session, page, start_date)

                debt_details = []
                debt_items = soup.findAll('body')[0].findAll('tr')[3:-1]
                if debt_items != []:
                    for i in debt_items:
                        j = i.findAll('td')
                        if ('Money Market Instrument' in j[0].text) or ('Securitised Debt' in j[0].text) or ('All Mutual Funds' in j[0].text): # Unnecessary rows
                            continue
                        temp = {
                            'Category' : 'Debt',
                            'Sector' : j[0].text,
                            'Sub_Sector' : j[1].text,
                            'AUM_Cr' : j[10].text,
                            'Relevant_Date' : date_value(run_month)
                        }
                        debt_details.append(temp)
                    debt_temp_df = pd.DataFrame(debt_details)
                    debt_temp_df = debt_temp_df.replace('\xa0',np.nan)
                    debt_temp_df['Sector'] = debt_temp_df['Sector'].ffill()
                    debt_temp_df.dropna(subset = ['AUM_Cr'], inplace = True)
                    debt_df = pd.concat([debt_df,debt_temp_df])
                    print("Debt done for :", run_month)

            pw_session.stop()

            # ----------------------------------- EQUITY --------------------------------------------------

            pw_session, page, soup, cond = getSoup('equity', start_date)

            equity_details = []
            equity_items = soup.findAll('body')[0].findAll('tr')[3:-1]
            if equity_items != []:
                for i in equity_items:
                    j = i.findAll('td')
                    temp = {
                        'Category' : 'Equity',
                        'Sector' : j[0].text,
                        'Sub_Sector' : np.nan,
                        'AUM_Cr' : j[1].text,
                        'Relevant_Date' : date_value(run_month)
                    }
                    equity_details.append(temp)
                equity_temp_df = pd.DataFrame(equity_details)
                equity_df = pd.concat([equity_df,equity_temp_df])
                print("Equity done for :", run_month)

            pw_session.stop()
            # --------------------------------------- REIT -------------------------------------------------

            pw_session, page, soup, cond = getSoup('reit', start_date)
            if cond == True:
                reit_details = []
                reit_items = soup.findAll('body')[0].findAll('tr')[2:]
                if reit_items != []:
                    for i in reit_items:
                        j = i.findAll('td')
                        temp = {
                            'Category' : j[0].text.replace('Amount (in Crores)','').strip(),
                            'Sector' : np.nan,
                            'Sub_Sector' : np.nan,
                            'AUM_Cr' : j[1].text,
                            'Relevant_Date' : date_value(run_month)
                        }
                        reit_details.append(temp)
                    reit_temp_df = pd.DataFrame(reit_details)
                    reit_df = pd.concat([reit_df,reit_temp_df])
                    print("REIT done for :", run_month)

            start_date += relativedelta(months= 1)
            bool1 = False
            pw_session.stop()

        final_df = pd.concat([debt_df,equity_df,reit_df])
        final_df['AUM_Cr'] = pd.to_numeric(final_df['AUM_Cr'].apply(lambda x: x.replace(',','')))
        print('this is:',list(final_df['Sector']))
        final_df['Sector'] = final_df['Sector'].replace('  +', ' ', regex = True).str.replace('&','AND').str.strip()
        final_df['Sub_Sector'] = final_df['Sub_Sector'].replace('  +', ' ', regex = True).str.strip()
        final_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

        Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from SEBI_AMC_SECTOR_WISE_INVESTMENT_MONTHLY_DATA",engine)
        Latest_Date = Latest_Date["Max"][0]
        final_df = final_df[final_df['Relevant_Date'] > Latest_Date]

        if final_df.empty:
            print('No Data Found')
        else:
            final_df.to_sql(name = "SEBI_AMC_SECTOR_WISE_INVESTMENT_MONTHLY_DATA" ,if_exists = "append",con = engine,index = False)

        MySql_CH.ch_truncate_and_insert('SEBI_AMC_SECTOR_WISE_INVESTMENT_MONTHLY_DATA')


        ############################################### DEBT ALL DATA COLLECTION ####################################################
        print('Running DEBT ALL DATA')


        # Cleanup for the below code to run efficiently and avoid errors
        variables = ['debt_details','debt_df','debt_items','debt_temp_df','temp','final_df']
        for variable in variables:
            if variable in locals():
                del variable



        max_date = pd.read_sql("select max(Relevant_Date) as Max from SEBI_AMC_SECTOR_WISE_INVESTMENT_DEBT_MONTHLY_DATA",engine)["Max"][0]
        start_date = datetime.date(max_date.year,max_date.month,1)
        end_date = today.date()

        debt_df = pd.DataFrame()
        final_df = pd.DataFrame()

        while start_date < end_date:

            run_month = start_date.strftime('%b%y').lower()

            # ---------------------------------------- DEBT -------------------------------------------------

            pw_session, page, soup, cond = getSoup('debt', start_date)
            if cond == True:
                debt_details = []
                debt_items = soup.findAll('body')[0].findAll('tr')[3:-1]
                if debt_items != []:
                    for i in debt_items:
                        j = i.findAll('td')
                        if ('Money Market Instrument' in j[0].text) or ('Securitised Debt' in j[0].text) or ('All Mutual Funds' in j[0].text): # Unnecessary rows
                            continue
                        temp = {
                            'Category' : 'Debt',
                            'Sector' : j[0].text,
                            'Sub_Sector' : j[1].text,
                            'LT_90_Days' : j[2].text,
                            'From_90_To_182_Days' : j[4].text,
                            'From_182_Days_To_1_Year' : j[6].text,
                            'From_1_Year_And_Above' : j[8].text,
                            'Total_AUM_Cr' : j[10].text,
                            'Relevant_Date' : date_value(run_month)
                        }
                        debt_details.append(temp)
                    debt_temp_df = pd.DataFrame(debt_details)
                    debt_temp_df = debt_temp_df.replace('\xa0',np.nan)
                    debt_temp_df['Sector'] = debt_temp_df['Sector'].ffill()
                    debt_temp_df.dropna(subset = ['Total_AUM_Cr'], inplace = True)
                    debt_df = pd.concat([debt_df,debt_temp_df])
                    print("Debt done for :", run_month)

            start_date += relativedelta(months= 1)
            pw_session.stop()

        final_df = debt_df
        for i in list(final_df.columns[final_df.columns.str.contains('AUM_Cr')]):
            final_df[i] = pd.to_numeric(final_df[i].apply(lambda x: x.replace(',','')))
        final_df['Sector'] = final_df['Sector'].replace('  +', ' ', regex = True).str.replace('&','AND').str.strip()
        final_df['Sub_Sector'] = final_df['Sub_Sector'].replace('  +', ' ', regex = True).str.strip()
        final_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

        Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from SEBI_AMC_SECTOR_WISE_INVESTMENT_DEBT_MONTHLY_DATA",engine)
        Latest_Date = Latest_Date["Max"][0]
        final_df = final_df[final_df['Relevant_Date'] > Latest_Date]
        if final_df.empty:
            print('No Data Found')
        else:
            final_df.to_sql(name = "SEBI_AMC_SECTOR_WISE_INVESTMENT_DEBT_MONTHLY_DATA" ,if_exists = "append",con = engine,index = False)

            # Clickhouse upload
            # clickhouse_final_df = pd.read_sql(f'Select * from SEBI_AMC_SECTOR_WISE_INVESTMENT_DEBT_MONTHLY_DATA where Relevant_Date > {Latest_Date}',engine)
            # client.execute("INSERT INTO AdqvestDB.SEBI_AMC_SECTOR_WISE_INVESTMENT_DEBT_MONTHLY_DATA VALUES", clickhouse_final_df.values.tolist())


        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
