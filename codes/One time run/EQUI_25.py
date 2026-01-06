import pandas as pd
import sqlalchemy
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import datetime as datetime
from datetime import date
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import csv
import calendar
import pdb
import time
import json
from dateutil import parser
import timeit
import warnings
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db

def last_day_of_month(rdate):
    next_month = rdate.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
  
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "EQUIFAX"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
        url = 'https://www.equifax.co.in/legal_notice/suit_filed_details/en_in'
        links_list = []
        links_date = []
        src = requests.get(url)
        content = src.text
        soup = BeautifulSoup(content,'lxml')
        links = soup.findAll('a',attrs = {'class':"name"},href = True)
        for l in links:
            links_list.append(l['href'])
            links_date.append(l.text)
        #links_list = [links_list[0]]
        #links_date = [links_date[0]]
        for i,j in zip(links_date,links_list):
            present_date = last_day_of_month(pd.to_datetime(i,format = '%b %Y').date())
            mon = present_date.strftime('%b')
            yr = present_date.year
            #if Latest_Date != present_date:
            print(i)
            url_ = j
            src = requests.get(url_)
            content = src.text
            soup = BeautifulSoup(content,'lxml')
            pages = soup.find('div',attrs = {'class':"suits-filed-next-btn"})
            page_count = pages.find('span').get_text()
            total = pages.find('span').next_sibling.strip()
            total_count = [int(i) for i in total.split() if i.isdigit()][0]
            page_num = round(total_count/int(page_count))
            print("page count:",page_num) 
            table = pd.DataFrame()
            for p in range(page_num+1):
                url_ = 'https://www.equifax.co.in/full-file/en_in?p_p_id=IndiaFullFile_INSTANCE_YwWP0hUl3X7R&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_cacheability=cacheLevelPage&type=0&month='+str(mon)+'&year='+str(yr)+'&page='+str(p)
                r = requests.get(url_)
                data = r.json()['rows']
                for each in data:
                    df = pd.DataFrame(each).transpose()
                    df.columns = ['Unwanted','Category_Of_Bank_FI', 'Bank', 'Branch','State','Sr_No','Party','Registered_Address','Outstanding_Amount_In_Lacs','Suit', 'Other_Bank', 'Director_1','Director_1_DIN', 'Director_2', 'Director_2_DIN', 'Director_3','Director_3_DIN', 'Director_4','Director_4_DIN', 'Director_5','Director_5_DIN', 'Director_6','Director_6_DIN', 'Director_7','Director_7_DIN', 'Director_8','Director_8_DIN', 'Director_9','Director_9_DIN', 'Director_10','Director_10_DIN', 'Director_11', 'Director_11_DIN', 'Director_12','Director_12_DIN','Director_13','Director_13_DIN','Director_14','Director_14_DIN']
                    table = pd.concat([table,df],axis = 0) 
            table = table[['Category_Of_Bank_FI', 'Bank', 'Branch','State','Party','Registered_Address','Outstanding_Amount_In_Lacs','Suit', 'Other_Bank', 'Director_1','Director_1_DIN', 'Director_2', 'Director_2_DIN', 'Director_3','Director_3_DIN', 'Director_4','Director_4_DIN', 'Director_5','Director_5_DIN', 'Director_6','Director_6_DIN', 'Director_7','Director_7_DIN', 'Director_8','Director_8_DIN', 'Director_9','Director_9_DIN', 'Director_10','Director_10_DIN', 'Director_11', 'Director_11_DIN', 'Director_12','Director_12_DIN','Director_13','Director_13_DIN','Director_14','Director_14_DIN']]
            table['Source'] = 'EQUIFAX' 
            table['Category'] = '25 lacs and above'    
            table["Relevant_Date"] = present_date
            table['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            table.to_sql(name = "CIBIL_EQUIFAX_CRIF_DEFAULTERS_MONTHLY_QUARTERLY_DATA",con = engine,if_exists = 'append',index = False)
            print('data uploaded for',present_date)
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')            
