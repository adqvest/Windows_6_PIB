# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 09:24:53 2020

@author: DELL
"""

from selenium.common.exceptions import NoSuchElementException
import sys
import re
import os
import sqlalchemy
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium import webdriver
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log



india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
job_start_time = datetime.datetime.now(india_time)
table_name = 'AGMARKNET_DATA'
scheduler = ''

con_string = 'mysql+pymysql://abhishek:Abhi%shek3@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/TestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')


def input_fun(driver, st, date):
    global no_of_ping
    mon = date.strftime("%B")
    year = date.year

    st_xpath = '//select[@name="ctl00$cphBody$cboState"]'
    time.sleep(1)
    no_of_ping += 1
    driver.find_elements_by_xpath(st_xpath + "//option[contains(text(), '" + st + "')]")[0].click()
    time.sleep(2)

    mon_xpath = '//select[@name="ctl00$cphBody$cboMonth"]'
    time.sleep(1)
    no_of_ping += 1
    driver.find_elements_by_xpath(mon_xpath + '//option[contains(text(), "' + mon + '")]')[0].click()
    time.sleep(2)

    year_xpath = '//select[@name="ctl00$cphBody$cboYear"]'
    time.sleep(1)
    no_of_ping += 1
    driver.find_elements_by_xpath(year_xpath + '//option[contains(text(), "' + str(year) + '")]')[0].click()
    time.sleep(2)

    return driver

def clean_up(chrome,st):

        html=chrome.page_source
        df=pd.read_html(html)
        dsoup=BeautifulSoup(html,'html.parser')
        select = dsoup.find('font', {'color':'Maroon'})
        date=select.contents[0].strip()
        df=df[3]
        cond1=(df['Market'].str.lower().str.contains('group')==False) & ((df['Market']==df['Arrivals'])==True)
        cond2=(df['Market'].str.lower().str.contains('group')==True) & ((df['Market']==df['Arrivals'])==True)
        df['Product']=np.where(cond1,df['Market'],None)
        df['Group']=np.where(cond2,df['Arrivals'],None)
        col=['Product','Group']
        df[col]=df.loc[:,col].ffill()
        df.dropna(axis=0,inplace=True)
        df['Group']=df['Group'].str.replace('Group:','')
        df=df.drop_duplicates(keep=False)
        df = df.query("Market != Arrivals")
        new_cols = df.columns.to_list()
        new_cols = [x.replace(" ",'_').replace("-","_") for x in new_cols]
        df.columns = new_cols
        format_str = '%d/%m/%Y' # The format
        datetime_obj = datetime.datetime.strptime(date, format_str)
        df=df.rename(columns={'Arrivals':'Arrivals_String'})
        df['Arrivals']=np.where(df['Arrivals_String']=='NR',0,df['Arrivals_String'])
        df['Relevant_Date']=datetime_obj.strftime("%Y-%m-%d")
        df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
        df['Last_Updated'] = ''
        df['State']=st
        df[['Arrivals','Minimum_Prices', 'Maximum_Prices','Modal_Prices']]=df[['Arrivals','Minimum_Prices', 'Maximum_Prices','Modal_Prices']].apply(pd.to_numeric,errors='coerce')
        df['Relevant_Date'] =  pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
        df=df[['State','Group','Product','Market', 'Arrivals_String','Arrivals', 'Unit_of_Arrivals', 'Variety', 'Minimum_Prices',
               'Maximum_Prices', 'Modal_Prices', 'Unit_of_Price'
               , 'Relevant_Date', 'Runtime', 'Last_Updated']]

        return df

def update(state,driver):

    global no_of_ping
    try:
    #    driver=webdriver.Chrome(executable_path= r"D:\Adqvest\Selenium Extension\chromedriver.exe")
        query = "select max(Relevant_Date) as RD from TestDB.AGMARKNET_PAST_DATA where State='" + state + "'"
        max_date = pd.read_sql(query,con = engine)
        if(max_date['RD'].isnull().all()):
            start_date = datetime.date(2017, 4, 1)
        else:
            start_date = max_date['RD'][0]
        end_date = datetime.date(yesterday.year,yesterday.month,yesterday.day)
        while (start_date<=end_date):
            date =start_date
            driver = input_fun(driver, state, date)
            driver.implicitly_wait(10)
            xpath = '//*[@id="cphBody_Calendar1"]/tbody//a'
            eles = driver.find_elements_by_xpath(xpath)
            all_text = []
            for ele in eles:
                all_text.append(ele.text)
            if all_text == []:
                continue
            tracker_all_dates = []
            all_text_tracker = []
            for text in all_text:
                rel_date = datetime.date(date.year, date.month, int(text))
                if(rel_date>=start_date):
                    all_text_tracker.append(text)
                    print(text,rel_date)
                    tracker_all_dates.append(rel_date)    #                                no_of_ping += 1
            for text in all_text_tracker:
                code_rel_date = datetime.date(date.year, date.month, int(text))
                print(code_rel_date)
                driver = input_fun(driver, state, date)
                print(text)
                rel_date = date.strftime("%B %Y")
                time.sleep(1)
                limit = 0
                while True:
                    try:
                        cal = driver.find_elements_by_xpath('//*[@id="cphBody_Calendar1"]/tbody/tr[1]/td/table/tbody//td')[0].text
                        if(cal.strip()==rel_date):
                            break
                        else:
                            raise Exception("Date Mismatch")
                    except:
                        limit += 1
                        if(limit>10):
                            raise Exception("Internet slow")
                        time.sleep(2)
                ele = driver.find_elements_by_xpath(xpath + '[text() = "' + text + '"]')[0]
                no_of_ping += 1
                ele.click()
                time.sleep(2)
                try:
                    xpath_submit='//*[@id="cphBody_btnSubmit"]'
                    time.sleep(3)
                    driver.implicitly_wait(10) # seconds
                    submit = driver.find_element_by_xpath(xpath_submit)
                    no_of_ping += 1
                    submit.click()
                    driver.implicitly_wait(5)
                except NoSuchElementException:
                    runtime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                time.sleep(2)
                output=clean_up(driver,state)
                output.to_sql(name='AGMARKNET_PAST_DATA',con=engine,if_exists='append',index=False)
                time.sleep(2)
                driver.get(site_url)
                start_date = datetime.date(date.year,date.month,1)

                if(st_date_1.month==12):
                    st_date_1 = datetime.date(st_date_1.year + 1, 1, 1)
                else:
                    st_date_1 = datetime.date(st_date_1.year, st_date_1.month + 1, 1)
    except:
        pass

    return driver


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        main_limit=0
        while True:
            try:
                site_url = "https://agmarknet.gov.in/PriceAndArrivals/CommodityDailyStateWise.aspx"
                driver = webdriver.Chrome(r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                driver.get(site_url)
                no_of_ping += 1
                r = requests.Session()
                sesh1 = r.get(site_url)
                no_of_ping += 1
                soup1 = BeautifulSoup(sesh1.content , "lxml")
                states = []
                select = soup1.find('select', id="cphBody_cboState")
                for value in select.stripped_strings:
                    states.append(value)
                states = states[1:]
                states = [e for e in states if e not in ('Lakshadweep', 'Sikkim', 'Dadra and Nagar Haveli','Daman and Diu')]
                print(states)
                for state in states:
                    output = update(state,driver)
                del output
                log.job_end_log(table_name,job_start_time, no_of_ping)
                break
            except:
                main_limit += 1
                if(main_limit>10):
                    break
                    raise Exception("Error in code")
                time.sleep(5)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
