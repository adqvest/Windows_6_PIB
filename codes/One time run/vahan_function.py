
#!/usr/bin/env python
# coding: utf-8

# In[1]:



import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from bs4 import BeautifulSoup

import re
import ast
import datetime as datetime
from pytz import timezone
import requests

import numpy as np
import time

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


# In[2]:


engine = adqvest_db.db_conn()
connection = engine.connect()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'VAHAN_TABLES'




chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
download_file_path = r"C:\Users\Administrator\vahan"
os.chdir(download_file_path)

def init_driver(chrome_driver=chrome_driver_path, download_file_path = download_file_path):

    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
        }
    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get("https://vahan.parivahan.gov.in/vahan4dashboard/")
    driver.maximize_window()
    return driver

def clicking_on_view_summary(driver):
    #************* click on summary view ************************************
    limit = 0
    while True:
        try:
            driver.find_element_by_xpath("//div[@id='j_idt17']//div[@class='ui-selectonemenu-trigger ui-state-default ui-corner-right']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
            time.sleep(1)
            driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='Summary View']").click()
            time.sleep(15)
            break
        except:
            limit = limit + 1
            driver.refresh()
            print("error while clicking summary vire")
            if(limit>8):
                driver.quit()
                raise Exception('error in init_driver while clicking on summary view')
            time.sleep(3)
    return driver

def clicking_actual_value(driver):
    #************* taking actual value from dropdown input ******************
    limit = 0
    while True:
        try:
            driver.find_element_by_xpath("//div[@id='j_idt46']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
            time.sleep(2)
            driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all ui-state-highlight'][@data-label='Actual Value']").click()
            time.sleep(1)
            break
        except:
            limit = limit + 1
            print('error')
            if(limit>8):
                driver.quit()
                raise Exception('error in init_driver')

            time.sleep(3)

    return driver


################################## function to clean data and write it into database

def pre_process(filename, type_):
    if('category' in type_.lower()):
        vahan = pd.read_excel(filename)
        vahan.columns = list(range(vahan.shape[1]))
        vahan.drop(0, axis=1, inplace=True)
        vahan.columns = ['Vehicle_Category', 'Total']
        vahan['Total'] = vahan['Total'].map(str).str.replace(',','')
        vahan['Total'] = vahan['Total'].astype(float)
        vahan['Relevant_Date'] = datetime.date(2016,9,30)
        vahan['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        os.remove(filename)
        return vahan

    elif('class' in type_.lower()):
        vahan = pd.read_excel(filename)
        vahan.columns = list(range(vahan.shape[1]))
        vahan.drop(0, axis=1, inplace=True)
        vahan.columns = ['Vehicle_Class', 'Total']
        vahan['Total'] = vahan['Total'].map(str).str.replace(',','')
        vahan['Total'] = vahan['Total'].astype(float)
        vahan['Relevant_Date'] = datetime.date(2016,9,30)
        vahan['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        os.remove(filename)
        return vahan

    elif('fuel' in type_.lower()):
        vahan = pd.read_excel(filename)
        vahan.columns = list(range(vahan.shape[1]))
        vahan.drop(0, axis=1, inplace=True)
        vahan.columns = ['Fuel_Type', 'Total']
        vahan['Total'] = vahan['Total'].map(str).str.replace(',','')
        vahan['Total'] = vahan['Total'].astype(float)
        vahan['Relevant_Date'] = datetime.date(2016,9,30)
        vahan['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        os.remove(filename)
        return vahan


################################################### downloading file and calling saving and pre-procesing function ###########################
def download_file(driver, type_ , x, st):
    ###########################################################################
    limit =0
    while True:
        try:
            driver = click_down_button(driver)
            time.sleep(1)
            # print("//ul[@id='datatableCategoryWise:dropdown3_items']//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='"+type_+"']")
            driver.find_element_by_xpath("//ul[@id='datatableCategoryWise:dropdown3_items']//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='"+type_+"']").click()

            time.sleep(1)
            break
        except:
            if(('Category Wise' in type_) ):
                break
            limit = limit + 1
            if(limit>8):
                raise Exception('error in download_file 1')

            time.sleep(3)
    ###########################################################################
    limit =0
    while True:
        try:
            # if(type_=='Category Wise'):
            driver.find_element_by_xpath("//a[@id='datatableCategoryWise:csv']").click()
            # else:
            #     driver.find_element_by_xpath("//a[@id='datatableSelectionWise:csv']").click()

            time.sleep(1)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in download_file 2')

            time.sleep(3)
    ###########################################################################
    # limit =0
    # while True:
    #     try:
    #         if(type_=='Category Wise'):
    #             break
    #         driver.find_element_by_xpath("//button[@id='datatableSelectionWise:j_idt189']").click()
    #
    #         time.sleep(1)
    #         break
    #     except:
    #         limit = limit + 1
    #         if(limit>8):
    #             raise Exception('error in download_file 3')
    #
    #         time.sleep(3)
    time.sleep(1)
    filename = os.listdir()[0]
    vahan = pre_process(filename, type_)
    try:
        os.remove(filename)
        print("try deleting downloaded registration file ")
    except:
        pass
    print('#############')
    return vahan


def get_date():

    break_year = 2016
    mon = 4
    year = 2012
    dates = []
    while True:

        start = datetime.date(year, mon, 1)

        if(year==break_year):
            break
        end = datetime.date(year+1,mon-1,calendar.monthrange(year+1,mon-1)[1])
        dates.append([start,end])

        year = year + 1
    print(dates)
    return dates


def fill_date(driver, date):
    start_year = date[0].strftime('%Y')
    start_mon = date[0].strftime('%b')

    end_year = date[1].strftime('%Y')
    end_mon = date[1].strftime('%b')
    end_date = int(date[1].strftime('%d'))

    limit = 0
    while True:
        try:
            driver.find_element_by_xpath("//input[@id='id_fromDate_input']").click()
            driver.find_element_by_xpath("//select[@class='ui-datepicker-year']/option[text()="+start_year+"]").click()
            driver.find_element_by_xpath("//select[@class='ui-datepicker-month']/option[text()='"+start_mon+"']").click()
            driver.find_element_by_xpath("//a[@class='ui-state-default'][text()=1]").click()

            time.sleep(1)
            break
        except:

            limit = limit + 1
            if(limit>5):
                if((end_mon==today.strftime('%b')) & (end_year==today.strftime('%Y'))):
                    break
            if(limit>8):
                raise Exception('error in fill_date 1')

            time.sleep(3)


    #************* filling upto date ******************
    limit =0
    while True:
        try:

            driver.find_element_by_xpath("//input[@id='id_uptoDate_input']").click()
            driver.find_element_by_xpath("//select[@class='ui-datepicker-year']/option[text()="+end_year+"]").click()
            driver.find_element_by_xpath("//select[@class='ui-datepicker-month']/option[text()='"+end_mon+"']").click()
            driver.find_element_by_xpath("//a[@class='ui-state-default'][text()="+str(end_date)+"]").click()
            time.sleep(2)
            break
        except:


            limit = limit + 1
            if(limit>5):
                break


            time.sleep(3)

    print('completed till this part')
    return driver

def states(driver):
    while True:
        try:
            soup = BeautifulSoup(driver.page_source, 'lxml')
            state_info = {}
            state_option = soup.findAll('select', attrs={'id':'j_idt39_input'})[0].findAll('option')
            for x in state_option:
                val = x.attrs['value']
                if(val == '-1'):
                    continue
                else:
                    state_info[val] = x.text

            time.sleep(5)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in states')

            time.sleep(3)
    return state_info


def fill_state(driver, st, state_info):
    limit =0
    while True:
        try:
            print('entered this part')
            driver.find_element_by_xpath("//div[@id='j_idt39']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
            time.sleep(1)
            driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + state_info[st] + "']").click()
            #print(state)
            time.sleep(1)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in fill_state')

            time.sleep(3)
    return driver


def rtos(driver):
    #****************** taking out all rto office information for particular state automatically and saving into dictionary ********************
    limit = 0
    while True:
        try:
            soup = BeautifulSoup(driver.page_source, 'lxml')
            rto = soup.findAll('select',attrs={'id':'selectedRto_input'})[0].findAll('option')
            rto_info = {}
            for x in rto:
                text = x.text
                x = x.attrs
                if(x['value'] == '-1'):
                    continue
                else:
                    rto_info[int(x['value'])] = text
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in rtos')

            time.sleep(3)
    return rto_info


def fill_rto(driver, rto, rto_info):
    limit =0
    while True:
        try:
            print('in rto')
            driver.find_element_by_xpath("//div[@id='selectedRto']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
            time.sleep(0.5)
            driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + rto_info[rto] + "']").click()
            time.sleep(0.5)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in fill_rto')

            time.sleep(3)
    return driver


def click_more_registration(driver):
    limit =0
    while True:
        try:
            driver.find_element_by_xpath("//a[@id='j_idt58']").click()
            time.sleep(1)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in click_more_registration')

            time.sleep(3)
    return driver




def inside_rto_registration(driver):
    limit =0
    while True:
        try:
            driver.find_element_by_xpath("//a[@id='datatable_rtoWise:0:j_idt136:1:j_idt138']").click()
            time.sleep(1)
            no_element = False
            break
        except NoSuchElementException as e:
            if(limit>1):
                no_element = True
                break
            limit = limit + 1
            time.sleep(1)
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in inside_rto_registration')

            time.sleep(3)

    return [driver, no_element]






############################
def click_submit(driver):
    #************* clicking on submit after filling dates ******************
    limit = 0

    while True:
        try:
            driver.find_element_by_xpath("//button[@id='j_idt44']").click()
            time.sleep(1)
            break
        except:

            limit = limit + 1
            if(limit>8):
                raise Exception('error in click_submit')

            time.sleep(3)

    return driver


def click_down_button(driver):
    limit =0
    while True:
        try:
            driver.find_element_by_xpath("//div[@id='datatableCategoryWise:dropdown3']//div[@class='ui-selectonemenu-trigger ui-state-default ui-corner-right']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
            time.sleep(1)
            break
        except:
            limit = limit + 1
            if(limit>8):
                raise Exception('error in click_down_button')

            time.sleep(3)

    return driver

def clean_fun(x):
    date = x.split('(')[-1].replace(')','').strip()
    code = x.split(date)[0].strip().split('-')[-1].replace('(','').strip()
    rto = x.split(code)[0]
    rto = re.sub('-$', '', rto.strip()).strip()
    date = datetime.datetime.strptime(date, '%d-%b-%Y').date()
    return (date, code, rto)

def rto_match(x):
    x = x.upper()
    x = re.sub(r'  +',' ',x)
    x = x.strip()
    return x


def table_check():
    query = "SELECT * FROM information_schema.tables WHERE table_schema = 'AdqvestDB' AND TABLE_NAME in ('VAHAN_CATEGORY_YEAR_WISE_DATA_3')"
    df = pd.read_sql(query, con=engine)
    if(df.shape[0]>0):
        return True
    else:
        return False


#******************************************************** main function *************************************************************************************************
def main_selenium():
    try:
        os.chdir(download_file_path)
        print(os.listdir())
        for i in os.listdir():
            os.remove(i)
        driver = init_driver()
        driver = clicking_on_view_summary(driver)
        # driver = clicking_actual_value(driver)
        dates = get_date()
        for date in dates:

            state_done = pd.read_sql("select * from AdqvestDB.VAHAN_STATE_DONE_3 where Relevant_Date='" + date[1].strftime('%Y-%m-%d') + "'", con=engine)
            if(state_done.shape[0]==0):
                print('go ahead')
            elif(state_done['Date_Status'].isnull().all()):
                print('go ahead')
            else:
                print('skipping date ',date[0],' cause data already extracted')
                continue
            driver = fill_date(driver, date)
            state_info = states(driver)

            print(state_info)
            if(state_done.shape[0]==0):

                state_done_info = {'Relevant_Date':date[1],'State':[x.split('(')[0] for x in list(state_info.values())]}
                state_done_info = pd.DataFrame(state_done_info)
                print(state_done_info)
                state_done_info.to_sql('VAHAN_STATE_DONE_3', con=engine, if_exists='append', index=False)
            for _, row in state_done[state_done['State_Status'].notnull()].iterrows():
                key = list(state_info.keys())[[x.split('(')[0] for x in list(state_info.values())].index(row['State'])]
                del state_info[key]

            print(state_info)
            for st in state_info.keys():
                driver = fill_state(driver,st,state_info)
                state = state_info[st]
                state = state.split('(')[0]
                print('check 1')
                query = "select State, RTO_Office_Raw FROM VAHAN_CATEGORY_YEAR_WISE_DATA_3 WHERE State='" + state + "' and Relevant_Date = '" + date[1].strftime('%Y-%m-%d') + "'"
                category_existing = pd.read_sql(query, engine)


                category_rto_list = category_existing['RTO_Office_Raw'].tolist()

                remove_rto = set(category_rto_list)
                print(remove_rto)

                rto_info = rtos(driver=driver)
                for row in remove_rto:
                    key = list(rto_info.keys())[[rto_match(rt) for rt in list(rto_info.values())].index(row)]
                    print(row,' : ', rto_info[key])
                    del rto_info[key]


                for x in rto_info.keys():
                    print('state - ',state)
                    print('rto - ', rto_info[x])

                    driver = fill_rto(driver=driver, rto=x, rto_info=rto_info)
                    driver = click_submit(driver)
                    driver = click_more_registration(driver=driver)

                    ls = inside_rto_registration(driver)
                    driver = ls[0]
                    no_element_registration = ls[1]
                    curr_rto_name = rto_match(rto_info[x])
                    if(no_element_registration==False):
                        for type_ in ['Category Wise']:
                            if(type_ == 'Category Wise'):
                                if((category_existing.shape==0) | (curr_rto_name not in category_rto_list)):
                                    vahan = download_file(driver, type_ , x, st)
                                    vahan['State'] = state
                                    vahan['RTO_Office_Raw'] = rto_info[x]
                                    vahan['RTO_Office_Raw'] = vahan['RTO_Office_Raw'].str.upper()
                                    vahan['RTO_Office_Raw'] = vahan['RTO_Office_Raw'].apply(lambda x:re.sub(r'  +',' ',x))
                                    vahan['RTO_Office_Raw'] = vahan['RTO_Office_Raw'].str.strip()
                                    rto_fun_data = vahan['RTO_Office_Raw'].map(clean_fun)
                                    vahan['RTO_Date'] = rto_fun_data.apply(lambda x:x[0])
                                    vahan['RTO_Code'] = rto_fun_data.apply(lambda x:x[1])
                                    vahan['RTO_Office'] = rto_fun_data.apply(lambda x:x[2])
                                    vahan['Relevant_Date'] = date[1]
                                    vahan.to_sql(name='VAHAN_CATEGORY_YEAR_WISE_DATA_3',con=engine,if_exists='append',index=False)
                                    #category_data = pd.concat([category_data, vahan])
                                else:
                                    print('skip category')

                    else:
                        no_element_registration = False



                connection.execute("update VAHAN_STATE_DONE_3 set State_Status = 'Close' , Runtime='"+(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')+"' where Relevant_Date='" + date[1].strftime('%Y-%m-%d') + "' and State='" + state + "'")
                connection.execute('commit')

            connection.execute("update VAHAN_STATE_DONE_3 set Date_Status = 'Close' where Relevant_Date='" + date[1].strftime('%Y-%m-%d') + "'")
            connection.execute('commit')
            raise Exception("manully generated error")


        driver.quit()
    except:
        try:
            driver.quit()
        except:
            pass
        raise Exception('error in main_selenium')
