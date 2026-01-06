
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
import io

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from dateutil.relativedelta import *
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import unidecode
from quantities import units

from clickhouse_driver import Client
client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com', user='default', password='@Dqu&TP@ssw0rd', database='AdqvestDB', port=9000)

no_of_ping = 0


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'VAHAN_MAKER_VS_CATEGORY_INDIA_LEVEL_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        #functions

        def get_months(driver):
            limit = 0
            while True:
                try:
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    # all_months = soup.findAll('div', class_='ui-grid-row resp-month')[0].findAll('a', id=re.compile(r'j_idt(.*?)'))
                    all_months = soup.findAll('li', id=re.compile(r'groupingTable:selectMonth_(.*?)'))
                    try:
                        int(all_months[0].text)
                        all_months = all_months[1:]
                    except:
                        pass

                    month_info = [x.text.strip() for x in all_months]
                    time.sleep(0.5)
                    break
                except:
                    limit = limit + 1
                    if(limit>8):
                        raise Exception('error in get_months')

                    time.sleep(3)
            return month_info


        def click_on_month(driver, mon):
            #************* clicking on submit after filling dates ******************
            global no_of_ping
            limit = 0

            while True:
                try:
                    no_of_ping += 1
                    driver.find_element('xpath',"//div[@class='ui-grid-row resp-month']//a[contains(text(), '" + mon.upper() + "')]").click()
                    time.sleep(2)
                    break
                except:

                    limit = limit + 1
                    if(limit>8):
                        raise Exception('error in click_on_month')

                    time.sleep(3)

            return driver

        def init_driver(chrome_driver,download_file_path):

            prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            options.add_experimental_option('prefs', prefs)

            driver = webdriver.Chrome(executable_path=chrome_driver,options = options)
            driver.get("https://vahan.parivahan.gov.in/vahan4dashboard/")
            robot.add_link("https://vahan.parivahan.gov.in/vahan4dashboard/")
            driver.maximize_window()
            return driver

        def clicking_tabular_summary(driver):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"/html/body/form/div[1]/div/nav/div[2]/ul/li[2]/div/div[3]").click()
                    time.sleep(4)
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//html/body/div[2]/div/ul/li[4]").click()
                    time.sleep(2)
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in clicking_tabular_summary')

                    time.sleep(3)

            return driver

        def choose_y_axis(driver,y_axis):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//label[@id='yaxisVar_label']").click()
                    time.sleep(2)
                    driver.find_element(By.XPATH,"//li[@data-label = '"+y_axis+"']").click()

                    time.sleep(1)
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in choose_y_axis')

                    time.sleep(3)

            return driver

        def choose_x_axis(driver,x_axis):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//label[@id='xaxisVar_label']").click()
                    time.sleep(2)
                    driver.find_element(By.XPATH,"//*[starts-with(@id, 'xaxisVar')]/li[@data-label = '"+x_axis+"']").click()

                    time.sleep(1)
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in choose_x_axis')

                    time.sleep(3)

            return driver

        def click_submit(driver):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//*[@class = 'button-section']/button[@type = 'submit']").click()

                    time.sleep(3)
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in click_submit')

                    time.sleep(3)

            return driver


        def click_year(driver,year):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//div[@id = 'selectedYear']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()

                    time.sleep(1)
                    # driver.find_element(By.XPATH,"//li[@data-label = '"+year+"']").click()
                    driver.find_element(By.XPATH,"//*[@id = 'selectedYear_items']/li[@data-label = '"+year+"']").click()
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in click_submit')

                    time.sleep(3)

            return driver


        def click_month(driver,month):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//div[@id = 'groupingTable:selectMonth']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()

                    time.sleep(1)
                    driver.find_element(By.XPATH,"//li[@data-label = '"+month+"']").click()
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in click_submit')

                    time.sleep(3)

            return driver




        def download_file(driver):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//a[@id = 'groupingTable:xls']").click()
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        driver.quit()
                        raise Exception('error in click_submit')

                    time.sleep(3)
            time.sleep(5)
            return driver




        def clean_fun(x):
            date = x.split('(')[-1].replace(')','').strip()
            code = x.split(date)[0].strip().split('-')[-1].replace('(','').strip()
            rto = x.split(code)[0]
            rto = re.sub('-$', '', rto.strip()).strip()
            date = datetime.datetime.strptime(date, '%d-%b-%Y').date()
            return (date, code, rto)



        def read_file(type_,rel_date,overall = False):
            with open(os.listdir()[0], "rb") as f:
                    file_io_obj = io.BytesIO(f.read())
            vahan_df = pd.read_excel(file_io_obj, engine='openpyxl')
            vahan_df.columns = [x for x in range(vahan_df.shape[1])]
            vahan_df = vahan_df.ffill()
            start_index = vahan_df[vahan_df[0] == "1"].index[0] - 1
            vahan_df.drop(columns = [0],inplace = True)
            vahan_df = vahan_df[start_index:]
            vahan_df.columns = vahan_df.loc[start_index]
            vahan_df.drop(start_index,inplace = True)
            vahan_df.reset_index(drop = True,inplace = True)
            vahan_df.columns = [x.replace(r'\xa0','').strip() for x in vahan_df.columns.tolist()]
            vahan_df.drop_duplicates(inplace=True)



            vahan_full_df = pd.DataFrame()
            col = vahan_df.columns.tolist()[1:]
            for c in col:
                if(type_ == "Maker_Vs_Cat_Vs_Fuel"):
                    df = vahan_df[['Maker', c]]
                    df["Vehicle_Category"] = c


                df = df.rename(columns={c:'Total'})
                vahan_full_df = pd.concat([vahan_full_df, df])
            if(type_ == "Maker_Vs_Cat_Vs_Fuel"):
                vahan_full_df = vahan_full_df[vahan_full_df["Vehicle_Category"].str.lower().str.contains("total") == False]


            for file in os.listdir():
                print('INITIAL')
                #os.close(file)
                print('Close')
                os.remove(file)
                print('Removed')

            vahan_full_df["Relevant_Date"] = rel_date

            vahan_full_df["Runtime"] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: x.replace(",",""))
            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: int(x))

            return vahan_full_df



        def main_function():
            global no_of_ping
            main_limit = 0

            while True:
                try:
                    chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

                    download_file_path = r"C:\Users\Administrator\Vahan_India_Level"

                    os.chdir(r"C:\Users\Administrator\Vahan_India_Level")


                    time.sleep(5)
                    for file in os.listdir():
                        os.remove(file)

                    driver = init_driver(chrome_driver=chrome_driver_path,download_file_path = download_file_path)
                    driver = clicking_tabular_summary(driver)
                    no_of_ping += 1

                    all_years = list(range(today.year - 1, today.year + 1))

                    #Maker vs Category
                    driver = choose_y_axis(driver,'Maker')
                    driver = choose_x_axis(driver,'Vehicle Category')

                    vahan_maker_cat_fuel_rto_level = pd.DataFrame()
                    

                    for year in all_years:
                        driver = click_year(driver, str(year))
                        time.sleep(1)
                        driver = click_submit(driver)
                        month_info = get_months(driver)

                        for mon in month_info:
                            rel_date = datetime.datetime.strptime(str(year) + " " + mon, "%Y %b").date()
                            rel_date = datetime.date(rel_date.year, rel_date.month, calendar.monthrange(rel_date.year, rel_date.month)[1])
                            print("Date :",rel_date)
                            try:
                                print(today)
                            except:
                                pass    
                            if((rel_date.month==today.month) & (rel_date.year==today.year)& (today.day==1)):
                                rel_date = datetime.date(rel_date.year, rel_date.month-1, calendar.monthrange(rel_date.year, rel_date.month-1)[1])
                                print("Date :",rel_date)

                            if ((rel_date.month==today.month) & (rel_date.year==today.year)& (today.day!=1)):
                                
                                rel_date = datetime.date(rel_date.year, rel_date.month, today.day-1)
                                print("Date :",rel_date)    
                            driver = click_month(driver, mon)

                            soup = BeautifulSoup(driver.page_source)
                            if('no records found' in str(soup).lower()):
                                pass
                            else:
                                time.sleep(1)
                                driver = download_file(driver)
                                time.sleep(3)

                                vahan_df_maker_vs_cat_vs_fuel = read_file(type_ = "Maker_Vs_Cat_Vs_Fuel",rel_date = rel_date)
                                print('read successfully')

                                print(vahan_df_maker_vs_cat_vs_fuel)
                                vahan_maker_cat_fuel_rto_level = pd.concat([vahan_maker_cat_fuel_rto_level,vahan_df_maker_vs_cat_vs_fuel])



                    if(len(vahan_maker_cat_fuel_rto_level)>0):
                        vahan_maker_cat_fuel_rto_level = vahan_maker_cat_fuel_rto_level[vahan_maker_cat_fuel_rto_level["Total"]!=0]
                    
                        ''' Data insertion '''

                    engine = adqvest_db.db_conn()
                    connection = engine.connect()

                    query = "delete from AdqvestDB.VAHAN_MAKER_VS_CATEGORY_INDIA_LEVEL_DAILY_DATA"
                    connection.execute(query)
                    connection.execute('commit')

                    # engine = adqvest_db.db_conn()
                    # connection = engine.connect()

                    print("Final DataFrame :",vahan_maker_cat_fuel_rto_level.shape)
                    vahan_maker_cat_fuel_rto_level.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_INDIA_LEVEL_DAILY_DATA", con=engine, index=False, if_exists='append')
                    

                        
                    break
                    
                    

                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                    print(error_msg)
                    main_limit += 1
                    if(main_limit < 10):
                        continue
                    else:
                        driver.quit()
                        raise Exception(error_msg)
                        break
                try:
                    driver.quit()
                except:
                    pass

        today = datetime.datetime.now(india_time)

        all_years = list(range(today.year - 1, today.year + 1))
        main_function()

        


        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
