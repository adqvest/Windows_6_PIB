
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
from dateutil.relativedelta import *
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLog as log
import adqvest_db


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
    table_name = 'VAHAN_MAKER_CATEGORY_TABULAR_SUMMARY_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        #functions

        def init_driver(chrome_driver,download_file_path):

            prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
            options = webdriver.ChromeOptions()
            options.add_experimental_option('prefs', prefs)

            driver = webdriver.Chrome(executable_path=chrome_driver,options = options)
            driver.get("https://vahan.parivahan.gov.in/vahan4dashboard/")
            driver.maximize_window()
            return driver

        def clicking_tabular_summary(driver):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element_by_xpath("//div[@class = 'ui-selectonemenu-trigger ui-state-default ui-corner-right']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    time.sleep(2)
                    no_of_ping += 1
                    driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][@data-label='Tabular Summary']").click()
                    time.sleep(1)
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
                    driver.find_element_by_xpath("//label[@id='yaxisVar_label']").click()
                    time.sleep(2)
                    driver.find_element_by_xpath("//li[@data-label = '"+y_axis+"']").click()

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
                    driver.find_element_by_xpath("//label[@id='xaxisVar_label']").click()
                    time.sleep(2)
                    driver.find_element_by_xpath("//*[starts-with(@id, 'xaxisVar')]/li[@data-label = '"+x_axis+"']").click()

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
                    driver.find_element_by_xpath("//*[@class = 'button-section']/button[@type = 'submit']").click()

                    time.sleep(1)
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
                    driver.find_element_by_xpath("//div[@id = 'selectedYear']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()

                    time.sleep(1)
                    # driver.find_element_by_xpath("//li[@data-label = '"+year+"']").click()
                    driver.find_element_by_xpath("//*[@id = 'selectedYear_items']/li[@data-label = '"+year+"']").click()
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
                    driver.find_element_by_xpath("//div[@id = 'groupingTable:selectMonth']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()

                    time.sleep(1)
                    driver.find_element_by_xpath("//li[@data-label = '"+month+"']").click()
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
                    driver.find_element_by_xpath("//a[@id = 'groupingTable:xls']").click()
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


        def get_states(driver):
            limit = 0
            while True:
                try:
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    state_info = {}
                    state_option = soup.findAll('select', attrs={'id':'j_idt31_input'})[0].findAll('option')
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
            global no_of_ping
            limit =0
            while True:
                try:
                    print('entered this part')
                    no_of_ping += 1
                    driver.find_element_by_xpath("//div[@id='j_idt31']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    time.sleep(1)
                    no_of_ping += 1
                    #driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + state_info[st] + "']").click()
                    driver.find_element_by_xpath("//li[@data-label = '"+state_info[st]+"']").click()

                    #print(state)
                    time.sleep(1)
                    break
                except:
                    limit = limit + 1
                    if(limit>8):
                        print('error in fill_state')
                        raise Exception('error in fill_state')

                    time.sleep(3)
            return driver

        def clean_state(x):
            x = x.split('(')[0].strip()
            return x

        def clean_rto(rto):
            rto = rto.upper()
            rto = re.sub(r'  +',' ',rto).strip()
            return rto

        def clean_fun(x):
            date = x.split('(')[-1].replace(')','').strip()
            code = x.split(date)[0].strip().split('-')[-1].replace('(','').strip()
            rto = x.split(code)[0]
            rto = re.sub('-$', '', rto.strip()).strip()
            date = datetime.datetime.strptime(date, '%d-%b-%Y').date()
            return (date, code, rto)

        def get_rtos(driver):
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
            global no_of_ping
            limit =0
            while True:
                try:
                    print('in rto')
                    no_of_ping += 1
                    driver.find_element_by_xpath("//div[@id='selectedRto']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    time.sleep(0.5)
                    no_of_ping += 1
        #             driver.find_element_by_xpath("//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + rto_info[rto] + "']").click()
                    driver.find_element_by_xpath("//li[@data-label = '"+rto_info[rto]+"']").click()
                    time.sleep(0.5)
                    break
                except:
                    limit = limit + 1
                    if(limit>8):
                        raise Exception('error in fill_rto')

                    time.sleep(3)
            return driver


        def read_file(type_):
            vahan_df = pd.read_excel(os.listdir()[0])
            vahan_df.columns = [x for x in range(vahan_df.shape[1])]
            vahan_df = vahan_df.ffill()
            start_index = vahan_df[vahan_df[0] == "1"].index[0] - 1
            vahan_df.drop(columns = [0],inplace = True)
            vahan_df = vahan_df[start_index:]
            vahan_df.columns = vahan_df.loc[start_index]
            vahan_df.drop(start_index,inplace = True)
            vahan_df.reset_index(drop = True,inplace = True)
            vahan_df.columns = [x.replace(r'\xa0','').strip() for x in vahan_df.columns.tolist()]



            vahan_full_df = pd.DataFrame()
            col = vahan_df.columns.tolist()[1:]
            for c in col:
                if(type_ == "Maker_Vs_Cat"):
                    df = vahan_df[['Maker', c]]
                    df["Vehicle_Category"] = c
                elif(type_ == "Maker_Vs_Fuel"):
                    df = vahan_df[['Maker', c]]
                    df["Fuel"] = c
                elif(type_ == "Fuel_Vs_Cat"):
                    df = vahan_df[['Fuel', c]]
                    df["Vehicle_Category"] = c

                df = df.rename(columns={c:'Total'})
                vahan_full_df = pd.concat([vahan_full_df, df])
            if(type_ == "Maker_Vs_Cat"):
                vahan_full_df = vahan_full_df[vahan_full_df["Vehicle_Category"].str.lower().str.contains("total") == False]
            elif(type_ == "Fuel_Vs_Cat"):
                vahan_full_df = vahan_full_df[vahan_full_df["Vehicle_Category"].str.lower().str.contains("total") == False]
            elif(type_ == "Maker_Vs_Fuel"):
                vahan_full_df = vahan_full_df[vahan_full_df["Fuel"].str.lower().str.contains("total") == False]


            for file in os.listdir():
                os.remove(file)

            vahan_full_df["State"] = state
            vahan_full_df["RTO_Office_Raw"] = rto

            rto_fun_data = vahan_full_df['RTO_Office_Raw'].map(clean_fun)
            vahan_full_df['RTO_Date'] = rto_fun_data.apply(lambda x:x[0])
            vahan_full_df['RTO_Code'] = rto_fun_data.apply(lambda x:x[1])
            vahan_full_df['RTO_Office'] = rto_fun_data.apply(lambda x:x[2])

            vahan_full_df["Relevant_Date"] = dates_df["Relevant_Date"][i]

            vahan_full_df["Runtime"] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: x.replace(",",""))
            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: int(x))

            return vahan_full_df




        main_limit = 0

        while True:
            try:
                #chrome_driver_path = r"D:\advaith\Adqvest\chromedriver.exe"
                chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                #download_file_path = r"D:\advaith\Adqvest\Junk"
                download_file_path = r"C:\Users\Administrator\Junk"

                os.chdir(r"C:\Users\Administrator\Junk")

                for file in os.listdir():
                    os.remove(file)

                driver = init_driver(chrome_driver=chrome_driver_path,download_file_path = download_file_path)
                driver = clicking_tabular_summary(driver)


                state_info = get_states(driver)

                ss = [clean_state(x) for x in  list(state_info.values())]
                maker_done = pd.DataFrame(ss, columns=['State'])
                maker_done['Relevant_Date'] = today.date()

                maker_df = pd.read_sql("select * from VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine)
                if(maker_df.empty):
                    maker_done.to_sql("VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine, index=False, if_exists='append')



                # distinct_states = pd.read_sql("select distinct(State) from VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine)
                # state_list = np.array_split(distinct_states,3)[0]["State"].tolist()
                # remove_state = maker_df[(maker_df['State_Status'].notnull()) | (maker_df['State'].isin(state_list) == False)]

                distinct_states = pd.read_sql("select distinct(State) from VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine)

                distinct_states = distinct_states[distinct_states["State"].isin(['Uttar Pradesh'])]

                state_list = distinct_states["State"].tolist()
                remove_state = maker_df[(maker_df['State_Status'].notnull()) | (maker_df['State'].isin(state_list) == False)]


                # remove_state = maker_df[maker_df['State_Status'].notnull()]
                print(state_list)
                for row in set(remove_state['State'].to_list()):
                    print(row)

                    key = list(state_info.keys())[[clean_state(s) for s in list(state_info.values())].index(row)]
                    del state_info[key]


                for st in state_info:
                    driver = fill_state(driver,st,state_info)

                    state = state_info[st]
                    state = clean_state(state)

                    rto_info = get_rtos(driver)

                    rto_info = get_rtos(driver)
                    ss = [clean_rto(x) for x in  list(rto_info.values())]
                    maker_done = pd.DataFrame(ss, columns=['RTO_Office_Raw'])
                    maker_done['State'] = state
                    maker_done['Relevant_Date'] = today.date()
                    maker_df = pd.read_sql("select * from VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine)
                    if(maker_df[maker_df['State']==state]['RTO_Office_Raw'].isnull().all()):
                        maker_done.to_sql("VAHAN_TABULAR_SUMMARY_STATE_DONE", con=engine, index=False, if_exists='append')

                    remove_rto = maker_df[(maker_df['State']==state) & (maker_df['RTO_Office_Raw'].notnull()) & (maker_df['RTO_Status'].notnull())]
                    for row in set(remove_rto['RTO_Office_Raw'].to_list()):
                        key = list(rto_info.keys())[[clean_rto(s) for s in list(rto_info.values())].index(row)]
                        print(row,' : ', rto_info[key])
                        del rto_info[key]




                    for x in rto_info.keys():
                        print('state - ',state)
                        print('rto - ', rto_info[x])
                        driver = fill_rto(driver=driver, rto=x, rto_info=rto_info)

                        rto = rto_info[x]
                        rto = rto.upper()
                        rto = re.sub(r'  +',' ',rto).strip()


                        ####################################################################

                        start_date = pd.read_sql("select max(Relevant_Date) as Relevant_Date from VAHAN_FUEL_VS_CATEGORY_RTO_LEVEL_DATA_Adv where RTO_Office_Raw = '"+rto+"'",engine)["Relevant_Date"][0]
                        if(start_date == None):
                            start_date = datetime.date(2018,1,31)
                            # start_date = datetime.date(2020,7,1)
                        end_date = datetime.date(2020,8,31)
                        date_list = [start_date]
                        while start_date < end_date:

                            start_date = start_date + relativedelta(months=+1)
                            start_date = start_date + relativedelta(day=31)
                            date_list.append(start_date)

                        dates_df = pd.DataFrame({"Relevant_Date":date_list})
                        dates_df["Month"] = dates_df["Relevant_Date"].apply(lambda x: datetime.datetime.strftime(x,"%b").upper())
                        dates_df["Year"] = dates_df["Relevant_Date"].apply(lambda x: datetime.datetime.strftime(x,"%Y"))
                        dates_df.reset_index(drop = True,inplace = True)
                        ####################################################################


                        vahan_maker_cat_rto_level = pd.DataFrame()
                        vahan_fuel_cat_rto_level = pd.DataFrame()
                        vahan_fuel_maker_rto_level = pd.DataFrame()
                        for i in range(len(dates_df)):

                            #Maker vs Category
                            driver = choose_y_axis(driver,'Maker')
                            driver = choose_x_axis(driver,'Vehicle Category')

                            driver = click_year(driver,dates_df["Year"][i])

                            driver = click_submit(driver)

                            driver = click_month(driver,dates_df["Month"][i])

                            time.sleep(1)
                            soup = BeautifulSoup(driver.page_source)
                            if('no records found' in str(soup).lower()):
                                pass
                            else:
                                driver = download_file(driver)

                                vahan_df_maker_vs_cat = read_file(type_ = "Maker_Vs_Cat")
                                print(vahan_df_maker_vs_cat)
                                vahan_maker_cat_rto_level = pd.concat([vahan_maker_cat_rto_level,vahan_df_maker_vs_cat])


                            #Maker vs Fuel vs Category
                            driver = choose_y_axis(driver,'Maker')
                            driver = choose_x_axis(driver,'Fuel')

                            driver = click_year(driver,dates_df["Year"][i])

                            driver = click_submit(driver)

                            driver = click_month(driver,dates_df["Month"][i])

                            time.sleep(1)
                            soup = BeautifulSoup(driver.page_source)
                            if('no records found' in str(soup).lower()):
                                pass
                            else:
                                driver = download_file(driver)

                                vahan_df_maker_vs_fuel = read_file(type_ = "Maker_Vs_Fuel")
                                print(vahan_df_maker_vs_fuel)
                                vahan_fuel_maker_rto_level = pd.concat([vahan_fuel_maker_rto_level,vahan_df_maker_vs_fuel])

                            # #Fuel vs Category
                            # driver = choose_y_axis(driver,'Fuel')
                            # driver = choose_x_axis(driver,'Vehicle Category')
                            #
                            # driver = click_year(driver,dates_df["Year"][i])
                            #
                            # driver = click_submit(driver)
                            #
                            # driver = click_month(driver,dates_df["Month"][i])
                            # soup = BeautifulSoup(driver.page_source)
                            # time.sleep(1)
                            # if('no records found' in str(soup).lower()):
                            #     pass
                            # else:
                            #     driver = download_file(driver)
                            #
                            #     vahan_df_fuel_vs_cat = read_file(type_ = "Fuel_Vs_Cat")
                            #     print(vahan_df_fuel_vs_cat)
                            #     vahan_fuel_cat_rto_level = pd.concat([vahan_fuel_cat_rto_level,vahan_df_fuel_vs_cat])



                        vahan_maker_cat_rto_level.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_RTO_LEVEL_DATA_Adv", con=engine, index=False, if_exists='append')
                        vahan_fuel_maker_rto_level.to_sql(name = "VAHAN_MAKER_VS_FUEL_RTO_LEVEL_DATA_Adv", con=engine, index=False, if_exists='append')
                        # vahan_fuel_cat_rto_level.to_sql(name = "VAHAN_FUEL_VS_CATEGORY_RTO_LEVEL_DATA_Adv", con=engine, index=False, if_exists='append')

                        connection.execute("update VAHAN_TABULAR_SUMMARY_STATE_DONE set RTO_Status = 'Done',Run_By = '1' where State = '"+state+"' and RTO_Office_Raw = '" +rto+"'")
                        connection.execute("commit")

                        connection.execute("update VAHAN_TABULAR_SUMMARY_STATE_DONE set Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"' where State = '"+state+"' and RTO_Office_Raw = '" +rto+"'")
                        connection.execute("commit")

                    connection.execute("update VAHAN_TABULAR_SUMMARY_STATE_DONE set State_Status = 'Done' where State = '"+state+"'")
                    connection.execute("commit")
                break

            except:
                error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg)
                main_limit += 1
                if(main_limit < 15):
                    continue
                else:
                    break










        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
