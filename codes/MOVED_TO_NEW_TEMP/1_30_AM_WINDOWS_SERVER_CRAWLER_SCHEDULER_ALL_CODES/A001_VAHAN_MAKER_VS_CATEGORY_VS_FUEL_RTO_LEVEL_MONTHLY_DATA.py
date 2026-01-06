
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
    table_name = 'VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA'
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

        def get_fuels(driver):
            #****************** taking all types of fuel and saving into dictionary ********************
            limit = 0
            while True:
                try:
                    try:
                        driver.find_element(By.XPATH,"//*[@id='filterLayout']/div[1]/a/span").click()
                    except:
                        pass
                    driver.find_element(By.XPATH,'//*[@id="filterLayout-toggler"]/span/a').click()
                    time.sleep(0.5)
                    #no_of_ping += 1
                    fuel_info = {}
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    fuel = soup.findAll('table',attrs={'id':'fuel'})[0].findAll('label')
                    fuel_type=[]
                    for i in fuel:
                        fuel_type.append(i.text)
                    indexes=[]

                    required_fuels=['CNG ONLY','DIESEL','ELECTRIC(BOV)','PETROL','PETROL/CNG','PURE EV']
                    for i in required_fuels:
                        fuel_info[i]=fuel_type.index(i)+1

                    print('done')
                    #driver.find_element(By.XPATH,'//*[@id="filterLayout"]/div[1]/a/span').click()
                    break
                except:
                    limit = limit + 1
                    print('error')
                    if(limit>8):
                        raise Exception('error in fuel')

                    time.sleep(3)
            return (driver,fuel_info,len(fuel_type))


        def download_file(driver):
            global no_of_ping
            limit = 0
            while True:
                try:
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//a[@id = 'groupingTable:xls']").click()
                    time.sleep(5)
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
            time.sleep(10)
            while True:
                try:
                    time.sleep(10)
                    soup = BeautifulSoup(driver.page_source, 'lxml')

                    u=soup.findAll('div', attrs={'class':'ui-selectonemenu-panel ui-widget ui-widget-content ui-corner-all ui-helper-hidden ui-shadow ui-input-overlay'})[1]
                    idd=u.li['id'].replace('0','input')
                    state_option = soup.findAll('select', attrs={'id':idd})[0].findAll('option')
                    state_info = {}
                    # try:
                    #     state_option = soup.findAll('select', attrs={'id':'j_idt31_input'})[0].findAll('option')
                    # except:
                    #     state_option = soup.findAll('select', attrs={'id':'j_idt32_input'})[0].findAll('option')
                    for x in state_option:
                        val = x.attrs['value']
                        if(val == '-1'):
                            continue
                        else:
                            state_info[val] = x.text

                    time.sleep(5)
                    break
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1])
                    print(error_type)
                    print(error_msg)
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
                    driver.find_element(By.XPATH,"/html/body/form/div[2]/div/div/div[1]/div[2]/div[3]/div/label").click()
                    # try:
                    #     try:
                    #         driver.find_element(By.XPATH,"//div[@id='j_idt31']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    #     except:
                    #         driver.find_element(By.XPATH,"//div[@id='j_idt32']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    # except:
                    #     driver.find_element(By.XPATH,"//div[@id='j_idt43']//div[@class = 'ui-selectonemenu-trigger ui-state-default ui-corner-right']").click()

                    time.sleep(2)
                    no_of_ping += 1
                    driver.find_element(By.XPATH,"//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + state_info[st] + "']").click()
                    # driver.find_element(By.XPATH,"//li[@data-label = '"+state_info[st]+"']").click()

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
                    driver.find_element(By.XPATH,"//div[@id='selectedRto']//span[@class='ui-icon ui-icon-triangle-1-s ui-c']").click()
                    time.sleep(1)
                    no_of_ping += 1
        #             driver.find_element(By.XPATH,"//li[@class='ui-selectonemenu-item ui-selectonemenu-list-item ui-corner-all'][text()='" + rto_info[rto] + "']").click()
                    driver.find_element(By.XPATH,"//li[@data-label = '"+rto_info[rto]+"']").click()
                    time.sleep(1)
                    break
                except:
                    limit = limit + 1
                    if(limit>8):
                        raise Exception('error in fill_rto')

                    time.sleep(3)
            return driver


        def read_file(type_,state,rto,rel_date,overall = False):
            print("Directory :",os.getcwd())
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
            if(overall == False):
                vahan_full_df["State"] = state
                vahan_full_df["RTO_Office_Raw"] = rto

                rto_fun_data = vahan_full_df['RTO_Office_Raw'].map(clean_fun)
                vahan_full_df['RTO_Date'] = rto_fun_data.apply(lambda x:x[0])
                vahan_full_df['RTO_Code'] = rto_fun_data.apply(lambda x:x[1])
                vahan_full_df['RTO_Office'] = rto_fun_data.apply(lambda x:x[2])

            vahan_full_df["Relevant_Date"] = rel_date

            vahan_full_df["Runtime"] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: x.replace(",",""))
            vahan_full_df["Total"] = vahan_full_df["Total"].apply(lambda x: int(x))

            return vahan_full_df

        def get_dates(cond):

            distinct_date = [datetime.date(2021,6,30)]

            query = "select min(Relevant_Date) as Min FROM VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS"
            min_date = pd.read_sql(query, engine)
            min_date = min_date["Min"][0]

            distinct_date.append(min_date)


            dates_df = pd.DataFrame({"Relevant_Date":pd.date_range(start = min(distinct_date), end = max(distinct_date), freq ='M').tolist()})
            dates_df["Relevant_Date"] = dates_df["Relevant_Date"].apply(lambda x: x.date())
            dates_df = dates_df.append(pd.DataFrame({"Relevant_Date":[min_date]}))
            dates_df["Month"] = dates_df["Relevant_Date"].apply(lambda x: datetime.datetime.strftime(x,"%b").upper())
            dates_df["Year"] = dates_df["Relevant_Date"].apply(lambda x: datetime.datetime.strftime(x,"%Y"))
            dates_df = dates_df.sort_values(by = "Relevant_Date",ascending = False)
            dates_df.drop_duplicates(subset = ["Month","Year"],inplace = True)
            dates_df.reset_index(drop = True,inplace = True)

            if(cond):
                dates_df = dates_df[:5]

            else:
                dates_df = dates_df[:5] #change this later

            dates_df.reset_index(drop = True,inplace = True)
            max_date=dates_df['Relevant_Date'].max()
            dates_df=dates_df[dates_df['Relevant_Date']!=max_date]

            dates_df.reset_index(drop = True,inplace = True)
            return dates_df

        def data_check_step1():
            rto_df = pd.read_sql("Select  State, Sum(Total) as RTO_Total, Relevant_Date from AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTHLY_DATA group by State, Relevant_Date order by State",engine)

            state_df = pd.read_sql("Select  State, Sum(Total) as State_Total, Relevant_Date from AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING  group by State, Relevant_Date order by State",engine)

            df1=pd.merge(rto_df, state_df, on=['Relevant_Date', 'State'])
            df1['Error_Pct']=abs((df1['State_Total']/df1['RTO_Total']-1))

            error_df=df1[df1['Error_Pct']>5] 

            if error_df.shape[0]>=1:
               raise Exception ("cumulative RTO data not matching with State level data")    

        def data_check():



            error_df_3 = pd.DataFrame()
            data_df = pd.read_sql("Select  Sum(Total) as RTO ,RTO_Office_Raw,State,RTO_Code,Relevant_Date from AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING  group by RTO_Office_Raw,Relevant_Date",engine)
            rel_date = data_df["Relevant_Date"].unique().tolist()
            for date in rel_date:
                rel_date_df = data_df[data_df["Relevant_Date"] == date]
                rel_date_df = rel_date_df.nlargest(10,"RTO")
                min_value = rel_date_df["RTO"].min()
                rel_date_df["Ratio"] = rel_date_df["RTO"]/min_value
                rel_date_df = rel_date_df[rel_date_df["Ratio"] >5]

                error_df_3 = pd.concat([error_df_3,rel_date_df])
            error_df_3 = error_df_3[["RTO_Office_Raw","RTO_Code","State","Relevant_Date"]]

            error_df = error_df_3

            if(error_df.empty):
                pass
            else:
                error_df.drop_duplicates(inplace = True)


                for index,i in error_df.iterrows():
                    connection.execute("update VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS set RTO_Status = Null,State_Status = Null where RTO_Office_Raw = '" +i["RTO_Office_Raw"]+"'")
                    connection.execute("commit")

                    connection.execute("update VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS set State_Status = Null where State = '" +i["State"] +"'")
                    connection.execute("commit")


                    connection.execute("delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING where RTO_Code = '" +i["RTO_Code"]+"' and Relevant_Date >= '"+error_df["Relevant_Date"].min().strftime("%Y-%m-%d")+"' and Relevant_Date <= '"+error_df["Relevant_Date"].max().strftime("%Y-%m-%d")+"'")
                    connection.execute("commit")


                raise Exception


        def main_function():
            global no_of_ping
            # global table_name
            # global job_start_time
            main_limit = 0

            while True:
                try:
                    #chrome_driver_path = r"D:\advaith\Adqvest\chromedriver.exe"
                    chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    #download_file_path = r"D:\advaith\Adqvest\Junk"
                    download_file_path = r"C:\Users\Administrator\Junk_Vahan_Full_RTO"

                    os.chdir(r"C:\Users\Administrator\Junk_Vahan_Full_RTO")


                    time.sleep(5)
                    for file in os.listdir():
                        os.remove(file)

                    driver = init_driver(chrome_driver=chrome_driver_path,download_file_path = download_file_path)
                    driver = clicking_tabular_summary(driver)
                    no_of_ping += 1


                    state_info = get_states(driver)

                    ss = [clean_state(x) for x in  list(state_info.values())]
                    maker_done = pd.DataFrame(ss, columns=['State'])
                    maker_done['Relevant_Date'] = today.date()

                    maker_df = pd.read_sql("select * from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS", con=engine)
                    if(maker_df.empty):
                        maker_done.to_sql("VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS", con=engine, index=False, if_exists='append')

                    #########################################
                    query = "select min(Relevant_Date) as Min FROM VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS"
                    max_date = pd.read_sql(query, engine)
                    max_date = max_date["Min"][0]

                    if(max_date.day == 1):
                        cond = True
                    else:
                        cond = False

                    dates_df = get_dates(cond)

                    #########################################

                    remove_state = maker_df[(maker_df['State_Status'].notnull())]

                    print(state_info)
                    for row in set(remove_state['State'].to_list()):
                        print(row)

                        key = list(state_info.keys())[[clean_state(s) for s in list(state_info.values())].index(row)]
                        del state_info[key]


                    for st in state_info:
                        driver.refresh()
                        driver = fill_state(driver,st,state_info)

                        state = state_info[st]
                        state = clean_state(state)

                        rto_info = get_rtos(driver)

                        ss = [clean_rto(x) for x in  list(rto_info.values())]
                        maker_done = pd.DataFrame(ss, columns=['RTO_Office_Raw'])
                        maker_done['State'] = state
                        maker_done['Relevant_Date'] = today.date()
                        maker_df = pd.read_sql("select * from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS", con=engine)
                        if(maker_df[maker_df['State']==state]['RTO_Office_Raw'].isnull().all()):
                            maker_done.to_sql("VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS", con=engine, index=False, if_exists='append')

                        remove_rto = maker_df[(maker_df['State']==state) & (maker_df['RTO_Office_Raw'].notnull()) & (maker_df['RTO_Status'].notnull())]
                        for row in set(remove_rto['RTO_Office_Raw'].to_list()):
                            key = list(rto_info.keys())[[clean_rto(s) for s in list(rto_info.values())].index(row)]
                            print(row,' : ', rto_info[key])
                            del rto_info[key]




                        for x in rto_info.keys():
                            print('state - ',state)
                            print('rto - ', rto_info[x])
                            driver.refresh()
                            driver = fill_state(driver,st,state_info)
                            time.sleep(1)
                            driver = fill_rto(driver=driver, rto=x, rto_info=rto_info)

                            rto = rto_info[x]
                            rto = rto.upper()
                            rto = re.sub(r'  +',' ',rto).strip()


                            vahan_maker_cat_fuel_rto_level = pd.DataFrame()

                            #Maker vs Category
                            driver = choose_y_axis(driver,'Maker')
                            driver = choose_x_axis(driver,'Vehicle Category')

                            dates_df.reset_index(drop=True, inplace=True)

                            for i in range(len(dates_df)):



                                driver = click_year(driver,dates_df["Year"][i])

                                driver = click_submit(driver)

                                driver = click_month(driver,dates_df["Month"][i])

                                time.sleep(1)
                                driver,fuel_info,length=get_fuels(driver)
                                print(fuel_info)

                                for jk in fuel_info.items():
                                    print(jk)
                                    driver.find_element(By.XPATH,"//*[@id='fuel']/tbody/tr["+str(jk[1])+"]/td/label").click()
                                    # try:
                                    #     driver.find_element(By.XPATH,"//*[@id='j_idt66']/span[2]").click()
                                    # except:
                                    #     driver.find_element(By.XPATH,"//*[@id='j_idt67']/span[2]").click()
                                    driver.find_element(By.XPATH,"/html/body/form/div[2]/div/div/div[3]/div/div[1]/div[1]/span/button").click()   
                                    time.sleep(1)



                                    soup = BeautifulSoup(driver.page_source)
                                    if('no records found' in str(soup).lower()):
                                        pass
                                    else:
                                        time.sleep(1)
                                        driver = download_file(driver)
                                        time.sleep(3)

                                        vahan_df_maker_vs_cat_vs_fuel = read_file(type_ = "Maker_Vs_Cat_Vs_Fuel",state = state,rto = rto,rel_date = dates_df["Relevant_Date"][i])
                                        print('read successfully')
                                        vahan_df_maker_vs_cat_vs_fuel['Fuel']=jk[0]
                                        print(vahan_df_maker_vs_cat_vs_fuel)
                                        vahan_maker_cat_fuel_rto_level = pd.concat([vahan_maker_cat_fuel_rto_level,vahan_df_maker_vs_cat_vs_fuel])

                                    driver.find_element(By.XPATH,"//*[@id='fuel']/tbody/tr["+str(jk[1])+"]/td/label").click()

                                other_fuels=[x for x in range(1,length+1) if x not in fuel_info.values()]

                                for jk in other_fuels:
                                    driver.find_element(By.XPATH,"//*[@id='fuel']/tbody/tr["+str(jk)+"]/td/label").click()

                                # driver.find_element(By.XPATH,"//*[@id='j_idt67']/span[2]").click()
                                driver.find_element(By.XPATH,"/html/body/form/div[2]/div/div/div[3]/div/div[1]/div[1]/span/button").click()
                                time.sleep(2)




                                soup = BeautifulSoup(driver.page_source)
                                if('no records found' in str(soup).lower()):
                                    pass
                                else:
                                    time.sleep(1)
                                    driver = download_file(driver)
                                    time.sleep(1)
                                    vahan_df_maker_vs_cat_vs_fuel = read_file(type_ = "Maker_Vs_Cat_Vs_Fuel",state = state,rto = rto,rel_date = dates_df["Relevant_Date"][i])
                                    vahan_df_maker_vs_cat_vs_fuel['Fuel']='Others'
                                    print(vahan_df_maker_vs_cat_vs_fuel)
                                    vahan_maker_cat_fuel_rto_level = pd.concat([vahan_maker_cat_fuel_rto_level,vahan_df_maker_vs_cat_vs_fuel])




                            if(vahan_maker_cat_fuel_rto_level.empty == False):
                                vahan_maker_cat_fuel_rto_level = vahan_maker_cat_fuel_rto_level[vahan_maker_cat_fuel_rto_level["Total"]!=0]
                            vahan_maker_cat_fuel_rto_level.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING", con=engine, index=False, if_exists='append')


                            connection.execute("update VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS set RTO_Status = 'Done' where State = '"+state+"' and RTO_Office_Raw = '" +rto+"'")
                            connection.execute("commit")

                            connection.execute("update VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS set Runtime = '"+datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S")+"' where State = '"+state+"' and RTO_Office_Raw = '" +rto+"'")
                            connection.execute("commit")


                            log.job_end_log(table_name,job_start_time,no_of_ping)



                        connection.execute("update VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS set State_Status = 'Done' where State = '"+state+"'")
                        connection.execute("commit")


                    break

                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                    print(error_msg)
                    main_limit += 1
                    if(main_limit < 200):
                        continue
                    else:
                        driver.quit()
                        raise Exception(error_msg)
                        break
                try:
                    driver.quit()
                except:
                    pass
            data_check_step1()        
            data_check()
            query = "delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS"
            connection.execute(query)
            connection.execute('commit')

            #maker vs category
            date_vahan = pd.read_sql("select min(Relevant_Date) as Date from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING", con=engine)
            date_vahan = date_vahan['Date'][0]
            date_vahan = datetime.date(date_vahan.year, date_vahan.month, 1)
            date_vahan = date_vahan.strftime('%Y-%m-%d')

            query = "delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA where Relevant_Date>='" + date_vahan + "'"
            connection.execute(query)
            connection.execute('commit')


            try:
                df = pd.read_sql("select * from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING where Relevant_Date>='" + date_vahan + "'", con=engine)
                df.drop_duplicates(subset=['Maker', 'Maker_Clean', 'Total', 'Vehicle_Category', 'Fuel', 'State',
                       'RTO_Office_Raw', 'RTO_Date', 'RTO_Code', 'RTO_Office',
                       'Relevant_Date'], inplace = True)
                df.to_sql(name='VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA', con=engine, index=False, if_exists='append')
                #ClickHouse

            except:
                print('ERROR 3')
                raise Exception('error in VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING')


            query = "drop table VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING"
            connection.execute(query)
            connection.execute('commit')


        def table_check():
            query = "SELECT * FROM information_schema.tables WHERE table_schema = 'AdqvestDB' AND TABLE_NAME in ('VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING')"
            df = pd.read_sql(query, con=engine)
            if(df.shape[0]>0):
                return True
            else:
                return False

    

        #main code starts here

        today = datetime.datetime.now(india_time)


        query = "SELECT * FROM VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STATUS"
        vahan_maker_state_done = pd.read_sql(query, con=engine)
        if(( vahan_maker_state_done.shape[0]>0 ) | (table_check())):

            print('A')
            main_function()

            log.job_end_log(table_name,job_start_time,no_of_ping)

        else:
            if(today.day in [9]):#change it to 2 after run

                print('B')

                query = "create table AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA_STAGING select * from AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA limit 0"
                connection.execute(query)
                connection.execute('commit')




                main_function()

                log.job_end_log(table_name,job_start_time,no_of_ping)


            else:
                log.job_end_log(table_name,job_start_time,no_of_ping)















    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)


        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
