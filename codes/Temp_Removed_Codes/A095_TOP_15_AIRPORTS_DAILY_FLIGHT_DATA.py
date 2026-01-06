import pandas as pd
import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import re
import sqlalchemy
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from pandas.core.common import flatten
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    #****   Date Time *****
    import datetime
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    year=today.year
    current_date=today.date()

    

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'TOP_15_AIRPORTS_DAILY_FLIGHTS_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\Junk"
        #%%
        
        
        
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }

        airport_code=['bom','blr','maa','ccu','hyd','cok','goi','amd','jai','pnq','gau','bbi','lko','trv','pat']
        airport_name=['Bengaluru Kempegowda International Airport (BLR/VOBL)','Chennai International Airport (MAA/VOMM)','Kolkata International Airport (CCU/VECC)','Hyderabad Rajiv Gandhi International Airport (HYD/VOHS)',
                     'Cochin International Airport (COK/VOCI)','Goa International Airport (GOI/VOGO)','Ahmedabad International Airport (AMD/VAAH)','Jaipur Airport (JAI/VIJP)','Pune Airport (PNQ/VAPO)','Guwahati International Airport (GAU/VEGT)','Bhubaneswar Biju Patnaik International Airport (BBI/VEBS)',
                     'Lucknow Chaudhary Charan Singh Airport (LKO/VILK)','Trivandrum International Airport (TRV/VOTV)','Patna Jay Prakash Narayan International Airport (PAT/VEPT)','Mumbai Chhatrapati Shivaji International Airport (BOM/VABB)']
        
        
        # airport_name=['Mumbai Chhatrapati Shivaji International Airport (BOM/VABB)']

        count=0
        df2=pd.DataFrame()
        # while(True):
        #
        #     try:

        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--incognito')
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver_path,chrome_options=options)
        
        driver.get(url='https://www.flightradar24.com/data/airports/india')
        time.sleep(10)
        driver.maximize_window()
        driver.implicitly_wait(30)
        time.sleep(30)
        driver.find_element(By.XPATH, '//button[@id = "onetrust-accept-btn-handler"]').click()
        time.sleep(3)
        driver.find_element(By.XPATH, '//*[@class="btn-stay"]').click()
        time.sleep(2)
        airport_name=airport_name[count:]
        
        arrival_dep=[]
        for airport_n in airport_name:
            driver.implicitly_wait(30)
            try:
                driver.find_element(By.LINK_TEXT, airport_n).click()
                time.sleep(10)
            except:
                element = driver.find_element(By.LINK_TEXT, airport_n)
                driver.execute_script("arguments[0].click();", element)
                time.sleep(10)
                
            
            for move in ['arrivals','departures']:
                
                print(f'Working on {airport_n}-------{move}')
                limit = 0
                try:
                    ele=driver.find_element(By.LINK_TEXT, "Show all "+move)
                    time.sleep(30)
                    ele.click()

                    if (len(driver.window_handles) == 2):
                        driver.switch_to.window(window_name=driver.window_handles[-1])
                        driver.close()
                        driver.switch_to.window(window_name=driver.window_handles[0])
                        ele2=driver.find_element(By.LINK_TEXT, "Show all "+move)
                        time.sleep(30)
                        ele2.click()

                    for i in range(10):
                        try:
                            #driver.find_element_by_css_selector('.btn.btn-table-action.btn-flights-load').click()
                            ele3=driver.find_element(By.XPATH, "//button[text()='Load earlier flights']")
                            time.sleep(30)
                            ele3.click()
                            if (len(driver.window_handles) == 2):
                                driver.switch_to.window(window_name=driver.window_handles[-1])
                                driver.close()
                                driver.switch_to.window(window_name=driver.window_handles[0])
                        except:
                            break

                    time.sleep(20)
                    soup=BeautifulSoup(driver.page_source)
                    
                    san=[str(a) for  a in soup.find_all("tr",{"class": "hidden-xs hidden-sm ng-scope"})]
                    san1=[a for  a in soup.find_all("tr",{"class": "hidden-xs hidden-sm ng-scope"})]
                    from datetime import datetime

                    item_list=[]
                    for k,v in zip(san1,san):
                        item = {}
                        print('-------------------------------------')
                        v=str(k).split("ng-repeat=")[0].split("data-date=")[-1].strip().replace('"','')+'-'+f'{str(year).strip()}'
                        date=datetime.strftime(datetime.strptime(v,'%A, %b %d-%Y'),'%Y-%m-%d')
                        # print(k.text)
                        print(date)
                        # start
                        
                        # print(k.find('td',class_="ng-binding").text)
                        item['Time']=k.find('td',class_="ng-binding").text
                        #Flight Num
                        # print(k.find('td',class_="p-l-s cell-flight-number").text.strip())
                        item['Flight_Number']=k.find('td',class_="p-l-s cell-flight-number").text.strip()
                        #From
                        item['Source']=k.find('span',class_="hide-mobile-only ng-binding").text
                        # print(k.find('span',class_="hide-mobile-only ng-binding").text)
                        #Airline
                        item['Airline']=k.find('td',class_="cell-airline").text.replace('-','').strip()
                        # print(k.find('td',class_="cell-airline").text.replace('-','').strip())
                        #Air Craft
                        item['Aircraft']=k.find_all('span','td',class_="ng-binding")[1].text
                        # print(k.find_all('span','td',class_="ng-binding")[1].text)
                        #Landing time 
                        item['Status']=k.find_all('td',class_="ng-binding")[1].text
                        # print(k.find_all('td',class_="ng-binding")[1].text)
                        #Rel date
                        item['Relevant_Date']=date
                        
                        item_list.append(item)
                        
                        
                    df1=pd.DataFrame.from_dict(item_list)
                    df1['Relevant_Date'] = pd.to_datetime(df1['Relevant_Date'], format='%Y-%m-%d')
                    df1['Relevant_Date']=df1['Relevant_Date'].dt.date
                    
                    df1['Airport']=airport_n
                    df1['Movement']=move.title()

                    if move=='departures':
                        df1['Destination']=df1['Source']
                        df1['Source']=''
                    else:
                        df1['Destination']=''

                    print(df1.describe())

                    driver.back()
                    time.sleep(10)
                except:
                    limit += 1
                    time.sleep(2)
                    if limit == 5:
                        print(f'{move} not clicked')
                        
                arrival_dep.append(df1)
            driver.back()
            time.sleep(10)
            count+=1
            print(count)
        driver.close()
        #%%
        final_df=pd.concat(arrival_dep)
        final_df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

        if count==15:
            final_df=final_df.drop_duplicates(['Time','Flight_Number','Airline','Aircraft','Airport','Source','Status','Destination','Movement','Relevant_Date'])
            # final_df=final_df.drop_duplicates(['Airport','Flight_Number','Airline','Time','Movement','Relevant_Date'])
            final_df=final_df[final_df['Relevant_Date']<=current_date]
            print(final_df.describe())
            
            max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from TOP_15_AIRPORTS_DAILY_FLIGHTS_DATA",engine)["Max"][0]
            final_df = final_df[final_df["Relevant_Date"]> max_rel_date]
            print(final_df)
            if(final_df.empty == False):
                final_df.to_sql("TOP_15_AIRPORTS_DAILY_FLIGHTS_DATA", index=False, if_exists='append', con=engine)
                print('Data loded')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
