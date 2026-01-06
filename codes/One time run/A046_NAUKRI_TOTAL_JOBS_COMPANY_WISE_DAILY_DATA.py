# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 10:46:36 2023

@author: Abdulmuizz
"""

import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import os
from pytz import timezone
import datetime
import re
import sys
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.common.by import By

import warnings
from selenium import webdriver
warnings.filterwarnings('ignore')
import timeit

from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import time

#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
client = ClickHouse_db.db_conn()
engine = adqvest_db.db_conn()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

#%%
def assign_company_industry(df):

     dict_df = pd.read_sql("select * from AdqvestDB.GENERIC_DICTIONARY_TABLE where Input_Table = 'NAUKRI_TOTAL_JOBS_COMPANY_DATA' and Output_Table = 'NAUKRI_TOTAL_JOBS_COMPANY_DATA_CLEAN_DATA' and Output_Col = 'Industry' group by Input",engine)
 
     dict_df = dict_df[["Input","Output"]]
     dict_df.columns = ["Company","Industry"]

     df = df.merge(dict_df,on = "Company",how = 'left')
     df["Industry"] = np.where(df["Industry"].isnull(),"",df["Industry"])
 
     return df

def clean_industry_names(df):
    names_df = pd.read_sql("select * from GENERIC_DICTIONARY_TABLE where Input_Table = 'NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA' and Output is not null", engine)
    df.insert(1,'Industries_Mapped','')
    df['Industries_Mapped'] = df.merge(names_df,how = 'left', left_on = 'Industries', right_on = 'Input')['Output']
    return df

def Upload_Data(table_name, data, db: list):
    
    if 'MySQL' in db:
        data['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        engine = adqvest_db.db_conn()
        data.to_sql(table_name, con=engine, if_exists='append', index=False)

    if 'Clickhouse' in db:

        click_max_date = client.execute(f"select max(Runtime) from {table_name}")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query = f'select * from {table_name} WHERE Runtime > "' + click_max_date + '"'
        df = pd.read_sql(query,engine)
        client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())

#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '3_AM_WINDOWS_SERVER_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
#%%
        

        # driver_path="C:/Users/Santonu/Desktop/ADQvest/Chrome Driver/chromedriver.exe"
        # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")

        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        # path = os.getcwd()

        download_path=os.getcwd()
        
        prefs = {
            "download.default_directory": driver_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
            }
        
        options = webdriver.ChromeOptions()
        
        options.add_experimental_option('prefs', prefs) 
        options.add_experimental_option('excludeSwitches', ['enable-automation']) 
        
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument('--incognito')
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("--use-fake-ui-for-media-stream")
        options.add_argument("--use-fake-device-for-media-stream")
        options.add_experimental_option("prefs", prefs)

        #%%
        session = requests.Session()

        links_req = session.get("https://www.naukri.com/jobs-by-location")
        cookies = '; '.join([i+'='+j for i,j in session.cookies.get_dict().items()])
        
        soup = BeautifulSoup(links_req.content, 'lxml')
        all_columns = soup.findAll('div', class_='column')
        all_links = {}

        for column in all_columns:
            for state in column.findAll('div', class_ = 'section_white_title'):
                state_dict = {}
                for idx,city in enumerate(state.findAll('a')):
                    # print(city)
                    if idx == 0:
                        state_name = city.text.replace('Jobs in ','').strip()
                    else:
                        city_name = city.text.replace('Jobs in ','').strip()
                        city_dict = {city_name: city['href']}
                        state_dict.update(city_dict)
                all_links[state_name] = state_dict

        #%%t
        engine = adqvest_db.db_conn()
        
        max_date= pd.read_sql('select max(Relevant_Date) as Relevant_Date from NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA',engine)
        max_date=max_date['Relevant_Date'][0]
        if max_date==today.date():
            print('Today data Collected')
            query2="""select distinct State as State ,City as City from NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA where Relevant_Date='""" + str(max_date) + """';"""
            today_data= pd.read_sql(query2,engine)
            print(today_data)
            
            col_state=today_data.State.to_list()
            col_city=today_data.City.to_list()
            
            # col_state=['Tamil Nadu']
            # col_city=['Trichy']

            for s,c in zip(col_state,col_city):
                for k,v in all_links.items():
                        if k in [s]:
                            del v[c]

        all_links={k:v for  k,v in all_links.items() if len(v)!=0 }    
            
        df_list=[]
        for k,v in all_links.items():
            State=k
            for k1,v1 in v.items():
                city=k1
                df={}
                driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
                driver.implicitly_wait(10)
                # print(v1)
                print(f'City----->{city}')
                print(f'Link----->{v1}')
                
                industry_text = "//span[text()='Industry']"
                company_text = "//span[text()='Top companies']"
 
            
                # v1='https://www.naukri.com/jobs-in-tuticorin'
                driver.get(v1)
                time.sleep(4)

                # Added Click to verify if element is clicked | Pushkar | 21 Aug 2023
                click = 0
                try:
                    elem1=driver.find_element(By.XPATH, '//*[@id="qctopGroupId"]/span')
                    driver.execute_script("arguments[0].scrollIntoView(true);", elem1)
                    time.sleep(2)
                    driver.find_element(By.XPATH, '//*[@id="qctopGroupId"]/span').click()
                    time.sleep(6)
                    click = 1
                except:
                    try:
                        driver.find_element(By.XPATH, "//span[contains(text(), 'Top companies')]")
                        print('FOund')
                        click = 1
                    except:
                        continue
                
                time.sleep(5)
                soup=BeautifulSoup(driver.page_source)
                if re.findall('No results found', str(soup)):
                                print(f'This particular Link is Empty\n---->{v1}')
                                continue
                driver.quit()
                
                #COMPANY
                print('-----------------COMAPANY------COMPANY_WISE--------------------------------------')
                nores=[i.text for i in  soup.find_all('div',class_="no-results") if (re.findall('No results found', i.text))]

                if click == 1:
                    if len(nores)==0:
                        df3=pd.DataFrame()
                        
                        try:
                            l2 = soup.find_all('div',class_="filterTooltip bgWhite z-depth-2",id='tooltip')
                            company1=[i.find_all('p',class_="fleft txtLbl") for i in l2][0]

                            company2=[i.text for i in company1]
                            company2={s.split('(')[0]:s.split('(')[1].split(')')[0] for s in company2}
                            
                            company=pd.DataFrame([company2])
                            company=company.T
                            company['Industries']=company.index
                            company.columns=['Number','Company']
                            company.index=range(len(company))
                            company=company[['Company','Number']]
                            
                        except:
                            try:
                                com =[i.text for i in soup.find_all('div',class_="filterOptns", attrs={'data-filter-id':'topGroupId'})]
                                s1=[i.split(')') for i in com][0]
                                s2=[i for i in s1 if i!='']
                                company={s.split('(')[0]:s.split('(')[1].split(')')[0] for s in s2}

                            except:
                                try:
                                    com =[i.text for i in soup.find_all('div',class_="styles_filterOptns__1vq77", attrs={'data-filter-id':'topGroupId'})]
                                    time.sleep(5)
                                    s1=[i.split(')') for i in com][0]
                                    s2=[i for i in s1 if i!='']
                                    company={s.split('(')[0]:s.split('(')[1].split(')')[0] for s in s2}
                                
                                except:
                                    com =[i.text for i in soup.find('div',class_="swiper-wrapper").findAll('p')]
                                    time.sleep(5)
                                    s1=[i.split(')')[0] for i in com]
                                    s2=[i for i in s1 if i!='']
                                    company={s.split('(')[0]:s.split('(')[1].split(')')[0] for s in s2}
                                
                            

                            company=pd.DataFrame([company])
                            company=company.T
                            company['Company']=company.index
                            company.columns=['Number','Company']
                            company.index=range(len(company))
                            company=company[['Company','Number']]
                        

                        df3=assign_company_industry(company)
                        df3['State']=State
                        df3['City']=k1
                        df3['Relevant_Date'] =today.date()
                        df3["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                        print(df3)
                        df3.to_sql('NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA', if_exists='append', index = False, con=engine)
                        print(df3)
                        engine = adqvest_db.db_conn()
                            
        data_check_df = pd.read_sql("select sum(Number) as Value,Relevant_Date  from NAUKRI_TOTAL_JOBS_COMPANY_WISE_DAILY_DATA  group by Relevant_Date order by Relevant_Date Desc limit 5",engine)
        current_value = data_check_df['Value'][0]
        avg_value = data_check_df['Value'][1:].mean()
        if abs(avg_value-current_value) < 2000:
           print('Data successfully collected')
           
        else:
            raise Exception('Less Data Points collected Please check and rerun the code')
           
            
            

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

        
                
if(__name__=='__main__'):
    run_program(run_by='manual')



