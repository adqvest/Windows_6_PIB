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

from adqvest_robotstxt import Robots
robot = Robots(__file__)
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
    # l1=['Media, Entertainment & Telecom','Professional Services','Miscellaneous','Technology',
    #     'Consumer, Retail & Hospitality','BPM','Media, Entertainment & Telecom','Manufacturing & Production',
    #      'Healthcare & Life Sciences','BFSI','Infrastructure, Transport & Real Estate']       

    # for i in l1:
    #     df['Industries']=df['Industries'].apply(lambda x:x.replace(i,'',1))

    names_df = pd.read_sql("select Input as Industries,Output as Industries_Mapped  from GENERIC_DICTIONARY_TABLE where Input_Table = 'NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA' and Output is not null", engine)
    # df.insert(1,'Industries_Mapped','')
    df=pd.merge(df,names_df,how = 'left', on = 'Industries')
    return df

def Upload_Data(table_name, data, db: list):
    engine = adqvest_db.db_conn()
    
    if 'MySQL' in db:
        # data['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

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
    table_name = 'NAUKRI_TOTAL_JOBS_STATE_CITY_INDUSTRY_DAILY_DATA_2_TABLE'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '11_AM_WINDOWS_SERVER_SCHEDULER_2'
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
        robot.add_link("https://www.naukri.com/jobs-by-location")
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
        #%%
        #%%

        engine = adqvest_db.db_conn()
        max_date= pd.read_sql('select max(Relevant_Date) as Relevant_Date from NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA',engine)
        max_date=max_date['Relevant_Date'][0]
        if max_date==today.date():
            print('Today data Collected')
            query2="""select distinct State as State ,City as City from NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA where Relevant_Date='""" + str(max_date) + """';"""
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
 
                # v1='https://www.naukri.com/jobs-in-lakhtar'
                driver.get(v1)
                time.sleep(3)
                try:
                    elem2=driver.find_element(By.XPATH, '//*[@id="industryTypeIdGid"]/span')
                    time.sleep(1)
                    driver.execute_script("arguments[0].scrollIntoView(true);", elem2)
                    time.sleep(2)
                    driver.find_element(By.XPATH, '//*[@id="industryTypeIdGid"]/span').click()
                    time.sleep(10)
                except:
                    pass
                
                
                soup=BeautifulSoup(driver.page_source)
                if re.findall('This site can’t be reached', str(soup)):
                    print('website is blocking')
                    raise Exception('website is blocking')

                if re.findall('No results found', str(soup)):
                    print(f'This particular Link is Empty\n---->{v1}')
                    continue

                time.sleep(2)
                driver.quit()
                
                
                nores=[i.text for i in  soup.find_all('div',class_="no-results") if (re.findall('No results found', i.text))]
                print(nores)
                if len(nores)==0:
                    print('-----------------JOBS---STATE_CITY-----------------------------------------')
                    df1={}
                    # data=[i for i in soup.find_all(class_='h1-wrapper')] Commented | Pushkar | 21 AUG 2023
                    # Added | Pushkar | 21 Aug 2023
                    try:
                        data = [soup.find('div', attrs={'id':'jobs-list-header'}).find('div')]
                        print(data)
                    except:
                        data = [i for i in soup.find_all(class_='h1-wrapper')]
                    # End | 21 Aug 2023
                    print(data)
                    jobs=data[0].text.lower().split('of')[1].split('jobs')[0].strip()
                    
                    df1['State']=State
                    df1['City']=k1
                    df1['Number']=jobs
                    df1['Relevant_Date'] =today.date()
                    df1["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    df1=pd.DataFrame([df1])
                    print(df1)
                    engine = adqvest_db.db_conn()
                    df1.to_sql('NAUKRI_TOTAL_JOBS_STATE_CITY_DAILY_DATA', if_exists='append', index = False, con=engine)

                    #INDUSTRY
                    print('-----------------INDUSTRY---INDUSTRY_WISE-----------------------------------------')
                    df2=pd.DataFrame()
                    
                    l1 = soup.find_all('div',class_="filterTooltip bgWhite z-depth-2",id='tooltip')
                    time.sleep(5)
                    if len(l1)!=0:
                        industry1=[i.find_all('p',class_="fleft txtLbl") for i in l1][0]
                        time.sleep(3)
                        industry2=[i.text for i in industry1]
                        industry2={s.split('(')[0]:int(s.split('(')[1].split(')')[0]) for s in industry2}
                        
                        industry=pd.DataFrame([industry2])
                        industry=industry.T
                        industry['Industries']=industry.index
                        industry.columns=['Number','Industries']
                        industry.index=range(len(industry))
                        industry=industry[['Industries','Number']]
                        print(industry)
                        print('----------------------------1')

                    else:
                        ind =[i.text for i in soup.find_all('div', attrs={'data-filter-id':'industryTypeGid'})]
                        time.sleep(5)
                        if len(ind)==1:
                            l2 = soup.find_all('div',class_="styles_expanded-filters-container__iPSSS")
                            time.sleep(3)
                            if len(l2)!=0:
                                l2=[i.text for i in l2[0].find_all('p')]
                                industry3={s.split('(')[0]:int(s.split('(')[1].split(')')[0]) for s in l2}
                                industry=pd.DataFrame([industry3])

                                industry=industry.T
                                industry['Industries']=industry.index
                                industry.columns=['Number','Industries']
                                industry.index=range(len(industry))
                                industry=industry[['Industries','Number']]
                                print('----------------------------2')


                            else:
                                industry2={s.split('(')[0]:int(s.split('(')[1].split(')')[0]) for s in ind}
                                print(f'------------->{industry2}')
                                industry=pd.DataFrame([industry2])
                                industry=industry.T
                                industry['Industries']=industry.index
                                industry.columns=['Number','Industries']
                                industry.index=range(len(industry))
                                industry=industry[['Industries','Number']]
                                print('----------------------------3')



                    df2=clean_industry_names(industry)
                    df2['State']=State
                    df2['City']=k1
                    df2['Relevant_Date'] =today.date()
                    df2["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    print(df2)
                    engine = adqvest_db.db_conn()
                    df2.to_sql('NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA', if_exists='append', index = False, con=engine)
                
                
 

        Upload_Data(f'NAUKRI_TOTAL_JOBS_INDUSTRY_WISE_DAILY_DATA',pd.DataFrame(),['Clickhouse'])
            

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_type)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

        
                
if(__name__=='__main__'):
    run_program(run_by='manual')



