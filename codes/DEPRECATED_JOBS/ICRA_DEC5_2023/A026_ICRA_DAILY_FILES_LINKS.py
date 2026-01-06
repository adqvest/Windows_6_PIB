import pandas as pd
#import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
from pytz import timezone
import requests
import sys
import time
import os
import datetime
import re
import sqlalchemy
import numpy as np
import json
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from pandas.core.common import flatten
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import MySql_To_Clickhouse as MySql_CH
from adqvest_robotstxt import Robots
robot = Robots(__file__)

#%%
engine = adqvest_db.db_conn()
#%%
def driver_parameter():
    prefs = {
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
    return options


def get_specfic_dict_key(di,l1): 
    
    multi_value_dict={}
    for k,v in di.items():
        if k in l1:
            multi_value_dict[k] =v
            
    return multi_value_dict


def Upload_Data(table_name, data, db: list):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    last_date_req = today.date() - 2*days
    data=data[(data['Relevant_Date'] > db_max_date) & (data['Relevant_Date'] < last_date_req)]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Data loded in Mysql-->{data.info()}")
        
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

#%%
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'ICRA_DAILY_FILES_LINKS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path = r"C:\Users\Administrator\Junk"
        #%%
        max_date = pd.read_sql("SELECT max(Relevant_Date) as Max from ICRA_DAILY_FILES_LINKS",engine)['Max'][0]
        start_date = max_date

        options=driver_parameter()
        driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
        url = 'https://www.icraresearch.in/CPRReport/Search'
        driver.get(url)
        time.sleep(5)
        robot.add_link(url)
        
        #%%
        final_df=pd.DataFrame()
        sectors = [{'Text': i.text.strip(), 'Value' : i.get_attribute('value')} for i in driver.find_element(By.XPATH, '//div[contains(text(),"Select Sector")]/parent::div//select[@id="SectorId"]').find_elements(By.XPATH, './/option')]
        for sect in sectors[1:]:
            if sect == sectors[15] or sect == sectors[30] or sect == sectors[45] or sect == sectors[60]:
                driver.refresh()
                time.sleep(5)
            sect_name=sect['Text']
            sect_id=sect['Value']

            try:
                Select(driver.find_element(By.XPATH, '//div[contains(text(),"Select Sector")]/parent::div//select[@id="SectorId"]')).select_by_value(sect_id)
            except:
                driver.refresh()
                time.sleep(2)
                Select(driver.find_element(By.XPATH, '//div[contains(text(),"Select Sector")]/parent::div//select[@id="SectorId"]')).select_by_value(sect_id)

            time.sleep(1)
            sub_sectors = [{'Text': i.text.strip(), 'Value' : i.get_attribute('value')} for i in driver.find_element(By.XPATH, '//select[@id="SubSectorId"]').find_elements(By.XPATH, './/option')]
            for sub_sct in sub_sectors[1:]:
                
                sub_sect_name=sub_sct['Text']
                sub_sect_id=sub_sct['Value']
                
                time.sleep(1)
                Select(driver.find_element(By.XPATH, '//div[contains(text(),"Sub Sector")]/parent::div//select[@id="SubSectorId"]')).select_by_value(sub_sect_id)
                button_xpath = '//button[contains(@class, "btn btn-animated btn-info btn-sm") and contains(text(), "Search")]'
                try:
                    time.sleep(0.5)
                    driver.find_element(By.XPATH, button_xpath).click()
                    time.sleep(2)
                except:
                    continue

                
                soup=BeautifulSoup(driver.page_source)
                time.sleep(1)
                print(f'Sector id-->{sect_id}')
                print(f'Sub Sector id-->{sub_sect_id}')
                ###########################################################################################################
                table=[i.find_all('tr') for i in soup.find_all('table') if re.findall('GetRationaleFile'.lower(), str(i).lower())]
                if len(table)>0:
                    table_row=[i.find_all('td') for i in table[0] if len(i)>0]
                    if len(table_row)>0:
                        dict_up={}
                        for i in table_row:
                            if len(i)>0:
                                links=[{'Links': 'https://www.icraresearch.in/'+j.find('a')['href']} for j in i if re.findall('GetRationaleFile'.lower(), str(j).lower())]
                                company=[{'Company_Name':j.find('a').text}  for j in i if re.findall('companyname'.lower(), str(j).lower())]
                                date=[{'Relevant_Date': datetime.datetime.strptime(str(j.text),'%d %b %Y')}  for j in i if (re.findall('RationalDate'.lower(), str(j).lower()) and j.text.strip()!='')]
                                
                                if len(date)==0:
                                    continue

                                finnal=company+links+date
                                merged_dict ={k: v for d in finnal for k, v in d.items()}
                                cpr_df=pd.DataFrame.from_dict([merged_dict])
                                
                                final_df=pd.concat([final_df,cpr_df])
                                final_df['Sector_Name']=sect_name
                                final_df['Sub_Sector_Name']=sub_sect_name
                                final_df['Sector_ID']=sect_id
                                final_df['Sub_Sector_ID']=sub_sect_id
                                
        #%%
        if len(final_df)>0:
            final_df.index = range(len(final_df.index))
            final_df['Relevant_Date']=pd.to_datetime(final_df['Relevant_Date'], format='%Y-%m-%d').dt.date
            final_df['File_Name']=final_df['Company_Name'].str.replace(' ','_')+'_'+final_df['Relevant_Date'].astype(str)+'.pdf'
            final_df['Download_Status']=np.nan
            final_df['Abbreviation_Status']=np.nan
            final_df['Data_Scrap_Status']=np.nan
            final_df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            
            final_df=drop_duplicates(final_df)
            Upload_Data('ICRA_DAILY_FILES_LINKS',final_df,['MySQL'])
            MySql_CH.ch_truncate_and_insert('ICRA_DAILY_FILES_LINKS')

            

        log.job_end_log(table_name,job_start_time, no_of_ping)
        
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line no " + str(sys.exc_info()[2].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
