# -*- coding: utf-8 -*-
"""
Created on Fri May  3 09:28:53 2024

@author: Santonu
"""

import datetime as datetime
import json
import requests
from bs4 import BeautifulSoup
import os
import re
import sys
import warnings
import dateutil
from dateutil.parser import parse
import pandas as pd
import numpy as np
import time
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select  
#%%
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
import MySql_To_Clickhouse as sqlch
import cleancompanies
from adqvest_robotstxt import Robots

robot = Robots(__file__)
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
#%%

def clean_txt(text):
    text = text.upper()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    #text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+ ml', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r"'\w+", " ", text)
    text = re.sub(r'  +', ' ', text).strip()
    text = text.replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace("'",' ').replace(';','').replace('"','')
    text = text.replace('/','').replace('+','')
    text = re.sub(r'[?|$|.|!]',r'',text)
    text = text.replace('THE','').replace('&','AND')
    text = text.replace('LIMITED','LTD')
    text = re.sub(r'  +',' ', text).strip()
    return text
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, data, db: list):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=drop_duplicates(data)
    data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f'Done for --->{db_max_date}')
        # print(data.info())

    if 'Clickhouse' in db:
        click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        # query2 =f"select * from {table_name} WHERE Relevant_Date > '" + click_max_date +"' and  Product='{product}';" 
        query2=f"select * from {table_name} where Relevant_Date>'{click_max_date}'"

        df = pd.read_sql(query2,engine)
        client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())
        

def get_page_content(url,layer_1=True,rel_month=''):
    chrome_driver = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
    download_file_path = r"C:\Users\Administrator\Junk_One_Time"
    download_file_path=os.getcwd()
    
    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }

    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
    driver.get(url)
    time.sleep(5)
    
    if layer_1==True:
        soup=BeautifulSoup(driver.page_source)
        driver.delete_all_cookies()
        driver.quit()
        return soup
    else:
        dropdown_element = driver.find_element(By.ID,"yearDD") 
        dropdown = Select(dropdown_element)
        dropdown.select_by_visible_text(rel_month)
        time.sleep(5)
        df=pd.read_html(driver.page_source)
        time.sleep(5)
        driver.delete_all_cookies()
        driver.quit()
        return df
    
def convert_date_format(input_date,output_format='%Y-%m-%d',input_format="%b '%y",Month_end=True):
    from datetime import datetime as dt
    from pandas.tseries.offsets import MonthEnd
    try:
        input_datetime = dt.strptime(str(input_date),input_format)
        output_date = input_datetime.strftime(output_format)
    except:
        input_datetime = dt.strptime(str(input_date),'%b, %Y')
        output_date = input_datetime.strftime(output_format)

    if Month_end==True:
        output_date=pd.to_datetime(str(output_date), format=output_format)+ MonthEnd(1)
        output_date=output_date.date()

    return output_date

  
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA,NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        #%%
        max_rel_date1 = pd.read_sql('Select Max(Relevant_Date) AS max_date FROM NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA', con=engine)['max_date'][0]
        max_rel_date2 = pd.read_sql('Select Max(Relevant_Date) AS max_date FROM NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA', con=engine)['max_date'][0]
        max_rel_date=min(max_rel_date1,max_rel_date2)
        #%%
        
        
        url='https://www.npci.org.in/what-we-do/netc-fastag/netc-ecosystem-statistics'
        robot.add_link(url)
        soup1=get_page_content(url)        
        months=soup1.find_all(class_="floatlabel filled",id="yearDD")[0]
        months=[i.text for i in months.find_all('option')]
        months_dates={i:convert_date_format(i,input_format="%b %Y") for i in months if convert_date_format(i,input_format="%b %Y")>max_rel_date}

        #%%
        df_issuer=pd.DataFrame()
        df_acquirer=pd.DataFrame()
        
        if len(months_dates)>0:
           for k,v in months_dates.items():
                print(k)
                df=get_page_content(url,layer_1=False,rel_month=k)
                if len(df)>0:
                   for i in df:
                       bank_type=i.columns[0][0].split('-')[-1].split('(')[0].strip()
                       print(bank_type)
                       df_x=i
                       if bank_type=='Acquirer Banks':
                           df_a=i.copy()
                           rel_date=re.findall(r'\((\w{3}\'\d{2})\)', df_a.columns[0][0])[0]
                           df_a.columns=[col[2] if type(col) is tuple else col for col in df_a.columns.values]
                           df_a['Relevant_Date']=rel_date
                           df_acquirer=pd.concat([df_acquirer,df_a])
                       elif bank_type=='Issuer Banks':   
                           df_i=i.copy()
                           rel_date=re.findall(r'\((\w{3}\'\d{2})\)', df_i.columns[0][0])[0]
                           df_i.columns=[col[1] if type(col) is tuple else col for col in df_i.columns.values]
                           df_i['Relevant_Date']=rel_date
                           df_issuer=pd.concat([df_issuer,df_i])
                      
#%%
        if len(df_acquirer)>0:
            df_acquirer=df_acquirer.rename(columns={'Acquirer Banks':'Bank_Name', 'Total Approved Volume (in Mn)':'Approved_Volume_Mn',
                    'Approved %':'Approved_Pct','BD %':'Business_Decline_Pct', 'TD %':'Technical_Decline_Pct'})
            
            df_acquirer['Bank_Name']=df_acquirer['Bank_Name'].apply(lambda x:x.upper())
            
            df_acquirer['Approved_Pct']=df_acquirer['Approved_Pct'].apply(lambda x:str(x).replace('%','') if x!=None else x)
            df_acquirer['Business_Decline_Pct']=df_acquirer['Business_Decline_Pct'].apply(lambda x:str(x).replace('%','') if x!=None else x)
            df_acquirer['Technical_Decline_Pct']=df_acquirer['Technical_Decline_Pct'].apply(lambda x:str(x).replace('%','') if x!=None else x)
            df_acquirer['Relevant_Date']=df_acquirer['Relevant_Date'].apply(lambda x:convert_date_format(x,input_format="%b'%y"))
            
            df_acquirer.drop(columns=['Sr. No'], inplace=True)
            df_acquirer["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            df_acquirer['Relevant_Date'] = pd.to_datetime(df_acquirer['Relevant_Date'], format='%Y-%m-%d')
            df_acquirer['Relevant_Date']=df_acquirer['Relevant_Date'].dt.date
            df_acquirer = df_acquirer[df_acquirer['Approved_Volume_Mn'].apply(lambda x: pd.to_numeric(x, errors='coerce')).notnull()]     

            df_acquirer['Bank_Name']=df_acquirer['Bank_Name'].apply(lambda x:clean_txt(x))
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'RBL', 'RBL BANK', df_acquirer['Bank_Name'])
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'CITI', 'CITI BANK', df_acquirer['Bank_Name'])
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'CITIBANK', 'CITI BANK', df_acquirer['Bank_Name'])
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'EQUITAS BANK', 'EQUITAS SMALL FINANCE BANK', df_acquirer['Bank_Name'])
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'DBS BANK LTD', 'DBS BANK INDIA LTD', df_acquirer['Bank_Name'])
            df_acquirer['Bank_Name'] = np.where(df_acquirer['Bank_Name'] == 'FINO PAYMENTS BANK LTD', 'FINO PAYMENTS BANK', df_acquirer['Bank_Name'])


            #%%
            df_acquirer, unmapped = cleancompanies.comp_clean(df_acquirer, 'Bank_Name', 'payments_npci', 'Bank_Clean', 'NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA')
            df_acquirer.reset_index(drop=True,inplace=True)

            Upload_Data('NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA',df_acquirer,['MySQL'])
            Upload_Data('NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA',df_acquirer,['Clickhouse'])

            if len(unmapped) > 0:
                       raise Exception(f'Company Clean Mapping not available for few companies for NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ACQUIRER_BANK_WISE_MONTHLY_DATA Please check GENERIC_COMPANY_UNMAPPED_TABLE for more details')                    
#%%

            
#%%
        if len(df_issuer)>0:
            df_issuer=df_issuer.rename(columns={'Issuer Member Bank':'Bank_Name', 
                                                'Total Volume (in Mn)':'Volume_Mn', 
                                                'Approved %':'Approved_Pct',
                                                'Deemed Approved %':'Deemed_Approved_Pct'})
            
            df_issuer['Bank_Name']=df_issuer['Bank_Name'].apply(lambda x:x.upper())
            df_issuer['Approved_Pct']=df_issuer['Approved_Pct'].apply(lambda x:str(x).replace('%','') if x!=None else x)
            df_issuer['Deemed_Approved_Pct']=df_issuer['Deemed_Approved_Pct'].apply(lambda x:str(x).replace('%','') if x!=None else x)
            df_issuer['Relevant_Date']=df_issuer['Relevant_Date'].apply(lambda x:convert_date_format(x,input_format="%b'%y"))
            df_issuer.drop(columns=['Sr. No'], inplace=True)
            
            df_issuer["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            df_issuer['Relevant_Date'] = pd.to_datetime(df_issuer['Relevant_Date'], format='%Y-%m-%d')
            df_issuer['Relevant_Date']=df_issuer['Relevant_Date'].dt.date
            df_issuer = df_issuer[df_issuer['Volume_Mn'].apply(lambda x: pd.to_numeric(x, errors='coerce')).notnull()]     
            
            df_issuer['Bank_Name']=df_issuer['Bank_Name'].apply(lambda x:clean_txt(x))
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'RBL', 'RBL BANK', df_issuer['Bank_Name'])
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'CITI', 'CITI BANK', df_issuer['Bank_Name'])
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'CITIBANK', 'CITI BANK', df_issuer['Bank_Name'])
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'EQUITAS BANK', 'EQUITAS SMALL FINANCE BANK', df_issuer['Bank_Name'])
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'DBS BANK LTD', 'DBS BANK INDIA LTD', df_issuer['Bank_Name'])
            df_issuer['Bank_Name'] = np.where(df_issuer['Bank_Name'] == 'FINO PAYMENTS BANK LTD', 'FINO PAYMENTS BANK', df_issuer['Bank_Name'])

            df_issuer=df_issuer[~(df_issuer.Bank_Name=='TOTAL')]
            df_issuer, unmapped = cleancompanies.comp_clean(df_issuer, 'Bank_Name', 'payments_npci', 'Bank_Clean', 'NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA')
            df_issuer.reset_index(drop=True,inplace=True)  

            Upload_Data('NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA',df_issuer,['MySQL'])
            Upload_Data('NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA',df_issuer,['Clickhouse'])
            if len(unmapped) > 0:
                       raise Exception(f'Company Clean Mapping not available for few companies for NPCI_NETC_FASTAG_ECOSYSTEM_STATS_ISSUER_BANK_WISE_MONTHLY_DATA Please check GENERIC_COMPANY_UNMAPPED_TABLE for more details')                    
            
        #%%        
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')