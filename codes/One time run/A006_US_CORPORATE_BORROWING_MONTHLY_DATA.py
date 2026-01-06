# -*- coding: utf-8 -*-
"""
Created on Fri Jul  5 10:00:06 2024

@author: Santonu
"""

import numpy as np
import pandas as pd
import re
import os
import sys
import warnings
import boto3
import glob
from pytz import timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import dateutil
import MySql_To_Clickhouse as MySql_CH
import adqvest_db
import adqvest_s3
import JobLogNew as log
from adqvest_robotstxt import Robots
import ClickHouse_db
robot = Robots(__file__)
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#%%
import datetime
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
#%%           
def convert_date_format(input_date,output_format='%Y-%m-%d',input_format="%b %Y",Month_end=True):
    from datetime import datetime as dt
    from pandas.tseries.offsets import MonthEnd
    try:
       input_datetime = dt.strptime(str(input_date),input_format)
       output_date = input_datetime.strftime(output_format)
    except:
        try:
            input_datetime = dt.strptime(str(input_date),"%B %Y")
            output_date = input_datetime.strftime(output_format)
        except:
            try:
                input_datetime = dt.strptime(str(input_date),"%B'%y")
                output_date = input_datetime.strftime(output_format)
            except:
                input_datetime = dt.strptime(str(input_date),"%b'%y")
                output_date = input_datetime.strftime(output_format)
    
    if Month_end==True:
        output_date=pd.to_datetime(str(output_date), format=output_format)+ MonthEnd(1)
        output_date=output_date.date()
    return output_date

def clean_values(x):
    x=float(str(x).replace(',',''))
    return x

def row_filling(df,col_idx,result_list=[],new_col='New_col'):
    df[new_col]=np.nan
    for i in result_list:
        row_index=df[df.iloc[:, col_idx].str.lower().str.contains(i.lower(),case=False,flags=re.IGNORECASE) ==True].index.to_list()
        if len(row_index)>0:
            df.loc[row_index[0],new_col]=df.iloc[row_index[0],col_idx]

    df[new_col]=df[new_col].ffill(axis=0)
    return df

def get_request_session(url,req='',data1={},headers_1={}):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    if req=='post':
        r=session.post(url, data = data1, headers=headers_1, timeout = 180)
    else:
        r=session.get(url)
        
   
    return r

def clean_location(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'_',' ',text) # replace & with space
    print(text)
    # text = re.sub(r'.',' ',text) # replace & with space

    # text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = text.upper()
    return text

def clean_location1(location):
    cleaned_string = re.sub(r'\s*\.\s*', '. ', location).strip()
    cleaned_string = re.sub(r'\s+', ' ', cleaned_string)
    unique_parts = list(dict.fromkeys([part.strip() for part in cleaned_string.split(',')]))
    print(unique_parts)
    return ', '.join(unique_parts)


def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, data,db: list):
    query=f"select distinct Relevant_Date as Relevant_Date from {table_name};"
    print(query)
    db_max_date = pd.read_sql(query,engine)   
    data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], format='%Y-%m-%d')
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    # print(data.info())
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        # print(f'Done for --->{db_max_date,ty}')
        print(data.info())

def process_df(df):
     df=[ i for i in df if len(i)>10][0]
     
     df.drop(columns=[col for col in df.columns if df[col].nunique() == 1], inplace=True)
     df.drop(columns=[col for col in df.columns if 'Unnamed' in col], inplace=True)
     df.drop(columns=[col for col in df.columns if col.replace(' ','').isnumeric()], inplace=True)
     df=df.rename(columns={df.columns[1]:'Type_of_Issue',df.columns[0]:'Sl_No'})
     df.dropna(axis=0,how='all',inplace=True)
     
     
     df[df.columns[1]]=   df[df.columns[1]].apply(lambda x:re.findall('[A-Za-z]+', x.replace('\n','')))
     df[df.columns[1]]=   df[df.columns[1]].apply(lambda x:' '.join(x).upper())
     df.reset_index(drop=True,inplace=True)
     
     df=row_filling(df,1,['ALL ISSUES NEW AND REFUNDING','BONDS','MEMO','STOCKS'],new_col='Category')
     df=row_filling(df,1,['BY TYPE OF OFFERING','BY INDUSTRY GROUP'],new_col='Sub_Category')
     
     df['Sub_Category']=np.where(df.Type_of_Issue==df.Category,np.nan,df['Sub_Category'])
     df.dropna(subset=[df.columns[0]],inplace=True)
     
     df['Sub_Category_2']=np.where(df.Type_of_Issue.isin(['ALL ISSUES NEW AND REFUNDING','BONDS','MEMO','STOCKS']),np.nan,df['Type_of_Issue'])
     df['Sub_Category']=np.where(df.Sub_Category_2.isin(['PRIVATE PLACEMENTS DOMESTIC']),np.nan,df['Sub_Category'])
     df['Sub_Category_2']=df['Sub_Category_2'].replace({'NONFINANCIAL':'NON FINANCIAL'})
     df['Sub_Category_2']=df['Sub_Category_2'].apply(lambda x: x.title() if pd.notnull(x) else x)
     
     df.drop(columns=['Type_of_Issue'], inplace=True)
     
     df = pd.melt(df, id_vars=['Sl_No','Category','Sub_Category','Sub_Category_2'], var_name='Relevant_Date', value_name='Value_USD_Mn')
     df['Relevant_Date']=df['Relevant_Date'].apply(lambda x: convert_date_format(x))
     df['Relevant_Date'] = pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
     df['Relevant_Date']=df['Relevant_Date'].dt.date

     df['Value_USD_Mn']=df['Value_USD_Mn'].replace({'n.a.':np.nan})
     df['Value_USD_Mn']=df['Value_USD_Mn'].apply(lambda x: clean_values(x))
     
     df['Runtime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

     
     return df
#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
    # os.chdir('/home/ubuntu/AdQvestDir')
   
    #     #****   Date Time *****
   

    job_start_time = datetime.datetime.now(india_time)
    table_name = "US_CORPORATE_BORROWING_MONTHLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
            
        #%%
        max_rel_date = pd.read_sql('SELECT Max(Relevant_Date) as max FROM US_CORPORATE_BORROWING_MONTHLY_DATA', con=engine)['max'][0]
        # max_rel_date=pd.to_datetime('2010-01-31',format='%Y-%m-%d').date()
        dates = pd.date_range(max_rel_date+relativedelta(months=1), today.date()+relativedelta(months=1) , freq='M')[::-1]


        #%%
        for dt in dates:
            dt=dt.date()
            print(f'Working on ---->{(dt)}')
            
            
            serach_key=dt.strftime('%Y')+dt.strftime('%m')+dt.strftime('%d')
            if (today.date() - dt).days <= 60:
                link='https://www.federalreserve.gov/data/corpsecure/current.htm'
                print(link)
                robot.add_link(link)
                r1=get_request_session(link)
                print(r1)
                df=pd.read_html(r1.content)
                
            else:
                try:
                    link="https://www.federalreserve.gov/data/corpsecure/corpsecure"+serach_key+".htm"
                    # robot.add_link(link)
                    r1=get_request_session(link)
                    df=pd.read_html(r1.content)
                    print(r1)
                    
                except:
                    link="https://www.federalreserve.gov/econresdata/releases/corpsecure/corpsecure"+serach_key+".htm"
                    r1=get_request_session(link)
                    # robot.add_link(link)
                    df=pd.read_html(r1.content)
                    print(r1)
                
            
           
            #%%
            df=process_df(df)   
            if len(df)>0:
                df=drop_duplicates(df)
                print(min(df['Relevant_Date']))
                engine.execute(f"Delete from US_CORPORATE_BORROWING_MONTHLY_DATA where Relevant_Date>='{min(df['Relevant_Date'])}'")
                Upload_Data('US_CORPORATE_BORROWING_MONTHLY_DATA',df,['MySQL'])

            
                    

        
        #%%
        log.job_end_log(table_name, job_start_time, no_of_ping)


    except:
            try:
                connection.close()
            except:
                pass
            error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
            print(error_msg)
       
            log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

        
if (__name__ == '__main__'):
    run_program(run_by='manual')

