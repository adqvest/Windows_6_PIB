# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 13:53:25 2025

@author: Santonu
"""
import re
import sys
import time
import warnings
import os
from pytz import timezone
import numpy as np
import pandas as pd
from tqdm import tqdm
import io
# import  fitz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import datetime

from clickhouse_driver import Client
chunking_client = Client('ec2-52-27-243-167.us-west-2.compute.amazonaws.com',
                user='default',
                password='Clickhouse@2024',
                database='AdqvestDB',
               port=9000)
#%%
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import JobLogNew as log
import adqvest_db
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import MySql_To_Clickhouse as MySql_CH
from  geoclean import geo_clean
from dbfunctions import get_quarter_fy_from_date
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#     #****   Date Time *****

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

WORKING_TABLE='NSE_DOC_LINKS_FROM_CORPUS_DAILY_DATA'


def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    columns.remove('File_ID')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, df, db: list, filter_columns=["Relevant_Date"]):
    if not filter_columns:
        print("⚠️ No filter columns provided. Skipping filtering.")
        return

    column_str = ", ".join(filter_columns)
    query = f"SELECT {column_str} FROM {table_name} GROUP BY {column_str}"
    collected_files = pd.read_sql(query, engine)
    
    for col in filter_columns:
        df = df[~df[col].isin(collected_files[col].tolist())]
    
    df=drop_duplicates(df)
    if 'MySQL' in db and not df.empty:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print("✅ Uploaded data:")
        print(df.info())
    else:
        print("⚠️ No new data to upload.")



def process_collected_data(df):
    df['Prefix']=df['Source_File_ID'].apply(lambda x:re.findall(r'[A-Z]', x)[0])
    try:
        file_id=pd.read_sql(f"select Distinct File_Id  from {WORKING_TABLE}",engine)
        file_id=max([int(re.findall("\d+", i)[0]) for i in file_id['File_Id'].to_list() if re.findall(r"\d+", i)], default=0)


    except:
        file_id=0
        
    df['File_ID']=range(int(file_id+1),int(file_id)+len(df)+1)
    df['File_ID']=df['Prefix']+df['File_ID'].astype(str)
    
    
    df['Report_Type']=df['Report_Type'].apply(lambda x:x.title())      
    df['Company_Name']=df['Company_Name'].apply(lambda x:x.title())                 
    df['File_Name'] = df['Company_Name'].astype(str) +\
                      "_0_"+df['Report_Type'].astype(str)+"_" + \
                      df['Relevant_Quarter'].astype(str) + "_" + \
                      df['File_ID'].astype(str)
    
    df['File_Name']=df['File_Name'].apply(lambda x:x.replace(".",""))
    df['File_Name']=df['File_Name'].apply(lambda x:x.replace("'S","s"))
    df['File_Name']=df['File_Name'].apply(lambda x:x.replace(" - ",""))
    df['File_Name']=df['File_Name'].apply(lambda x:re.sub(' ', '_', x))
    df['File_Name']=df['File_Name'].apply(lambda x:re.sub('/', '_', x)+'.pdf')
    df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

    return df
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
   # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
    # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
    os.chdir('C:/Users/Administrator/AdQvestDir')

    job_start_time = today
    table_name = WORKING_TABLE
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
            if(run_by == 'Adqvest_Bot'):
                log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
            else:
                log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            #%% 
            source_table="NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED"
            query=f"""
                    SELECT
                    Document_Id as Source_File_ID,
                    Sector,
                    Industry,
                    Document_Company as Company_Name,
                    Document_Type as Report_Type,
                    Document_Year as Relevant_Quarter,
                    Document_Date as Document_Date,
                    Document_Link,
                    url
                    FROM AdqvestDB.{source_table}
                    ARRAY JOIN extractAll(
                    Document_Content,
                    '(https?://[^\\s<>\"'']+\\.(pdf|doc|docx|xls|xlsx|ppt|pptx|txt|csv|zip|rar)[^\\s<>\"'']*)'
                    ) AS url
                    WHERE
                    (
                    multiMatchAny(lower(url), [
                    'investor', 'presentation', 'conference', 'call', 'earnings',
                    'investor presentation', 'conference call', 'earnings call',
                    'investor relations', 'financial results'
                    ])
                    OR
                    multiMatchAny(lower(Document_Content), [
                    'investor', 'presentation', 'conference', 'call', 'earnings',
                    'investor presentation', 'conference call', 'earnings call',
                    'investor relations', 'financial results'
                    ])
                    )
                    AND length(url) > 0;
                    """
            req_columns=['Source_File_ID','Sector','Industry','Company_Name','Report_Type','Relevant_Quarter','Document_Date','Document_Link','File_Link']
            df = chunking_client.execute(query)
            df = pd.DataFrame(df, columns=[i for i in req_columns])


            df.columns=['Source_File_ID','Sector','Industry','Company_Name','Report_Type','Relevant_Quarter','Document_Date','Document_Link','File_Link']
            df['Document_Date'] = pd.to_datetime(df['Document_Date'], format='%Y-%m-%d')
            df['Document_Date']=df['Document_Date'].dt.date
            df['Relevant_Date'] = today.date()
           
            
            df=process_collected_data(df)
            df['Source_Table']=source_table
            df.drop(columns=['Prefix'],inplace=True)
            df=df[['File_ID', 'Source_File_ID', 'Sector', 'Industry', 'Company_Name', 'Report_Type','Document_Date',
                   'Relevant_Quarter',  'Document_Link', 'File_Link',
                   'File_Name', 'Source_Table','Relevant_Date', 'Runtime']]
           
            
            #%%
            Upload_Data(WORKING_TABLE,df,["MySQL"],filter_columns=['Source_File_ID'])
          
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
#%%
