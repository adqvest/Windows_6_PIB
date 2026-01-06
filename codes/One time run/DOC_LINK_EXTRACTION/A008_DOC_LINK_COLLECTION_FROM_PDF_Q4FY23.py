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
import numpy as np
import pandas as pd
from tqdm import tqdm
import io
import  fitz
from pytz import timezone
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
from  dbfunctions import read_all_file_names_from_s3_folder,read_all_file_content

#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#     #****   Date Time *****

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, data, db: list):
    # query=f"select distinct Relevant_Date as Relevant_Date from {table_name}"
    # db_max_date = pd.read_sql(query,engine)
    # data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(data.info())



def extract_links_from_pdf(pdf_content):
     try:
         # pdf_content =download_file_content(link)
         doc = fitz.open(stream=pdf_content, filetype="pdf")
         page_links=set()
         for page_no,page_text in enumerate(doc):
            
             links_raw = page_text.get_links()
             # print(links_raw)
             links=(i["uri"] for i in links_raw if  "uri" in i and i["uri"])
             page_links.update(links)
         doc.close()
         # page_links=(i for i in page_links if re.search('mailto',i)==False)
         page_links = [i for i in page_links if not re.search(r'^mailto:', i)]
         return ",".join(page_links)
     except:
        return np.nan
     
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
   # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
    # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
    # os.chdir('/home/ubuntu/AdQvestDir/Adqvest_Function')

    job_start_time = today
    table_name = 'CORPUS_TABLE_WISE_EXTRACTED_DOC_LINKS_STATIC'
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
            # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")
            
            doc_id_collected=pd.read_sql("Select  Document_Id from CORPUS_TABLE_WISE_EXTRACTED_DOC_LINKS_STATIC group by Document_Id",con=engine)
           
            
            
            s3_folder='NSE_INVESTOR_INFORMATION_CORPUS_2'
            table='NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED'
            
            req_quarter=['Q4 FY23']
            
            
            response = read_all_file_names_from_s3_folder(s3_folder)
            response = [ x for x in response if f'{s3_folder}/' in x]
            response=[i for i in response if ' '.join(i.split('_')[-3:-1])  in req_quarter]
         
            
            df_s3=pd.DataFrame()
            df_s3['S3_Path']=response
            df_s3['Document_Id']=df_s3['S3_Path'].apply(lambda x:x.split('_')[-1].split('.pdf')[0])
            

            df = chunking_client.execute(f"Select Document_Id,Document_Company,Document_Type,Document_Year,Document_Date,max(Document_Link) as Document_Link from AdqvestDB.{table} WHERE Document_Year='{req_quarter[0]}' group by Document_Id,Document_Company,Document_Type,Document_Year,Document_Date;")
            df = pd.DataFrame(df, columns=[column for column in ['Document_Id','Document_Company','Document_Type','Document_Year','Document_Date','Document_Link']])
          
            df=df.merge(df_s3,on='Document_Id',how='inner')
            df['Extracted_Links']=np.nan
            df['Source_Table']=table
            df=df[~df['Document_Id'].isin(doc_id_collected['Document_Id'].to_list())]
            response=[]
            
            
            
            batch=[]
            batch_size=1000
            for index,obj in tqdm (df.iterrows(),total=len(df)):
                # print(index)
                files_content,file_name = read_all_file_content(obj['S3_Path'])
                extracted_links=extract_links_from_pdf(files_content)
                
                batch.append({**obj.to_dict(),'Extracted_Links': extracted_links})
                if len(batch) == batch_size:
                    df_batch = pd.DataFrame(batch)
                    df_batch['Relevant_Date'] = today.date()
                    df_batch['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    df_batch=df_batch[df_batch['Extracted_Links'].ne('')]
                    df_batch.drop(columns=['S3_Path'],inplace=True)
                    # df_batch = df_batch.drop_duplicates(subset=['Extracted_Links'], keep='last')
                    Upload_Data('CORPUS_TABLE_WISE_EXTRACTED_DOC_LINKS_STATIC', df_batch, ["MySQL"])
                    batch = []  
            
            if batch:
                df_batch = pd.DataFrame(batch)
                df_batch['Relevant_Date'] = today.date()
                df_batch['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df_batch=df_batch[df_batch['Extracted_Links'].ne('')]
                df_batch.drop(columns=['S3_Path'],inplace=True)
                # df_batch = df_batch.drop_duplicates(subset=['Extracted_Links'], keep='last')
                Upload_Data('CORPUS_TABLE_WISE_EXTRACTED_DOC_LINKS_STATIC', df_batch, ["MySQL"])

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
