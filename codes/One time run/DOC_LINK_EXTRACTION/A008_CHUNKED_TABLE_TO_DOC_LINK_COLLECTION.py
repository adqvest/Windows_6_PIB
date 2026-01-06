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
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)

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



# def extract_links_from_pdf(pdf_content):
     
#      # pdf_content =download_file_content(link)
#      doc = fitz.open(stream=pdf_content, filetype="pdf")
#      page_links={}
#      for page_no,page_text in enumerate(doc):
#          # print(page_text)
#          links_raw = page_text.get_links()
#          links=[i["uri"] for i in links_raw if  "uri" in i and i["uri"]]
#          links=list(set(links))
#          page_links[page_no]=links
#      doc.close()
#      return page_links
 
    
url_regex = re.compile(
                     r"""(?i)\b(                            # start group
                         (?:https?)://                      # protocol
                         [^\s<>"]+                          # host and path
                         | www\.[^\s<>"]+                   # or www.* links
                         | [a-zA-Z0-9.-]+\.[a-zA-Z]{2,}     # or bare domain.tld
                         (?:/[^\s]*)?                       # optional path
                     )""",
                     re.VERBOSE
                 )

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
   # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
    # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
    # os.chdir('/home/ubuntu/AdQvestDir/Adqvest_Function')

    job_start_time = today
    table_name = 'NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_DOC_LINKS'
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
       
        
            tables=chunking_client.execute("SHOW TABLES;")
            tables=[i[0] for i in tables ]
            table='NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_CHUNKED'
            doc_id_collected=pd.read_sql("Select  Document_Id from NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_DOC_LINKS group by Document_Id",con=engine)
            doc_ids=chunking_client.execute("Select Distinct Document_Id from AdqvestDB."+table+" group by Document_Id;")
            doc_ids=[i[0] for i in doc_ids]
            doc_ids=[i for i in doc_ids if i not in doc_id_collected['Document_Id'].to_list()]
            c=0
            final_df=pd.DataFrame()
            for doc in doc_ids:
                print(doc,"--------",c)
                df = chunking_client.execute(f"Select * from AdqvestDB.{table} WHERE Document_Id='{doc}';")
                df = pd.DataFrame(df, columns=[column[0] for column in chunking_client.execute(f"desc {table}")])
                df.drop(columns=['Embedding','Symbol','Industry_Sub_Category', 'Industry_Sub_Category_2','Published_Date',
                                 'Vector_Db_Status','Runtime_Scraped', 'Runtime_Chunking', 'Runtime_Milvus','Runtime_Final_Corpus',],inplace=True)
                
                
                df['Extracted_Links'] = df['Document_Content'].apply(lambda x: ",".join(list(set(url_regex.findall(x)))))
                # df.drop(columns=['Document_Content'],inplace=True)
                df=df[df['Extracted_Links'].ne('')]
      
                final_df=pd.concat([final_df,df])
                c=c+1
                if c==1000:
                    final_df['Relevant_Date'] = today.date()
                    final_df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    final_df.drop(columns=['Document_Content'],inplace=True)
                    final_df['Source_Table']=table
                    Upload_Data('NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS_DOC_LINKS',final_df,["MySQL"])
                    final_df=pd.DataFrame()
                    c=0
            
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
