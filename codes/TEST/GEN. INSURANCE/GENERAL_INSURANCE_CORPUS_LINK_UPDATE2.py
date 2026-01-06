# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 19:29:33 2024

@author: Rahul
"""

import warnings
warnings.filterwarnings('ignore')
import os
import re
import sys
import requests
import pandas as pd
from pytz import timezone
from datetime import datetime
import urllib.parse
from urllib.parse import unquote
from bs4 import BeautifulSoup

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def modify_url(x):
    
    page=str(x['no_pages'])
    doc_link=x['document_link']
    return doc_link+'#page='+page




def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    ## job log details
    job_start_time = datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        from clickhouse_driver import Client

        host = 'ec2-52-11-204-251.us-west-2.compute.amazonaws.com'
        user_name = 'default'
        password_string = 'Clickhouse@2024'
        db_name = 'AdqvestDB'

        client = Client(host, user=user_name, password=password_string, database=db_name)    

        df_links=pd.read_sql("select * from AdqvestDB.GEN_INSURANCE_LINK_UPDATE_STATUS_Temp_Rahul",engine)
        df_links=df_links[3000:6000]

        for i, row in df_links.iterrows():
            doc_id=row['document_id']
            print("Document ID :", doc_id)
            
            query = f"""select * from thurro_pdf_documents_vector_db_gen_insurance_corpus_final_3 WHERE document_id = '{doc_id}'"""
            a,cols = client.execute(query,with_column_types=True)
            a = pd.DataFrame(a, columns=[tuple[0] for tuple in cols])
            df1=a.copy()
            df1['no_pages']=range(1,len(df1)+1)
            df1['document_link_modified']=df1.apply(modify_url, axis=1)
            del df1['no_pages']
            client.execute("INSERT INTO thurro_pdf_documents_vector_db_gen_insurance_corpus_final_4 VALUES",df1.values.tolist())
            
            connection=engine.connect()
            connection.execute(f"update GEN_INSURANCE_LINK_UPDATE_STATUS_Temp_Rahul set Status = 'Done' where document_id={doc_id}")
            connection.execute("commit")        
         
        

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
