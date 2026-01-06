import re
import boto3
import requests
import sys
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'D:/AdQvestDir/Adqvest_Function')
import adqvest_db
import time
import pandas as pd
import xml.etree.ElementTree as ET
import datetime as dt
from botocore.config import Config
from botocore.exceptions import NoCredentialsError
from sqlalchemy import text
from datetime import datetime,timedelta
from pytz import timezone
import JobLogNew as log
# from zenrows import ZenRowsClient
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import httpx

import warnings
warnings.filterwarnings('ignore')
from zenrows import ZenRowsClient
zen_req = ZenRowsClient("eabc162057980f957e89b6e71cb469e438b41393")
#%%
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.now(india_time)
days       = timedelta(1)
yesterday = today - days

#%%
# d=[i for i in range(290151, 575414, 50000)]
pdf_down_dir=r"C:/Users/Santonu/Downloads/NSE/"
pdf_down_dir=r"C:/Users/Administrator/AdQvestDir/NSE_CORPUS_3/"

#%%

def sanitize_filename(filename):
    return filename.replace(':', '_')

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY ='2icYcvBViEnFyXs7eHF30WMAhm8hGWvfm5f394xK'
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
         's3',
         aws_access_key_id=ACCESS_KEY_ID,
         aws_secret_access_key=ACCESS_SECRET_KEY,
         config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
    data_s3 =  open(filename, 'rb')
    s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
    data_s3.close()
    print("Data uploaded to S3")
    
def upload_to_s3(url,filename):
    os.chdir(pdf_down_dir)
    sanitized_filename = sanitize_filename(filename)
    print(sanitized_filename)
    
    response=zen_req.get(url,timeout=300)
    # if response.status_code!=200:
    #     headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56"}
    #     response=requests.get(url,headers=headers,verify=False)


    if response.status_code == 200:
        with open(pdf_down_dir+sanitized_filename, 'wb') as file:
            file.write(response.content)
            print("PDF downloaded successfully.")
        
        file_size = os.path.getsize(pdf_down_dir+sanitized_filename)
        size_threshold = 1 * 1024  # 1 KB in bytes
        
        if file_size > size_threshold:
            S3_upload(sanitized_filename,'NSE_INVESTOR_INFORMATION_CORPUS_3')
            S3_upload(sanitized_filename,'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_3')
            os.remove(pdf_down_dir+sanitized_filename)
            return 'Done'
        else:
            return 'SIZE ISSUE'
    else:
        return 'SITE ISSUE'
    
        
#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    ## job log details
    job_start_time = datetime.now(india_time)
    #add table name
    table_name = 'NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = 'Test_MCA_server'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

       
#%%     

        delete_pdf = os.listdir(pdf_down_dir)
        delete_pdf = [os.path.join(pdf_down_dir, f) for f in delete_pdf]
        print(delete_pdf)
        for file in delete_pdf:
                os.remove(file)

        status_empty_data=pd.read_sql(f"select * from NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS where S3_Upload_Comments like '%%ISSUE%%' and Runtime >'2025-03-05 18:17:03' order by File_Id asc;", con=engine)
        status_empty_data=status_empty_data[status_empty_data['Links'].str.contains('.pdf')].reset_index(drop=True)
        status_empty_data=status_empty_data[~status_empty_data['Links'].str.contains('.zip')].reset_index(drop=True)
        status_empty_data=status_empty_data[status_empty_data['Links']!='-'].reset_index(drop=True)

        status_empty_data=status_empty_data[~status_empty_data['Links'].str.contains('.zip')].reset_index(drop=True)
        status_empty_data=status_empty_data[status_empty_data['Links']!='-'].reset_index(drop=True)
        status_empty_data = status_empty_data[3001:4500].reset_index(drop=True)
        print(status_empty_data)
        c=0
        for x in range(len(status_empty_data)):
            print(f'-------------------{c}')
            file_url = str(status_empty_data.loc[x]["Links"])
            file_name=str(status_empty_data.loc[x]["File_Name"])
            file_id=str(status_empty_data.loc[x]["File_Id"])
            try:
                status_val=upload_to_s3(file_url, file_name+ '.pdf')
                # os.remove(file)
            except:
                status_val='TIMEOUT ISSUE'
            
            try:
                if status_val=='Done':
                    query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set S3_Upload_Status='Done',S3_Upload_Comments=NULL where File_Id='{file_id}'"
                    connection.execute(query)
                    time.sleep(2)
                    print('Done updates')
                else:
                    query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set S3_Upload_Comments='{status_val}' where File_Id='{file_id}'"
                    connection.execute(query)
                    time.sleep(2)
                    print(status_val)
            except:
                pass
           
            
            c=c+1

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
