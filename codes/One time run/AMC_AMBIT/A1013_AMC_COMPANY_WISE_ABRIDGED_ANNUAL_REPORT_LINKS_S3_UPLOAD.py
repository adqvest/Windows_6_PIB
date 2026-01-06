import re
import boto3
import requests
import sys
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
# sys.path.insert(0, 'D:/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
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
pdf_down_dir=r"C:/Users/Administrator/AdQvestDir/AMC_ABRIDGED/"
req_quarter='Q3 FY24'
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
    
    response=zen_req.get(url)
    if response.status_code!=200:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56"}
        response=requests.get(url,headers=headers,verify=False,timeout=60)


    if response.status_code == 200:
        with open(pdf_down_dir+sanitized_filename, 'wb') as file:
            file.write(response.content)
            print("PDF downloaded successfully.")
        
        file_size = os.path.getsize(pdf_down_dir+sanitized_filename)
        size_threshold = 1 * 1024  # 1 KB in bytes
        
        if file_size > size_threshold:
            S3_upload(sanitized_filename,'AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT')
            S3_upload(sanitized_filename,'AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_TO_BE_CHUNKED')
            os.remove(pdf_down_dir+sanitized_filename)
            return 'DONE'
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
    table_name = 'AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS_S3_UPLOAD'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
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

        execution_list=pd.read_sql("select Company_Name,count(*) as No_links from AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS group by Company_Name order by No_links asc;", con=engine)
        execution_list=execution_list['Company_Name'].to_list()
        # execution_list = [
        #             "Kotak Mahindra Mutual Fund",
        #             "Mirae Mutual Fund",
        #             "DSP Mutual Fund",
        #             "Axis Mutual Fund",
        #             "Nippon Mutual Fund",
        #             "ICICI Prudential Mutual Fund",
        #             "SBI Mutual Fund",
        #             "HDFC Mutual Fund",
        #             "Aditya Birla Sun Life Mutual Fund",
        #             "Franklin Templeton Mutual Fund",
        #             "TATA Mutual Fund",
        #             "Bandhan Mutual Fund",
        #             "Edelweiss Mutual Fund",
        #             "HSBC Mutual Fund",
        #             "Franklin Templeton Mutual Fund",
        #             "Canara Robeco Mutual Fund",
        #             "Invesco Mutual Fund",
        #             "PPFAS Mutual Fund",
        #             "Sundaram Mutual Fund"
        #         ]
        
        execution_list=["Axis Mutual Fund"]

        for company in execution_list:

            # status_empty_data=pd.read_sql(f"select * from AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS", con=engine)
            status_empty_data=pd.read_sql(f"select * from AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS where S3_Update_Status is null and Company_Name='{company}' and Relevant_Year in ('FY22','FY23','FY24')", con=engine)
            status_empty_data=status_empty_data[status_empty_data['File_Link'].str.contains('.pdf')].reset_index(drop=True)
            status_empty_data=status_empty_data[~status_empty_data['File_Link'].str.contains('.zip')].reset_index(drop=True)
            status_empty_data=status_empty_data[status_empty_data['File_Link']!='-'].reset_index(drop=True)
            c=0
            for x in range(len(status_empty_data)):
                print(f'-------------------{c}')
                df = pd.DataFrame(columns=['Distinct_File_Name', "File_Link",'File_Id'])

                file_url = str(status_empty_data.loc[x]["File_Link"])
                file_name=str(status_empty_data.loc[x]["Distinct_File_Name"])
                file_name = re.sub(' ', '_', file_name)
                file_name = re.sub('/', '_', file_name)+ '.pdf'
                try:
                    status_val=upload_to_s3(file_url, file_name)
                    # os.remove(file)
                except:
                    status_val='TIMEOUT ISSUE'
                try:
                    if status_val=='DONE':
                        query='Update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set S3_Update_Status="DONE" where  File_Id='+str(int(status_empty_data.loc[x]["File_Id"]))
                        connection.execute(query)
                        time.sleep(2)
                        print('Done updates')
                    else:
                        query='Update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set S3_Comments="'+status_val+'" where File_Id='+str(int(status_empty_data.loc[x]["File_Id"]))
                        connection.execute(query)
                        time.sleep(2)
                        print(status_val)
                except:
                    engine = adqvest_db.db_conn()
                    connection = engine.connect()

                    if status_val=='DONE':
                        query='Update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set S3_Update_Status="DONE" where File_Id='+str(int(status_empty_data.loc[x]["File_Id"]))
                        connection.execute(query)
                        time.sleep(2)
                        print('Done updates')
                    else:
                        query='Update AMC_COMPANY_WISE_ABRIDGED_ANNUAL_REPORT_LINKS set S3_Comments="'+status_val+'" where  File_Id='+str(int(status_empty_data.loc[x]["File_Id"]))+' and Report_Type="'+status_empty_data.iloc[x]["Report_Type"]+'"'
                        connection.execute(query)
                        time.sleep(2)
                        print(status_val)
                c=c+1
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
