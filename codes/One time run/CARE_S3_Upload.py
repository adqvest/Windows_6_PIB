#!/usr/bin/env python
# coding: utf-8

# In[4]:


import sqlalchemy
import pandas as pd
from pandas.io import sql

import xml.etree.ElementTree as ET
import os
import requests
import json
from bs4 import BeautifulSoup
import PyPDF2
#import camelot
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import time
import zipfile
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import warnings
warnings.filterwarnings('ignore')
import adqvest_db
import JobLogNew as log
import adqvest_s3
#import rpy2.robjects as ro
import boto3
from botocore.config import Config




def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/CARE_DOWNLOAD_FOLDER')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CARE_RATINGS_DAILY_FILES_LINKS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        os.chdir('C:/Users/Administrator/AdQvestDir/CARE_DOWNLOAD_FOLDER')
        # access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
        # ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
        # ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        BUCKET_NAME = 'adqvests3bucket'

        links_df = pd.read_sql("select * from CARE_RATINGS_DAILY_FILES_LINKS where Download_Status is null and Abbreviation_Status is null", con=engine)
        links_df = links_df.sort_values(by = "Relevant_Date",ascending = True)
        for _,i in links_df.iterrows():
            try:
                url = i["Links"]
                print(url)
                no_of_ping += 1
                #ro.r('download.file("'+url+'", destfile = "C:/Users/Administrator/AdQvestDir/CARE_DOWNLOAD_FOLDER/' + i['File_Name'] + '", mode="wb",method = "libcurl")')
                r =  requests.get(url,verify = False,headers={"User-Agent": "XY"})
                path = "C:/Users/Administrator/AdQvestDir/CARE_DOWNLOAD_FOLDER/" + i['File_Name']
                with open(path,'wb') as f:
                    f.write(r.content)
                    f.close()

                query = "update CARE_RATINGS_DAILY_FILES_LINKS set Download_Status='Yes' where Relevant_Date='" + i['Relevant_Date'].strftime("%Y-%m-%d") + "' and Company_Name = '"+i["Company_Name"]+"'"
                print(query)
                connection.execute(query)
                connection.execute('commit')
                time.sleep(1)

                #upload data to s3 bucket


                file_name = i['File_Name']
                data =  open(path, 'rb')
                s3 = boto3.resource(
                    's3',
                    aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,
                    config=Config(signature_version='s3v4',region_name = 'ap-south-1')

                )
                #Uploading the file to S3 bucket
                s3.Bucket(BUCKET_NAME).put_object(Key='CARE_Ratings/'+i['File_Name'], Body=data)
                data.close()

                os.remove(path)


            except:
                print("No data")
                query = "update CARE_RATINGS_DAILY_FILES_LINKS set Download_Status='Failed' where Relevant_Date='" + i['Relevant_Date'].strftime("%Y-%m-%d") + "' and Company_Name = '"+i["Company_Name"]+"'"
                print(query)
                connection.execute(query)
                connection.execute('commit')



        log.job_end_log(table_name,job_start_time, no_of_ping)
        connection.close()
    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
