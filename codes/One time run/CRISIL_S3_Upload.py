# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 11:22:45 2022

@author: Abdulmuizz
"""

import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
#import matplotlib.pyplot

import os
import requests
import json
from bs4 import BeautifulSoup
#import tabula

from time import sleep
import random
#import camelot
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import zipfile
import sys
import time
#import tabula
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import glob

#import rpy2.robjects as ro
import os


import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import warnings
warnings.filterwarnings('ignore')

import adqvest_s3
#import rpy2.robjects as ro
import boto3
from botocore.config import Config

os.chdir('C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER')
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


# access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
# ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
# ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
BUCKET_NAME = 'adqvests3bucket'

files_in_drive = os.listdir()

query_df = "select * from CRISIL_FILES_LINKS_DAILY_DATA where Download_Status is null and Relevant_Date < '2022-03-25'"

crisil_link = pd.read_sql(query_df, con=engine)
if(crisil_link.shape[0]>0):
    crisil_link['Mon_Day'] = crisil_link['Relevant_Date'].apply(lambda x:x.strftime("%m-%d"))
    links_df = pd.DataFrame()
    for _, df in crisil_link.groupby('Mon_Day'):
        links_df = pd.concat([links_df, df])
    links_df = links_df.sort_values(by = "Relevant_Date",ascending = False)
    for _, row in links_df.iterrows():
        print(row['Rating_File_Name'])

        url = row['Rating_File_Link']
        #ro.r('download.file("'+url+'", destfile = "/home/ubuntu/crisil_data/' + row['Rating_File_Name'] + '", mode="wb")')
        r =  requests.get(url,verify = False,headers={"User-Agent": "XY"})
        time.sleep(3)
        path = "C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER/"  + row['Rating_File_Name']
        with open(path,'wb') as f:
            f.write(r.content)
            f.close()

        try:
            data =  open(path, 'rb')
            s3 = boto3.resource(
                's3',
                aws_access_key_id=ACCESS_KEY_ID,
                aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4',region_name = 'ap-south-1')

            )
            #Uploading the file to S3 bucket
            s3.Bucket(BUCKET_NAME).put_object(Key='CRISIL_Ratings/'+row['Rating_File_Name'], Body=data)
            data.close()
            query = "update CRISIL_FILES_LINKS_DAILY_DATA set Download_Status ='Yes' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
            connection.execute(query)
            connection.execute('commit')
            print('S3 Updated')
            os.remove(path)
        except:

            query = "update CRISIL_FILES_LINKS_DAILY_DATA set Download_Status ='No' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
            connection.execute(query)
            connection.execute('commit')
            print('Not Updated')
            os.remove(path)
            continue



print('All Uploaded')
