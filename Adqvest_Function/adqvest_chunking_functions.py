# -*- coding: utf-8 -*-
"""
Created on Wed Jul  2 01:44:34 2025

@author: Santonu
"""

import sys
import os
from fiscalyear import *
import fiscalyear

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import pandas as pd
import adqvest_s3
import boto3
import ntpath
from botocore.config import Config
import requests
from botocore.exceptions import NoCredentialsError


os.chdir('C:/Users/Administrator/AdQvestDir/')

def to_s3bucket(filepath,key):
    BUCKET_NAME = 'adqvests3bucket'

    filename = ntpath.basename(filepath)

    data = open(filepath, 'rb')
    s3 = boto3.resource(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
    # Uploading the file to S3 bucket
    s3.Bucket(BUCKET_NAME).put_object(Key=key + filename, Body=data)
    print("File Uploaded to S3 Bucket!")

def from_s3bucket(key,download_path,filename):
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
    # Downloading the file from S3 bucket
    s3.download_file(f'{BUCKET_NAME}',f'{key +"/"+filename}',f'{download_path + filename}')
    print("File downloaded from S3 Bucket!")




def get_financial_year(dt,prev_fiscal=False):
    fiscalyear.setup_fiscal_calendar(start_month=4)
    if dt.month<4:
        yr=dt.year
    else:
        yr=dt.year+1

    if prev_fiscal==True:
        yr=yr-1

    fy = FiscalYear(yr)
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    fy_year=str(fy.start.year)+'-'+str(fy.end.year)[-2:]
    # fy_year=str(fy.start.year)+'-'+str(fy.end.year)
    return str(fy.end.year)[-2:]

def get_fiscal_quarter(date, fiscal_start_month=4):
        month = date.month
        fiscal_quarter = ((month - fiscal_start_month) % 12) // 3 + 1
        return fiscal_quarter

def get_quarter_fy_from_date(dt):
    qtr="Q"+str(get_fiscal_quarter(date))
    fy='FY'+get_financial_year(date)
    return qtr+'_'+fy

#------------------------------------------------------------------------------------------------------------
def delete_file_from_s3(file_name,bucket_name = 'adqvests3bucket'):
    s3 = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_name)
#         print(f"Deleted {file_name} from S3 bucket: {bucket_name}")
        print("Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")
#-----------------------------------------------------------------------------------------------------------------------
def read_large_file_names_from_s3_folder(folder_name, size_threshold_mb=1,bucket_name="adqvests3bucket"):
    # Convert the size threshold to bytes (1 MB = 1,048,576 bytes)
#     size_threshold_bytes = size_threshold_mb * 1024 * 1024
    
    s3_client = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
    
    large_file_names = []
    page_count = 0
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
#                 if obj['Size'] > size_threshold_bytes:
                large_file_names.append(obj['Key'])
        # if page_count > 10:
        #     break
        page_count += 1
    return large_file_names
#-------------------------------------------------------------------------------------------------------
def read_all_file_names_from_s3_folder(folder_name,bucket_name="adqvests3bucket"):
    
    s3_client = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
    
    file_names = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                file_names.append(obj['Key'])

    print("-----------------Files Reading Done----------------------------")
    return file_names
#----------------------------------------------------------------------------------------
def read_all_file_content(obj,bucket_name='adqvests3bucket'):
    
    s3 = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )

    files_content = {}
    file_key = obj
    if not file_key.endswith('/'):

        file_name = file_key.split('/')[-1]

        file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_response['Body'].read()

        files_content[file_name] = file_content
    
    return files_content,file_name
#--------------------------------------------------------------------------------------
def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    # Embed the documents
    doc_embds = vo.embed(
        chunked_text, model="voyage-2", input_type="document"
    ).embeddings
    
    return doc_embds
#-------------------------------------------------------------------------------------