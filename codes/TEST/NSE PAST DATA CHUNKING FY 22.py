# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 14:54:39 2025

@author: GOKUL
"""

import os
import io
import re
import ast
import PyPDF2
import boto3
import json
import voyageai
import psycopg2
import anthropic
# import adqvest_db
import pandas as pd
import numpy as np
from pytz import timezone
import datetime as datetime
import pypdfium2 as pdfium

from collections import Counter

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams

from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

import pytesseract
from pdf2image import convert_from_path

import warnings
warnings.filterwarnings('ignore')

import datetime as dt
import pytz
import time

# import adqvest_s3
import boto3
import ntpath
from botocore.config import Config
import requests
import boto3
from botocore.exceptions import NoCredentialsError

import pdfquery
import json
from lxml import etree
from io import StringIO


from datetime import timedelta
from dateutil.relativedelta import relativedelta


import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
connection = engine.connect()
# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days
#%%
os.environ["VOYAGE_API_KEY"] = "pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk"
# os.environ["VOYAGE_API_KEY"] = "pa-zTzrc5RTasN81O4SZXWF9iehCfFOpK0GwH_e7iK8rhI"


def pdf_to_text(pdf_file):
    """Convert a PDF file directly to a list of text per page."""
    # Load the PDF
    pdf = pdfquery.PDFQuery(io.BytesIO(pdf_file))
    pdf.load()  # Load all pages
    
    # Get a list of pages
    pages = pdf.extract([['pages', 'LTPage']])
    page_elements = pages['pages']
    
    def extract_text_from_element(element):
        """Recursively extract text from an XML element and its children."""
        text_list = []
        if element.text and element.text.strip():
            text_list.append(element.text.strip())
        
        # Recursively process all child elements
        for child in element:
            text_list.extend(extract_text_from_element(child))
        
        return text_list
    
    # Initialize a list to store text for each page
    text_per_page = []
    
    for page in page_elements:
        # Convert each page to XML (in-memory)
        xml_data = etree.tostring(page, pretty_print=True).decode('utf-8')
        
        # Parse XML from the in-memory string
        root = etree.fromstring(xml_data)
        
        # Extract text from the XML tree
        text_list = extract_text_from_element(root)
        
        # Join all text entries into a single string with line breaks
        extracted_text = '\n'.join(text_list)
        
        # Append to the list of texts per page
        text_per_page.append(extracted_text)
    
    return text_per_page



def read_large_file_names_from_s3_folder(bucket_name, folder_name, size_threshold_mb=1):
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    # Convert the size threshold to bytes (1 MB = 1,048,576 bytes)
#     size_threshold_bytes = size_threshold_mb * 1024 * 1024
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
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



def read_all_file_names_from_s3_folder(bucket_name, folder_name):
    count = 0
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)
    
    file_names = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                file_names.append(obj['Key'])
                
    return file_names


def read_all_file_content(obj,bucket_name='adqvests3bucket'):
    print("i am here")
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )

    files_content = {}

#     file_key = obj['Key']
    file_key = obj
    # Skip if the key is the folder itself
    if not file_key.endswith('/'):

        file_name = file_key.split('/')[-1]

        # Reading each file's content
        file_response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_response['Body'].read()

        files_content[file_name] = file_content
#         print(f"File '{file_name}' read from S3 Bucket!")
    
    return files_content,file_name



# Function to delete file from S3
def delete_file_from_s3(file_name,bucket_name = 'adqvests3bucket'):
    
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY = '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'
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=Config(signature_version='s3v4', region_name='ap-south-1')
    )
    
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_name)
#         print(f"Deleted {file_name} from S3 bucket: {bucket_name}")
        print(f"Deleted from S3 bucket")
    except Exception as e:
        print(f"Error deleting {file_name} from S3 bucket: {e}")



def extract_text_from_pdf(files_content,file_name):
    
    # Splitting file name to extract details
    parts = file_name.split('_')

    # Extract the company name (everything before the first '0')
    company_name = ' '.join(parts[:parts.index('0')])

    # Find the index of the year and file ID
    relevant_year_index = next(i for i, part in enumerate(parts) if 'FY' in part)

    # Extract relevant year and file ID
    relevant_year = parts[relevant_year_index - 1] + ' ' + parts[relevant_year_index]  # Example: 'Q1 FY23'
    file_id = parts[-1]

    # Extract document type (everything between the '0' and the relevant year)
    report_type = ' '.join(parts[parts.index('0') + 1:relevant_year_index - 1])

    file_id = file_id.replace('.pdf','')

    pdf_content = files_content[file_name]

    # Load the PDF document
    pdf = pdfium.PdfDocument(io.BytesIO(pdf_content))

    text = ''
    for page_number in range(len(pdf)):
        page_text = pdf[page_number].get_textpage().get_text_range()

        text += 'PAGE NUMBER: ' + str(page_number+1) + '\n\n'
        text += 'COMPANY NAME: ' + str(company_name.strip()) + '\n\n'
        text += 'REPORT TYPE: ' + str(report_type.strip()) + '\n\n'
        text += 'RELEVANT YEAR: ' + str(relevant_year.strip()) + '\n\n'
        text += page_text
        text += '\n\n' 
    text = re.sub('\n\n\n','\n\n', text)
    return company_name,report_type,relevant_year,file_id,text



def create_embeddings(chunked_text):
    
    vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
    # Embed the documents
    doc_embds = vo.embed(
        chunked_text, model="voyage-2", input_type="document"
    ).embeddings
    
    return doc_embds


def extract_chunks_from_text(text):
    
    chunks = text.split("PAGE NUMBER: ")
    chunks = chunks[1:]
    return chunks


def analyse_chunk(chunk):
    
    is_true = False 
    
    pattern_1 = r"\bQ[1-4] & Year (201[0-9]|202[0-4]) \b"
    pattern_2 = r"\bQ[1-4] (201[0-9]|202[0-4]) \b"
    pattern_3 = r"\bQ[1-4](201[0-9]|202[0-4]) \b"
    pattern_4 = r"\bQ[1-4] (1[0-9]|2[0-4]) \b"
    pattern_5 = r"\bQ[1-4](1[0-9]|2[0-4]) \b"

    if re.findall(pattern_1, chunk) != []:
        is_true = True

    elif re.findall(pattern_2, chunk) != []:
        is_true = True

    elif re.findall(pattern_3, chunk) != []:
        is_true = True

    elif re.findall(pattern_4, chunk) != []:
        is_true = True

    elif re.findall(pattern_5, chunk) != []:
        is_true = True
        
    return is_true


# Function to replace multiple patterns using a placeholder method
def replace_multiple_patterns(text, replacements):
    # Create a list of unique placeholders
    placeholders = {pattern: f"__PLACEHOLDER_{i}__" for i, pattern in enumerate(replacements)}

    # Replace each pattern with its unique placeholder
    for pattern, placeholder in placeholders.items():
        text = re.sub(pattern, placeholder, text)

    # Replace each placeholder with its final replacement value
    for pattern, placeholder in placeholders.items():
        text = re.sub(placeholder, replacements[pattern], text)

    return text


def modify_chunk(chunk):

    # Automatically generate the replacement patterns
    replacements = {}
    for year in range(2010, 2050):
        replacements[f"Q1 {year}"] = f"Q4 FY{year}"
        replacements[f"Q2 {year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3 {year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4 {year}"] = f"Q3 FY{year + 1}"

    for year in range(10, 50):
        replacements[f"Q1 {year}"] = f"Q4 FY{year}"
        replacements[f"Q2 {year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3 {year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4 {year}"] = f"Q3 FY{year + 1}"

    for year in range(2010, 2050):
        replacements[f"H1 {year}"] = f"H1 CY{year}"
        replacements[f"H2 {year}"] = f"H2 CY{year + 1}"
        replacements[f"H3 {year}"] = f"H3 CY{year + 1}"
        replacements[f"H4 {year}"] = f"H4 CY{year + 1}"

    for year in range(10, 50):
        replacements[f"H1 {year}"] = f"H1 CY{year}"
        replacements[f"H2 {year}"] = f"H2 CY{year + 1}"
        replacements[f"H3 {year}"] = f"H3 CY{year + 1}"
        replacements[f"H4 {year}"] = f"H4 CY{year + 1}"
        
    for year in range(2010, 2050):
        replacements[f"Q1{year}"] = f"Q4 FY{year}"
        replacements[f"Q2{year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3{year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4{year}"] = f"Q3 FY{year + 1}"

    for year in range(10, 50):
        replacements[f"Q1{year}"] = f"Q4 FY{year}"
        replacements[f"Q2{year}"] = f"Q1 FY{year + 1}"
        replacements[f"Q3{year}"] = f"Q2 FY{year + 1}"
        replacements[f"Q4{year}"] = f"Q3 FY{year + 1}"

    for year in range(2010, 2050):
        replacements[f"H1{year}"] = f"H1 CY{year}"
        replacements[f"H2{year}"] = f"H2 CY{year + 1}"
        replacements[f"H3{year}"] = f"H3 CY{year + 1}"
        replacements[f"H4{year}"] = f"H4 CY{year + 1}"

    for year in range(10, 50):
        replacements[f"H1{year}"] = f"H1 CY{year}"
        replacements[f"H2{year}"] = f"H2 CY{year + 1}"
        replacements[f"H3{year}"] = f"H3 CY{year + 1}"
        replacements[f"H4{year}"] = f"H4 CY{year + 1}"

    # Apply replacements to the third element of each tuple in filtered_data
    chunk = replace_multiple_patterns(chunk, replacements)

    return chunk


def parse_quarter_year(qy_str):
    # Check if the string contains a quarter
    qy_str = qy_str.lower()
    if 'q' in qy_str:
        q, fy = qy_str.split(' ')
        quarter = int(q[1])
        year = int(fy.replace('fy', '20'))
        
        if quarter in [1, 2, 3]:
            # For Q1, Q2, Q3: map to April, July, October
            month = 3 + (quarter * 3)
            year = year - 1  # Since Q1 FY25 starts in April 2024, which is FY25 (2025)
        elif quarter == 4:
            # For Q4: map to January of the fiscal year
            month = 3
            year = year
        else:
            raise ValueError(f"Invalid quarter: {quarter}")
        day = 1  # Set day as 1
        return datetime.datetime(year, month, day).date() + relativedelta(months=1) - timedelta(days=1)
    else:
        # If only fiscal year is provided, set to January 1st
        fy = qy_str
        year = int(fy.replace('fy', '20'))
        return datetime.datetime(year, 3, 1).date() + relativedelta(months=1) - timedelta(days=1)



from clickhouse_driver import Client
host = 'ec2-52-88-156-240.us-west-2.compute.amazonaws.com'
user_name = 'default'
password_string = 'Clickhouse@2024'
db_name = 'AdqvestDB'

client = Client(host, user=user_name, password=password_string, database=db_name)


bucket_name = 'adqvests3bucket'
folder_name = 'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_3'
#%%
# response = read_all_file_names_from_s3_folder(bucket_name, folder_name)
response = read_large_file_names_from_s3_folder(bucket_name, folder_name)
response = [ x for x in response if 'NSE_INVESTOR_INFORMATION_CORPUS_TO_BE_CHUNKED_3/' in x]
print(len(response))
# response = response[1:]
response.reverse()

response = [x for x in response if re.search(r'FY22', x)]

# req_quarter=['Q1 FY19','Q3 FY19']
# response=[i for i in response if ' '.join(i.split('_')[-3:-1]) not in req_quarter]

execution_list=pd.read_sql(f"select * from NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS where S3_Upload_Status = 'Done' and Chunking_Status is null and Runtime >'2025-03-05 18:17:03' group by File_Name", con=engine)
execution_list=execution_list['File_Name'].to_list()
response=[i for i in response if i.rsplit('/', 1)[-1].removesuffix('.pdf') in execution_list]


doc_ids = client.execute(f'''SELECT distinct document_id FROM NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED;''')
doc_ids = [str(x).replace('(','').replace(',)','') for x in doc_ids]
print(len(doc_ids))

for obj in response:
    print(obj)
    files_content,file_name = read_all_file_content(obj)
    
    print("STARTED FILE :",file_name)

    try:
        company_name,report_type,relevant_year,file_id,text = extract_text_from_pdf(files_content,file_name)
        if str(file_id) not in doc_ids :
            print(relevant_year)

            if int(relevant_year[5:])>=18 and int(relevant_year[5:])<30:
                company_name = company_name.replace("'","\\'") # Escaping single quotes
                document_date = parse_quarter_year(relevant_year)
                
                p=0
                extracted_chunks = extract_chunks_from_text(text)

                if (len(extracted_chunks)>0):
                    # text_data = pdf_to_text(files_content[file_name])

                    for chunk,page_number in zip(extracted_chunks,range(len(extracted_chunks))):
                        chunk = chunk.replace("\\", "\\\\")  # Escaping backslashes
                        chunk = chunk.replace("\n", "\\n")  # Escaping newlines
                        chunk = chunk.replace("\r", "\\r")  # Escaping carriage returns
                        chunk = chunk.replace("'","\\'") # Escaping single quotes
                        chunk = 'PAGE NUMBER: ' + chunk

                        is_true = analyse_chunk(chunk)
                        if is_true:
                            new_chunk = modify_chunk(chunk)
                        else:
                            new_chunk = chunk

                        try:
                            chunk_embed = str(create_embeddings(chunk)).replace('[[','[').replace(']]',']')
                            client.execute(f"INSERT INTO NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS_CHUNKED (document_id,document_company,document_type,document_year,document_date,document_content,document_content_modify,embedding) VALUES ('{file_id}','{company_name}','{report_type}','{relevant_year}','{document_date}','''{chunk}''','''{new_chunk}''',{chunk_embed});") 
                        except:
                            p=p+1
                            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                            print("****** PAGE WAS SKIPPED ******",page_number)     
                            print(error_msg)

                    # delete_file_from_s3(obj)
                    print('FILE COMPLETED')
                    print(file_name)
                    # print(file_name.removesuffix('.pdf'))
                    # print(re.sub('.pdf','',file_name))
                    try:
                        file_name=re.sub('.pdf','',file_name)
                        query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set Chunking_Status='Done',Chunking_Comments=Null where File_Name='{file_name}'"
                        print(query)
                        connection.execute(query)
                    except:
                        file_name=re.sub('.pdf','',file_name)
                        query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set Chunking_Status='Done',Chunking_Comments=Null where File_ID='{file_id}'"
                        print(query)
                        connection.execute(query)

                    if p==len(extracted_chunks):
                        print('==========================================================')
                        query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set Chunking_Comments='No page extracted',Chunking_Status=Null where  File_ID='{file_id}'"
                        print(query)
                        connection.execute(query)


                   

                else:
                    query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set Chunking_Comments='No page extracted' where  File_ID='{file_id}'"
                    print(query)
                    connection.execute(query)
                    print("FILE IS TOO SMALL")
            else:
                print("FILE NOT RELEVANT")

        else:
            print('FILE ALREADY CHUNKED')
            # delete_file_from_s3(obj)


    except:
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        file_name=re.sub('.pdf','',file_name)
        print("****************** SOMETHING WRONG WITH FILE ******************")
        query=f"Update NSE_INVESTOR_INFORMATION_PAST_DATA_CORPUS set Chunking_Comments='{error_msg}' where File_ID='{file_id}'"
        print(query)
        connection.execute(query)
        print("FILE IS TOO SMALL")

