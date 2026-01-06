# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 10:12:07 2024

@author: Santonu
"""
import datetime
import os
import re
import sys
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from playwright.sync_api import sync_playwright
from pytz import timezone
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
from selenium.webdriver.support.ui import Select
import pdfplumber
from fiscalyear import *
import fiscalyear
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime
warnings.filterwarnings('ignore')
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import clickhouse_connect
import psycopg2
import voyageai

client = clickhouse_connect.get_client(
    host='ddmner0dzc.asia-southeast1.gcp.clickhouse.cloud',
    user='default',
    password='py~88bOEq4c1a',
    secure=True
)

postgres_test_con = psycopg2.connect(
host="ec2-3-108-253-129.ap-south-1.compute.amazonaws.com",
database="adqvest",
user="postgres",
password="@Thur&TPa@##123",
port=5432)
cursor_test = postgres_test_con.cursor()

#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
job_start_time = datetime.datetime.now(india_time)
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
#%%

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "KPMG_PM_POSHAN"
    scheduler = ''
    no_of_ping=0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        def create_embeddings(chunked_text):
    
            vo = voyageai.Client(api_key="pa-97JpiKNlfEEkhs8dBSZOloXM7aw3BI3NJ0ra-zaCSIk")
            # Embed the documents
            doc_embds = vo.embed(
                chunked_text, model="voyage-2", input_type="document"
            ).embeddings
            
            return doc_embds
            
        cursor_test.execute("SELECT widget_id, title, description, RTRIM(REGEXP_REPLACE(concat(sector_name, ' ', sub_sector, ' ', company, ' ', geography, ' ',sub_geo, ' ', segment, ' ', type_class, ' ', product, ' ', description, ' ', dynamic_tags_a, ' ',dynamic_tags_b, ' ', dynamic_tags_c, ' ', dynamic_tags_d), '\s+', ' ', 'g')) as complete_string FROM widgets_new where  not mark_delete order by widget_id")
        all_docs = cursor_test.fetchall()


        for x in range(len(all_docs)//1000 +1):
            print((x+1)*1000)
            batch = all_docs[x*1000:(x+1)*1000]
            for i in batch:
                new_tuple = i + (create_embeddings(i[3])[0],)
                client.query(f"INSERT INTO thurro_widget_new_vector (widget_id, title, description, combined_string, embedding) VALUES {new_tuple};")         

                #%%
                log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')