# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 14:02:40 2021

@author: abhis
"""
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import numpy as np
import re
from json import JSONDecoder

def extract_json_objects(text, decoder=JSONDecoder()):
    """Find JSON objects in text, and yield the decoded JSON data

    Does not attempt to look for JSON arrays, text, or other JSON types outside
    of a parent JSON object.

    """
    pos = 0
    while True:
        match = text.find('{', pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1


def count(url):
    headers = {
   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
           }
    r = requests.get(url , headers = headers , verify = False)

    print(r)

    demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
    jsons = []
    for result in demo_text:
        jsons.append(result)
    try:
        data1 = str(jsons[4]['pageDataV4']['browseMetadata']['totalProducts'])
    except:
        data1 = None
    try:
        catg = jsons[4]['pageDataV4']['browseMetadata']['productAnalyticsData']['category']
        catg = re.sub(r"(\w)([A-Z])", r"\1 \2", catg)
    except:
        try:
            catg = jsons[4]['pageDataV4']['browseMetadata']['breadcrumbsData'][0]['title']
        except:
            catg = None

    try:
        sub_catg1 = jsons[4]['pageDataV4']['browseMetadata']['productAnalyticsData']['subCategory']
        sub_catg1 = re.sub(r"(\w)([A-Z])", r"\1 \2", sub_catg1)
    except:
        try:
            sub_catg1 = jsons[4]['pageDataV4']['browseMetadata']['breadcrumbsData'][1]['title']
        except:
            sub_catg1 = None
    try:
        sub_catg2 = jsons[4]['pageDataV4']['browseMetadata']['productAnalyticsData']['superCategory']
        sub_catg2 = re.sub(r"(\w)([A-Z])", r"\1 \2", sub_catg2)
    except:
        try:
            sub_catg2 = jsons[4]['pageDataV4']['browseMetadata']['breadcrumbsData'][2]['title']
        except:
            sub_catg2 = None

    return [data1,catg,sub_catg1,sub_catg2]

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'FLIPKART_PRODUCTS_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        links = pd.read_sql("Select * from AdqvestDB.FLIPKART_SITE_LINKS",con=engine)

        data = pd.DataFrame()
        for i,row in links.iterrows():
            time.sleep(2)
            link = row['Links']
            try:
                data1 = count(link)
            except:
                continue
            df = pd.DataFrame({"Category":data1[1],"Sub_Category_1":data1[2],"Sub_Category_2":data1[3],"Total_Products":data1[0],"URL":link},index=[0])



            if ((df['Total_Products'][0] != None) and (df['Category'][0] != None)):

                df['Total_Products'] = pd.to_numeric(df['Total_Products'],errors='coerce')
                try:
                    df['Category'] = df['Category'].apply(lambda x : x.title())
                except:
            	    pass

                try:
                    df["Sub_Category_1"] = df["Sub_Category_1"].apply(lambda x : x.title())
                except:
                    pass
                try:
                    df["Sub_Category_2"] = df["Sub_Category_2"].apply(lambda x : x.title())
                except:
                    pass
                df.to_sql(name = "FLIPKART_TOTAL_PRODUCTS_DAILY_DATA",index = False,if_exists = 'append',con = engine)

                print("Uploaded")

            data = pd.concat([data,df])


        log.job_end_log(table_name,job_start_time, no_of_ping)

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
