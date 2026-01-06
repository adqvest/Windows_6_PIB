# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 14:21:37 2022

@author: Abhishek Shankar
"""

import unicodedata
import sqlalchemy
from dateutil import parser
import pandas as pd
from pandas.io import sql
from json import JSONDecoder
import os
import requests
from bs4 import BeautifulSoup
import json
import re
from dateutil import parser
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
#sys.path.insert(0,r"C:\Adqvest")
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import adqvest_db
import JobLogNew as log
from dateutil import parser
import feedparser
import json
import pandas as pd
import time

def run_program(run_by='Adqvest_Bot', py_file_name=None):
#    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'RSS_FEEDS_DAILY_REFRESH_RATE'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        wd = "C:\\Adqvest\\RSS Feeds\\"
        wd = "C:\\Users\\Administrator\\AdQvestDir\\"
        # df = pd.read_excel(wd+"RSS_Links.xlsx")
        df = pd.read_sql("Select Source_Url from RSS_FEED_SOURCE_LINK_STATIC", engine)
        data1 = pd.DataFrame()
        all_cols = []
        sname = []
        limit = 0
        for url in df['Source_Url'].to_list():
            print(url)
            time.sleep(1)
            try:
                NewsFeed = feedparser.parse(url)
                entry = NewsFeed.entries[1]

            #    print(entry.keys())
            #    print(len(NewsFeed.entries))

                a = json.dumps(NewsFeed)
                data = json.loads(a)

                colnames = [list(x.keys()) for x in data['entries']]
                colnames = sum(colnames,[])
                colnames = list(set(colnames))
                all_cols.append(colnames)


            #    sname.append(url.split("https://")[-1].split("/")[0].split("www.")[-1])
            #    sname.append(source)
                info = data['entries']
                d1 = pd.DataFrame(info)
                #dataframe = pd.read_json(NewsFeed)
                source = data['feed']['title']
                source_url = data['href']
                try:
                  parse_date = data['updated']
                except:
                  parse_date = None
                try:
                  source_update = data['feed']['sy_updateperiod']
                except:
                  source_update = None#parser.parse(data['feed']['published'])
                try:
                  source_lupdate = parser.parse(data['feed']['updated'])
                except:
                  source_lupdate = None
                  #source_lupdate = parser.parse(data['feed']['updated'])
                d1['Title_Detail'] = d1['title_detail'].apply(lambda x : x['value'])
                d1['Published_Date'] = d1['published'].apply(lambda x : parser.parse(x))
                try:
                  d1['Tags'] = d1['tags'].apply(lambda x : [y['term'] for y in x])
                except:
                  d1['Tags'] = None

                d1['Summary'] = d1['summary']
                d1['Source'] = source
                d1['Source_Url'] = source_url
                d1['Source_Update_Period'] = source_update
                try:
                  d1['Source_Last_Updated'] = parser.parse(data['feed']['updated'])
                except:
                  try:
                    d1['Source_Last_Updated'] = parser.parse(data['feed']['published'])
                  except:
                    d1['Source_Last_Updated'] = None
                try:
                  d1 = d1[['Source', 'Source_Url','Source_Update_Period', 'Source_Last_Updated','title', 'Summary','link','author', 'Title_Detail',
                       'Published_Date', 'Tags']]
                except:
                  d1 = d1[['Source', 'Source_Url','Source_Update_Period', 'Source_Last_Updated','title', 'Summary','link', 'Title_Detail',
                       'Published_Date']]
                d1.columns = [x.title() for x in d1.columns]
                d1['Source_Link'] = url.split("https://")[-1].split("/")[0].split("www.")[-1]
                d1['Runtime']=datetime.datetime.now()
                d1['Relevant_Date']=today.date()
                data1 = pd.concat([data1,d1])
            except:
                limit+=1
                if limit > 2:
                    raise Exception("ERROR IN RSS FEED : "+url)
                continue
        #data1 = data1.apply(lambda col: col.astype(str) if col.dtype == 'datetime64[ns]' else col)
        def conv_time(x):
          try:
            return pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S')
          except:
            return None
        for vals in ['Source_Update_Period','Source_Last_Updated','Published_Date','Runtime']:
            data1[vals]   = data1[vals].apply(lambda x : conv_time(x))
#        data1 = data1.apply(lambda col: pd.to_datetime(col) if col.dtype == 'datetime64[ns]' else col)
        data1 = data1.dropna(axis=1)
        data1.to_sql(name='RSS_FEEDS_DAILY_REFRESH_RATE',con=engine,if_exists='append',index=False)
        print("UPLOADED")
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(exc_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
#cols = pd.DataFrame({"Source":sname,"Names":all_cols})
