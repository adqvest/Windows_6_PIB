# -*- coding: utf-8 -*-
"""
Created on Fri Apr 30 17:45:03 2021

@author: abhis
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 14:59:09 2021

@author: abhis
"""

import pandas as pd
import requests
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import urllib
import datetime as datetime
import requests
import io
import numpy as np
import PyPDF2
from pytz import timezone
import sys
import warnings
import codecs
warnings.filterwarnings('ignore')
import numpy as np
import csv
import calendar
import pdb
import json
import calendar
import time
import os
from dateutil import parser
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db

import json
import urllib

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days



def get_geodata(x):

    key = "3203fa91954c8beaeab684773a340a02"
    ip = x
    url = "http://api.ipstack.com/" + ip +"?access_key=" + key
    response = requests.get(url).json()
    #print(response)
    df = pd.DataFrame(response,index=[0])
    return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPSTACK'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        headers = {
           "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
           'Content-Type': 'application/json;charset=UTF-8'
           }

        #os.chdir(r"C:\Adqvest\CAG")
        query = "Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA"
        all_ip = pd.read_sql(query,con=engine)
        value = all_ip.copy()
        #value = value.drop_duplicates("Ip_Address")
        nf = pd.DataFrame()
        #value = value.drop_duplicates("Ip_Address")

        import urllib
        for i,row in value.iterrows():
            print(i)
            try:
                ip = str(row['Ip Address'])
                print(i)
                df = get_geodata(ip)
                cols = df.columns
                cols = ["Ip_"+x for x in cols]
                df.columns = cols
                d = pd.DataFrame(row).T.reset_index()
                a = pd.concat([df,d],axis=1)
                a.to_sql(name="CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPSTACK_2",con=engine,if_exists='append',index=False)

            except:
                print(str(i)+"  NA")
                d1 = pd.DataFrame(row).T.reset_index()
                nf = pd.concat([nf,d1])
                time.sleep(3)
                continue

        nf.to_csv("Not FOUND IPStack ANnalysis.csv",index=False)
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
#
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
