import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
import json
from bs4 import BeautifulSoup


from time import sleep
import random

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


import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_DATA where Brand = 'Tanishq'",engine)
        last_rel_date = last_rel_date["Max"][0]
        print(last_rel_date)
        if(today.date()-last_rel_date >= datetime.timedelta(7)):
            cities = []
            address_list = []
            pin_code_list = []
            state_list = []
            latitude_list = []
            longitude_list = []
            no_of_ping += 1
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}
            r = requests.get("https://www.tanishq.co.in/wps/proxy/https/wc-services.crown.in/wcs/resources/store//10151/storelocator/cities?supportedBrand=TQ&responseFormat=json",headers = headers,verify = False)
            city_list = json.loads(r.content)
            city_list = city_list['listOfCities']


            for i in range(len(city_list)):
                print(city_list[i])

                limit = 0
                while True:
                    try:
                        r = requests.get("https://www.tanishq.co.in/wps/proxy/https/wc-services.crown.in/wcs/resources/store//10151/storelocator/byCityOrZip?cityOrZipcode="+city_list[i]+"&isServiceCenter=false&siteLevelStoreSearch=false&supportedBrand=TQ",headers = headers,verify = False)
                        city_data = json.loads(r.content)
                        no_of_ping += 1
                        time.sleep(1.5)
                        break
                    except:
                        limit += 1
                        if(limit < 5):
                            continue
                        else:
                            break
                try:
                    city_data = city_data["PhysicalStore"]
                except:
                    continue
                time.sleep(1)
                for j in range(len(city_data)):
                    cities.append(city_list[i])
                    address_list.append(city_data[j]["addressLine"])
                    #pin_code_list.append(city_data[j]["postalCode"])
                    state_list.append(city_data[j]["stateOrProvinceName"])
                    latitude_list.append(city_data[j]["latitude"])
                    longitude_list.append(city_data[j]["longitude"])
            # tanishq_df = pd.DataFrame({"City":cities,"Address":address_list,"Pin_Code":pin_code_list,"State":state_list,"Latitude":latitude_list,"Longitude":longitude_list})
            tanishq_df = pd.DataFrame({"City":cities,"Address":address_list,"State":state_list,"Latitude":latitude_list,"Longitude":longitude_list})
            tanishq_df["Address"] = tanishq_df["Address"].apply(lambda x: x[0])
            tanishq_df["Ticker"] = ''
            tanishq_df["Rating"] = ''
            tanishq_df["Place_Id"] = ''
            tanishq_df["No_Of_Reviewers"] = ''
            tanishq_df["Modified_Address"] = tanishq_df["City"] + " " + tanishq_df["Address"]
            tanishq_df["Modified_Address"] = tanishq_df["Modified_Address"].apply(lambda x: x.replace("#",""))
            tanishq_df["Brand"] = "Tanishq"
            tanishq_df["Relevant_Date"] = today.date()
            tanishq_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            tanishq_df['Company'] = 'Titan Company'
            tanishq_df.rename(columns={'Pin_Code':'Pincode'}, inplace=True)
            tanishq_df = tanishq_df[['Company', 'Brand', 'Address', 'City', 'State',
       'Latitude', 'Longitude', 'Relevant_Date', 'Runtime']]
            tanishq_df.to_sql(name = "STORE_LOCATOR_DATA",if_exists="append",index = False,con = engine)
        else:
            print("Data already present")
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
