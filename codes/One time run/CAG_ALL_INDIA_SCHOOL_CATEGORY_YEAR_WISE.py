# -*- coding: utf-8 -*-
"""
Created on Tue May  4 10:36:09 2021

@author: abhis
"""
import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import json
import time
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import numpy as np
import adqvest_db
import JobLogNew as log
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
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ALL_INDIA_SCHOOL_DATA_ENROLMENT_YEAR_WISE'
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


        '''

        Get all meta data

        '''

        '''

        States and Years

        '''
        data = {"dependencyValue": "{\"year\":\"none\",\"state\":\"national\",\"dist\":\"none\",\"block\":\"none\"}",
                "mapId": "96",
                "paramName": "civilian",
                "paramValue": "",
                "reportType": "T",
                "schemaName": "national"}
        url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getTabularData'

        headers = {'Content-type':'application/json', 'Accept':'application/json, text/plain, */*',"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
        time.sleep(1)
        r = requests.post(url,json=data,headers=headers)
        print(r)


        demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
        jsons = []
        for result in demo_text:
            jsons.append(result)
#            print(result)

        years = jsons[0]['yearList']

        data = {"extensionCall": "GET_STATE", "condition": ""}

        url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getMasterData'
        time.sleep(1)
        r = requests.post(url,json=data,headers=headers)
        print(r)
        demo_text = extract_json_objects(r.text, decoder=JSONDecoder())

        jsons = []
        for result in demo_text:
            jsons.append(result)
#            print(result)

        states = jsons[0]['rowValue']




        for st in states:
            st_code = st['udise_state_code']
            st_name = st['state_name']

            '''

            District

            '''

            headers = {'Content-type':'application/json', 'Accept':'application/json, text/plain, */*',"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

            data = {"condition": "where udise_state_code= '"+st_code+"' and inityear ='2018-19' order by district_name",
                    "extensionCall": "GET_DISTRICT"}

            url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getMasterData'
            time.sleep(1)
            r = requests.post(url,json=data,headers=headers)
            print(r)

            demo_text = extract_json_objects(r.text, decoder=JSONDecoder())

            jsons = []
            for result in demo_text:
                jsons.append(result)
#                print(result)

            districts = jsons[0]['rowValue']#List



            for dt in districts:
                dt_code = dt['udise_district_code']
                dt_name = dt['district_name']

                '''

                Block

                '''

                headers = {'Content-type':'application/json', 'Accept':'application/json, text/plain, */*',"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

                data = {"condition": "where udise_dist_code= '"+dt_code+"' order by block_name",
                        "extensionCall": "GET_BLOCK"}
                url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getMasterData'
                time.sleep(2)
                r = requests.post(url,json=data,headers=headers)
    #            print(r)

                demo_text = extract_json_objects(r.text, decoder=JSONDecoder())

                jsons = []
                for result in demo_text:
                    jsons.append(result)
    #                print(result)

                blocks = jsons[0]['rowValue']#List


                years = jsons[0]['yearList']
                years = [y for y in years if y != '2018-19' ][-2:]

                for bl in blocks:
                    bl_code = bl['udise_block_code']
                    bl_name = bl['block_name']

                    for yrs in years:

                        data = {"dependencyValue": "{\"year\":\""+yrs+"\",\"state\":\""+st_code+"\",\"dist\":\""+dt_code+"\",\"block\":\""+bl_code+"\"}",
                        "mapId": "96",
                        "paramName": "civilian",
                        "paramValue": "",
                        "reportType": "T",
                        "schemaName": "national"}
                        url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getTabularData'

                        headers = {'Content-type':'application/json', 'Accept':'application/json, text/plain, */*',"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
                        time.sleep(2)
                        r = requests.post(url,json=data,headers=headers)
                        print(r)


                        demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
                        jsons = []
                        for result in demo_text:
                            jsons.append(result)
    #                        print(result)
                        try:
                            output = pd.DataFrame()
                            for vals in jsons[0]['rowValue']:
                                df = pd.DataFrame(vals,index=[0])
                                output = pd.concat([output,df])
                                df['Year'] = yrs
                                df['State'] = st_name
                                df['District'] = dt_name
                                df['Block'] =  bl_name
                                df['Relevant_Date'] = today.date()
                                df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                                print(df)
        #                        break
                                df.to_sql("ALL_INDIA_SCHOOL_DATA_ENROLMENT_YEAR_WISE_HISTORICAL", index=False, if_exists='append', con=engine)
                        except:
                                continue
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
