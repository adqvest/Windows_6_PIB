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
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import adqvest_db
import JobLogNew as log
from json import JSONDecoder




def date1(x):
    return datetime.date(int(str(20)+x[-2:]),3,31)
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

def sub_replace(x):
    val = x
    try:
      if x.index("c")==0:

        return x.replace("c","Class_")
      else:
        return val
    except:
      return val
            #%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA'
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
#        i = 0
        r = requests.Session()
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
           'Content-type':'text/plain; charset=UTF-8',
           'Accept':'application/json, text/plain, */*'}

        '''

        Get all meta data

        '''

        '''

        States and Years

        '''
        data = {"extensionCall": "GET_STATE", "condition": ""}

        url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'
        time.sleep(1)
        r1 = r.post(url,json=data,headers=headers)
        print(r1)
        demo_text = extract_json_objects(r1.text, decoder=JSONDecoder())
        jsons = []
        for result in demo_text:
            jsons.append(result)
#            print(result)
        headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r1.cookies])

        lyears = jsons[0]['yearList']
        data = {"extensionCall": "GET_STATE", "condition": ""}
        time.sleep(3)
        url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'
        r2 = r.post(url,json=data,headers=headers)
        print(r2)
        demo_text = extract_json_objects(r2.text, decoder=JSONDecoder())

        jsons = []
        for result in demo_text:
            jsons.append(result)
#            print(result)

        states = jsons[0]['rowValue']

        for st in states:
            st_code = st['udise_state_code']
            st_name = st['state_name']
            output = pd.DataFrame()
            '''

            District

            '''

#            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
#               'Content-type':'text/plain; charset=UTF-8',
#               'Accept':'application/json, text/plain, */*'}
#            data = {"condition": "where udise_state_code= '"+st_code+"' and inityear ='2018-19' order by district_name",
#                    "extensionCall": "GET_DISTRICT"}
            data = {"extensionCall":"GET_DISTRICT","condition":"where udise_state_code= '"+str(st_code)+"' and ac_year = '"+lyears[0]+"' order by district_name "}

#            url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getMasterData'
            url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'
            time.sleep(10)
            r3 = r.post(url,json=data,headers=headers)
            print(r3)

            demo_text = extract_json_objects(r3.text, decoder=JSONDecoder())

            jsons = []
            for result in demo_text:
                jsons.append(result)
#                print(result)

            districts = jsons[0]['rowValue']#List
            r = requests.Session()
            for dt in districts:
                dt_code = dt['udise_district_code']
                dt_name = dt['district_name']
                years = jsons[0]['yearList']
    #            years = [y for y in years if y != '2018-19' ][-2:]

                for yrs in years:

#                    data = {"dependencyValue": "{\"year\":\""+yrs+"\",\"state\":\""+st_code+"\",\"dist\":\""+dt_code+"\",\"block\":\""+bl_code+"\"}",
#                    "mapId": "96",
#                    "paramName": "civilian",
#                    "paramValue": "",
#                    "reportType": "T",
#                    "schemaName": "national"}
                    data = {"mapId":"96","dependencyValue":"{\"year\":\""+str(yrs)+"\",\"state\":\""+str(st_code)+"\",\"dist\":\""+str(dt_code)+"\",\"block\":\""+str('none')+"\"}","isDependency":"Y","paramName":"civilian","paramValue":"","schemaName":"national","reportType":"T"}
                    data = {"dependencyValue":"{\"year\":\""+str(yrs)+"\",\"state\":\""+str(st_code)+"\",\"dist\":\""+str(dt_code)+"\",\"block\":\""+str('none')+"\"}",
                            "isDependency": "Y",
                            "mapId": "96",
                            "paramName": "civilian",
                            "paramValue": "",
                            "reportType": "T",
                            "schemaName": "national"}
#                    url = 'http://pgi.seshagun.gov.in/BackEnd-master/api/report/getTabularData'
#                  url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'
                    url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData'

#                    headers = {'Content-type':'application/json', 'Accept':'application/json, text/plain, */*',"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
                    time.sleep(10)
                    try:
                        r4 = r.post(url,json=data,headers=headers)
                    except:
                        time.sleep(10)
                        r4 = r.post(url,json=data,headers=headers)
                    print(r)
                    if r4.status_code!=200:
                       time.sleep(10)
                       r4 = r.post(url,json=data,headers=headers)
                    demo_text = extract_json_objects(r4.text, decoder=JSONDecoder())
                    jsons = []
                    for result in demo_text:
                        jsons.append(result)
#                        print(result)
                    k = 0
                    try:
                        output = pd.DataFrame()
                        for vals in jsons[0]['rowValue']:
                            df = pd.DataFrame(vals,index=[0])
                            df = df.T.reset_index()
                            type_name = df.iloc[df[df.iloc[:,0].str.lower().str.contains("item")].index[0]][0]
                            df = df[~((df.iloc[:,0].str.contains("total"))|(df.iloc[:,0].str.contains("location")) | (df.iloc[:,0].str.contains("item")))]
    #                        df.iloc[:,0] =  df.iloc[:,0].str.replace("c","Total_Class_")
    #                        df.iloc[:,0] =  df.iloc[:,0].str.replace("pp","Pre_Primary")
                            df.iloc[:,0] =  df.iloc[:,0].apply(lambda x : sub_replace(x))
                            df.iloc[:,0] =  df.iloc[:,0].str.replace("_b","_Boys")
                            df.iloc[:,0] =  df.iloc[:,0].str.replace("_g","_Girls")
                            df.iloc[:,0] =   df.iloc[:,0].apply(lambda x : x.title())
                            df = df.reset_index(drop=True)
                            df.columns = ['Variable','Value']
                            df['Value'] = pd.to_numeric(df['Value'],errors='ignore')
                            df['Category'] = type_name
                            output = pd.concat([output,df])
                        output.iloc[:,0] = output.iloc[:,0].str.replace("Pp","Pre_Primary")
    #                    output['Year'] = yrs
                        output['State'] = st_name
                        output['District'] = dt_name
                        #df['Relevant_Date'] = today.date()
                        output['Timeperiod'] = yrs
                        output['Relevant_Date'] = output['Timeperiod'].apply(lambda x : date1(x))
                        output['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    #                    print(output.head())
                        headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r4.cookies])

                        output.to_sql("SCHOOL_ENROLMENT_BY_CATEGORY_YEARLY_DATA_RERUN_Abhi", index=False, if_exists='append', con=engine)
                        print("UPLOADED -->",st_name," --> ",dt_name)
                    except:
                        k+=1
                        if k>10:
                            raise Exception("DATA LEAK while cleaning data")
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
