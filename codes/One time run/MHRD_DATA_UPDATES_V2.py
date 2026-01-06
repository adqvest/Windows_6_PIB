# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 15:24:55 2021

@author: Abhishek Shankar
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
warnings.filterwarnings('ignore')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import csv
import calendar
import pdb
import calendar
import adqvest_db
import time
import json
import JobLogNew as log


#con_string = 'mysql+pymysql://abhishek:Abhi%shek3@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
#engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
#os.chdir(r'D:\Adqvest\Airports')
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
engine = adqvest_db.db_conn()


def date1(x):
    return datetime.date(int(str(20)+x[-2:]),3,31)
def atof(text):
    try:
        retval = float(text)
    except ValueError:
        retval = text
    return retval

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    float regex comes from https://stackoverflow.com/a/12643073/190597
    '''
    return [ atof(c) for c in re.split(r'[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)', text) ]

#%%

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
    table_name = 'SCHOOL_ENROLMENT_BY_MANAGEMENT_YEARLY_DATA'
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
        r = requests.Session()
        url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'
        data = {"extensionCall":"GET_STATE","condition":""}
        #data = '{extensionCall: "GET_STATE", condition: ""}'
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
                       'Content-type':'text/plain; charset=UTF-8',
                       'Accept':'application/json, text/plain, */*'}
        r1 = r.post(url,json=data,headers=headers)
        print(r1)
        states = json.loads(r1.text)

        timeline = states['yearList']
        states = states['rowValue']
        i = 0


        data1 = pd.DataFrame()
#        timeline  = [timeline[0]]
#        states = [states[9]]
        headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r1.cookies])

        for tp in timeline:
            r = requests.Session()
            for st in states:
                st_code = st['udise_state_code']
                time.sleep(10)
                '''

                DISTRICT

                '''
                data = {"extensionCall":"GET_DISTRICT","condition":"where udise_state_code= '"+str(st_code)+"' and ac_year ='"+str(tp)+"' order by district_name "}

                url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getMasterData'

                r2 = r.post(url,json=data,headers=headers)
                print(r2)

                districts = json.loads(r2.text)['rowValue']
#                districts = [districts[8]]
#                headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])

                for dt in districts:
                    dt_code = dt['udise_district_code']


                    '''

                    FINAL

                    '''
                    tl = tp
                    st_code = st['udise_state_code']
                    dt_code = dt['udise_district_code']
        #            bl_code = bl['udise_block_code']

                    time.sleep(5)
                    url = 'https://dashboard.udiseplus.gov.in/BackEnd-master/api/report/getTabularData'
                    #data = {"mapId":"81","dependencyValue":"{\"year\":\"2019-20\",\"state\":\"28\",\"dist\":\"2822\",\"block\":\"282262\"}","isDependency":"Y","paramName":"civilian","paramValue":"","schemaName":"national","reportType":"T"}
                    data1 = {"mapId":"81","dependencyValue":"{\"year\":\""+str(tp)+"\",\"state\":\""+str(st_code)+"\",\"dist\":\""+str(dt_code)+"\",\"block\":\""+str('none')+"\"}","isDependency":"Y","paramName":"civilian","paramValue":"","schemaName":"national","reportType":"T"}
                    #print(data)
                    try:
                        r3 = r.post(url,json=data1,headers=headers)
                    except:
                        time.sleep(6)
                        r3 = r.post(url,json=data1,headers=headers)
                    print(r3)
                    i = 0
                    try:
                        data = json.loads(r3.text)

                        cols = data['columnName']
                        core = data['rowValue']

                        cols.sort(key=natural_keys)
                        cols = [x for x in cols if "id" not in x.lower()]
                        df = pd.DataFrame(core)
                        df = df[cols]
                        df = df[[x for x in cols if x.lower() != 'cat9' ]]
            #                df = df[[x for x in cols if (('total' in x.lower()) | ('sch' in x.lower()))]]
                        #a = df.T
                        total = df[df[[x for x in cols if 'sch' in x.lower()]].iloc[:,0].str.lower().str.contains("total")].index[0]
            #                final = df.iloc[0:total]
                        final = df
                        check1 = sum(final['Total'])
                        check2 = sum(df.iloc[total,:-1])
                        if check1!= check2:
                            raise Exception("TOTALS NOT TALLYING")

                        final.columns = ['Total','PS (I-V)','UPS (I-VIII)','HSS (I-XII)'	,'UPS (VI-VIII)','HSS (VI-XII)','SS (I-X)','SS (VI-X)','SS (IX-X)','HSS (IX-XII)','HSS (XI-XII)','Type']
                        final['State'] = st['state_name']
                        final['District'] = dt['district_name']
            #            final['Block'] = bl['block_name']
                        final['Timeperiod'] = tp
                        final['Relevant_Date'] = final['Timeperiod'].apply(lambda x : date1(x))
                        final['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                        final = final.iloc[0:total]
                        headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r3.cookies])
            #            data1 = pd.concat([data1,final])
                        final.to_sql("SCHOOL_ENROLMENT_BY_MANAGEMENT_YEARLY_RAW_DATA_RERUN_Abhi", index=False, if_exists='append', con=engine)
                        if((total > 100000)|(total > 1000000)):
                            raise Exception("ERROR DATA CLEANED")
                    except:
                        i+=1
                        if i >10:
                            raise Exception("ERROR IN CLEANING DATA")
                        continue



        #            if 1>2:
        #              break
        #
        #            i += 1
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
