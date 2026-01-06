import pandas as pd
import requests
#import sqlalchemy
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
from dateutil import parser
import re
import datetime as datetime
from datetime import date
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import csv
import calendar
import pdb
import calendar
import time
import os
import json
from dateutil import parser
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')

import numpy as np
import time
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
start_date = datetime.date(2022,11,12)
end_date = datetime.date(2022,11,14)
#%%
import adqvest_db
import JobLogNew as log
#%%
def ext_datetime(x):
    try:
        timestamp = datetime.datetime.fromtimestamp(x[0])
        return[timestamp.time(),timestamp.date()]
    except:
        return[np.nan,np.nan]
#%%

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days
    start_date = datetime.date(2022,6,26)
    end_date = datetime.date(2022,11,14)

    job_start_time = datetime.datetime.now(india_time)
    table_name = "TNPCB_DAILY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
  #%%
        while start_date <= end_date:
            '''page1:category_list'''
            headers = {
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                }
            url1 = 'http://117.232.97.121/TNPCBONLINEREAL/categoryList'
            r = requests.post(url1,data=json.dumps({}),headers=headers)

            categoryList = json.loads(r.text)['bodyContent']

            '''page2:category_details'''
            headers = {
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                }
            url2 = 'http://117.232.97.121/TNPCBONLINEREAL/CategoryDetails'
            allData = []

            table = pd.DataFrame()

            for i in range(len(categoryList)):
                time.sleep(2)
                r = requests.post(url2,data=json.dumps(categoryList[i]),headers=headers)
                #print(r.text)
                categoryDetails = json.loads(r.text)['bodyContent']
                #print(categoryDetails)
                for j in range(len(categoryDetails)):
                    df = pd.DataFrame(categoryDetails[j])
                    out = pd.concat([table,df])
                    #print(df)
                    codes = list(out['siteId'].unique())
                    #print(codes)
                    for x in codes:
                        params = list(out[out['siteId'] == x]['sourceParameters'])
                        postData = {"userType":"","userId":"","siteId":x,"parameters":params,"criteria":"15min","date":"","toDate":str(start_date),"reportFormat":"graph","fromDate":str(start_date)}
                        allData.append(postData)
                        #print(allData)

            for i in range(len(allData)):
                time.sleep(3)
                r = requests.post('http://117.232.97.121/TNPCBONLINEREAL/industry-graph',data=json.dumps(allData[i]),headers=headers)
                #print(r.text)
                maindata = json.loads(r.text)
                info1 = maindata['graphDetails']
                #print(info1)
                companyname,location,category = maindata['info'][0]['value'],maindata['info'][2]['value'],maindata['info'][3]['value']
                for l in range(len(info1)):
                    try:
                        df1 = pd.DataFrame(info1[l])
                    except:
                        df1 = pd.DataFrame(info1[l],index=[0])
                    df1['Category'] = category
                    df1['Company_Name'] = companyname
                    df1['Location'] = location
                    df1['Parameter'] = df1['parameter']
                    df1['Threshold_Value'] = df1['threshold']
                    df1['Unit'] = df1['unit']
                    try:
                        df1['Actual_Value'] = df1['invalidData'].str[1]
                        df1['Time'] = df1['invalidData'].apply(lambda x : ext_datetime(x)[0])
                        df1['Time'] = df1['Time'].apply(lambda x: x.strftime("%H:00:00"))
                        df1['Time'] = df1['Time'].drop_duplicates()
                        df1 = df1[df1['Time'].notna()]
                        df1['Relevant_Date'] = df1['invalidData'].apply(lambda x : ext_datetime(x)[1])
                    except:
                        pass

                    try:
                        df1.drop(['invalidData','threshold','parameter','unit'],axis = 1,inplace = True)
                    except:
                        df1.drop(['threshold','parameter','unit'],axis = 1,inplace = True)
                    df1.reset_index()
                    print(df1)
                    df1['Threshold_Value'] = pd.to_numeric(df1['Threshold_Value'] , errors = 'coerce')
                    df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                output = pd.DataFrame()
                output = pd.concat([output,df1])
            out = output
            drop_null = out.iloc[:,9][out.iloc[:,9].isnull()].index
            out = out.drop(drop_null)
            engine = adqvest_db.db_conn()
            out.to_sql(name = "TNPCB_DAILY_DATA",con = engine,if_exists = 'append',index = False)
            start_date = start_date + days
            #%%
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
    run_program(run_by = 'manual')
