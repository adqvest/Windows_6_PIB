# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 10:29:27 2020

@author: DELL
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
import numpy as np
import csv
import calendar
import pdb
import calendar
#import adqvest_db
import time
import json
#import JobLogNew as log


con_string = 'mysql+pymysql://abhishek:Abhi%shek3@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
#os.chdir(r'D:\Adqvest\Airports')
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
#%%
def date(x):
    return datetime.date(int(str(20)+x[-2:]),3,31)
#%%

state_json1 = {"requestdata":'{"query":{"stateid":"national","academic_year":"2018-19"},"filterarray":[{"script_type":"all"}],"MIS":true}',
       "requesttype":'noofschoolmanagementstatewisecollections_mis'}
url = 'http://dashboard.udiseplus.gov.in/api/admin/withoutauthenticate'
#headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",'Content-type': 'application/json;charset=UTF-8'}
headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
r = requests.post(url,data=state_json1,headers=headers)
print(r.status_code)
states = pd.DataFrame(json.loads(r.text)['messagedata']['result'])['state'].tolist()
dates = ['2018-19']
main=pd.DataFrame()
for dt in dates:
    for st in states:
        district_json = {"requestdata":'{"query":{"stateid":"'+st+'","academic_year":"'+dt+'"},"filterarray":[{"script_type":"all"}],"MIS":true}',
           "requesttype":'noofschoolmanagementstatewisecollections_mis'}
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
        r = requests.post(url,data=district_json,headers=headers)
        print(r.status_code)
        reqd_district= pd.DataFrame(json.loads(r.text)['messagedata']['result'])['district'].tolist()    
        for dist in reqd_district:
            main_json={"requestdata":'{"query":{"stateid":"'+st+'","district":"'+dist+'","academic_year":"'+dt+'"},"filterarray":[{"script_type":"all"}],"MIS":true}',
           "requesttype":'noofschoolmanagementstatewisecollections_mis'}
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
            r = requests.post(url,data=main_json,headers=headers)
            print(r.status_code)
            print(st+' , '+dist+' , '+dt)    
            reqd_data=pd.DataFrame(json.loads(r.text)['messagedata']['result'])
            reqd_data['District'] = dist
            main = pd.concat([main,reqd_data])
            main['Relevant_Date'] = date(dt)
#%%
try:
#    main['Relevant_Date'] = date(dt)
    cols = main.columns.tolist()
    cols = [x.title().replace(" ","_") for x in cols]        
    cols = ['Central_Tibetan_School',
 'Department_Of_Education',
 'District',
 'Government_Aided',
 'Jawahar_Navodaya_Vidyalaya',
 'Kendriya_Vidyalaya_Central_School',
 'Local_Body',
 'Madarsa_Recognized_By_Wakf_Board_Madarsa_Board',
 'Madarsa_Unrecognized',
 'Ministry_Of_Labor',
 'Other_Govt_Managed_Schools',
 'Private_Unaided_Recognized',
 'Railway_School',
 'Relevant_Date',
 'Sainik_School',
 'Social_Welfare_Department',
 'Total_Government',
 'Tribal_Welfare_Department',
 'Unrecognized',
 'Block',
 'State','Runtime']
    main.columns = cols
    main['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
    main = main[['State','District','Block','Central_Tibetan_School', 'Department_Of_Education', 'Government_Aided',
           'Jawahar_Navodaya_Vidyalaya', 'Kendriya_Vidyalaya_Central_School',
           'Local_Body', 'Madarsa_Recognized_By_Wakf_Board_Madarsa_Board',
           'Madarsa_Unrecognized', 'Ministry_Of_Labor',
           'Other_Govt_Managed_Schools', 'Private_Unaided_Recognized',
           'Railway_School', 'Sainik_School', 'Social_Welfare_Department',
           'Total_Government', 'Tribal_Welfare_Department', 'Unrecognized', 'Relevant_Date', 'Runtime']]
    
    main.to_sql(name='MHRD_SCHOOLS_BY_CATEGORY',con=engine,if_exists='append',index=False)  
except:
    print('INACCURATE DATA')
#%%
states = pd.read_sql('select Distinct(State) as state from AdqvestDB.MHRD_SCHOOLS_WITH_ELECTRICITY_CONNECTION',con=engine)['state'].tolist()
dates = ['2015-16','2016-17','2017-18']
main=pd.DataFrame()
for dt in dates:
    for st in states:
        district_json = {"requestdata":'{"query":{"stateid":"'+st+'","academic_year":"'+dt+'"},"filterarray":[{"script_type":"all:all"}],"MIS":true}',
           "requesttype":'noofschoolelectricityconcategorywiseandstatewise_mis'}
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
        r = requests.post(url,data=district_json,headers=headers)
        print(r.status_code)
        try:
            reqd_district= pd.DataFrame(json.loads(r.text)['messagedata']['result'])['district'].tolist()    
        except:
            pass
        for dist in reqd_district:
            main_json={"requestdata":'{"query":{"stateid":"'+st+'","district":"'+dist+'","academic_year":"'+dt+'"},"filterarray":[{"script_type":"all:all"}],"MIS":true}',
           "requesttype":'noofschoolelectricityconcategorywiseandstatewise_mis'}
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
            r = requests.post(url,data=main_json,headers=headers)
            print(r.status_code)
            try:
                print(st+' , '+dist+' , '+dt) 
                reqd_data=pd.DataFrame(json.loads(r.text)['messagedata']['result'])
                reqd_data['District'] = dist
                main = pd.concat([main,reqd_data])    
                main['Relevant_Date'] = date(dt)
            except:
                print('Inacurate Data')
                    
try:
    cols = main.columns.tolist()
    cols = [x.title().replace(" ","_") for x in cols]  
    cols = ['Higher_Secondary_Only_Jr__College',
     'Pr_Up_Pr_And_Secondary_Only',
     'Pr_With_Up_Pr_Sec_And_H_Sec',
     'Primary',
     'Primary_With_Upper_Primary',
     'Secondary_Only',
     'Secondary_With_Higher_Secondary',
     'Up_Pr_Secondary_And_Higher_Sec',
     'Upper_Pr_And_Secondary',
     'Upper_Primary_Only',
     'Block',
     'State',
     'District']
    main.columns = cols
    main['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
    main = main[['State', 'District','Block','Higher_Secondary_Only_Jr__College', 'Pr_Up_Pr_And_Secondary_Only',
           'Pr_With_Up_Pr_Sec_And_H_Sec', 'Primary', 'Primary_With_Upper_Primary',
           'Secondary_Only', 'Secondary_With_Higher_Secondary',
           'Up_Pr_Secondary_And_Higher_Sec', 'Upper_Pr_And_Secondary',
           'Upper_Primary_Only', 'Relevant_Date',
           'Runtime']]
    
#    main.to_sql(name='MHRD_SCHOOLS_WITH_ELECTRICITY_CONNECTION',con=engine,if_exists='append',index=False)  
except:
    print('INACCURATE DATA')
#%%
