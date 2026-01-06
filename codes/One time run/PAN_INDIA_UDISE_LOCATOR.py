# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 13:34:13 2021

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
    table_name = 'CAG_PAN_INDIA_UDISE_LOCATOR'
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
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}

        query = "Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type is NULL"

        data1 = pd.read_sql(query,con=engine)

        query = "Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type ='UDISE'"

        data2 = pd.read_sql(query,con=engine)

        query = "Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type !='UDISE'"

        data3 = pd.read_sql(query,con=engine)

        query = 'Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA'

        data4 = pd.read_sql(query,con=engine)


        data = data1.copy()
        data['Institution Code'] = data['Institution Code'].str.strip()
        data = data[((data['Institution Code'].str.len()>=10) & (data['Institution Code'].str.len()<=11))]
        data['Institution Code'] = np.where(data['Institution Code'].str.len()==10,"0"+data['Institution Code'],data['Institution Code'])
        #data = data.drop_duplicates('Institution Code')
        data['Code_Type'] = 'UDISE'
        #matching all UDISE CODES
        op = pd.concat([data,data2])


        data = op.drop_duplicates('Institution Code')

        nf = pd.DataFrame()
        main = pd.DataFrame()
        for i,row in data.iterrows():
            code = row['Institution Code']
            start_url = 'https://geoportal.nic.in/nicgis/rest/services/ESRINIC/schoolCode_AddressLocator/GeocodeServer/findAddressCandidates?SingleLine='+str(code)+'&f=json&outSR=%7B%22wkid%22%3A102100%7D&outFields=*&distance=50000&location=%7B%22x%22%3A9174069.140658302%2C%22y%22%3A3172937.4373815856%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&maxLocations=6'
            time.sleep(2)
            ip = row['Ip Address']
            try:
                r = requests.get(start_url,headers=header,verify=False)
            except:
                time.sleep(8)
                try:
                    r = requests.get(start_url,headers=header,verify=False)
                except:
                    continue
            try:
                data1 = json.loads(r.content)

                xmin = data1[ 'candidates'][0]['extent']['xmin']
                xmax = data1[ 'candidates'][0]['extent']['xmax']
                ymin = data1[ 'candidates'][0]['extent']['ymin']
                ymax = data1[ 'candidates'][0]['extent']['ymax']

                result_url = 'https://geoportal.nic.in/nicgis/rest/services/SCHOOLGIS/Schooldata/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A'+str(xmin)+'%2C%22ymin%22%3A'+str(ymin)+'%2C%22xmax%22%3A'+str(xmax)+'%2C%22ymax%22%3A'+str(ymax)+'%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=102100&quantizationParameters=%7B%22mode%22%3A%22view%22%2C%22originPosition%22%3A%22upperLeft%22%2C%22tolerance%22%3A1.194328566854024%2C%22extent%22%3A%7B%22xmin%22%3A68.40400999983837%2C%22ymin%22%3A6.893214000366479%2C%22xmax%22%3A97.02722199976724%2C%22ymax%22%3A35.032117999820365%2C%22spatialReference%22%3A%7B%22wkid%22%3A4326%2C%22latestWkid%22%3A4326%7D%7D%7D'

                r = requests.get(result_url)

                time.sleep(0.5)

                data1 = json.loads(r.content)

                df = pd.DataFrame(data1['features'][0]['attributes'],index=[0])

                print(df['latitude'][0],df['longitude'][0],df['vilname'][0], df['schcd'][0])
                df['Relevant_Date'] = datetime.date(2021,5,3)
                df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df['Ip Address'] = ip
                df.columns = [x.title() for x in list(df.columns)]
                df.to_sql(name="CAG_PAN_INDIA_UDISE_SCHOOL_LOCATOR_3",con=engine,if_exists='append',index=False)
                print("Uploaded")
                main = pd.concat([main,df])
            except:
                df = row#{"UDISE_Code":row['UDISE_Code'],"School_Name":row['School_Name'],"Village_Name":row['Village_Name'],"Block_Name":row['Block_Name'],"District_Name":row['District_Name'],"State_Name":row['State_Name']}
                df = pd.DataFrame(df).T
                nf = pd.concat([nf,df])
                print("NOT FOUND",len(nf))
                continue
        nf.to_csv("CAG_PAN_INDIA_IP_ANALYSIS_NF.csv",index=False)

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
