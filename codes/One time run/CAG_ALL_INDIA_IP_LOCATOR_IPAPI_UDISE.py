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
    import urllib
    GEO_IP_API_URL  = 'http://ip-api.com/json/'

    # Can be also site URL like this : 'google.com'
    IP_TO_SEARCH    = x

    # Creating request object to GeoLocation API
    req             = urllib.request.Request(GEO_IP_API_URL+IP_TO_SEARCH)
    # Getting in response JSON
    response        = urllib.request.urlopen(req).read()
    # Loading JSON from text to object
    json_response   = json.loads(response.decode('utf-8'))

    df = pd.DataFrame(json_response,index=[0])

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
    table_name = 'CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPAPI_UDISE'
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
       query = "Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type is NULL"

        #os.chdir(r"C:\Adqvest\CAG")
        udise = pd.read_sql("Select * from AdqvestDB.CAG_PAN_INDIA_UDISE_SCHOOL_LOCATOR_2",con=engine)

        value = pd.read_sql("Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type = 'UDISE';",con=engine)

        d1 = udise.copy()
        d2 = value.copy()

        d1.columns = ['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat', 'Schtype',
               'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location', 'Pincode',
               'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude', 'Latitude',
               'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1', 'Sdtname',
               'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd', 'Ud_St_N',
               'Ud_Dt_N', 'Lgd_Dt_Ud', 'Relevant_Date', 'Runtime', 'Ip Address']

        d1['Institution Code'] = d1['Institution Code'].str.strip()
        d2['Institution Code'] = d2['Institution Code'].str.strip()

        d1['Institution Code'] = np.where(d1['Institution Code'].str.len()==10,"0"+d1['Institution Code'],d1['Institution Code'])
        #d1['Institution Code'] = d1['Institution Code'].astype(float)
        #d2['Institution Code'] = d2['Institution Code'].astype(float)

        output = d1.merge(d2 , on = ['Institution Code'])

        #op = output[output['Ip Address_x']==output['Ip Address_y']]
        #output = output.drop_duplicates(['Ip Address_x','Ip Address_y'])


        output = output[['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat',
               'Schtype', 'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location',
               'Pincode', 'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude',
               'Latitude', 'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1',
               'Sdtname', 'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd',
               'Ud_St_N', 'Ud_Dt_N', 'Lgd_Dt_Ud','Ip Address_y', 'Raw_Code', 'Code_Type',
               'Relevant_Date_y', 'Runtime_y']]
        output.columns = ['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat',
               'Schtype', 'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location',
               'Pincode', 'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude',
               'Latitude', 'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1',
               'Sdtname', 'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd',
               'Ud_St_N', 'Ud_Dt_N', 'Lgd_Dt_Ud','Ip_Address', 'Raw_Code', 'Code_Type',
               'Relevant_Date', 'Runtime']


        value = output.copy()
        #value = value.drop_duplicates("Ip_Address")

        udise = pd.read_sql("Select * from AdqvestDB.CAG_PAN_INDIA_UDISE_SCHOOL_LOCATOR_2 where Relevant_Date = '2021-05-03'",con=engine)

        value = pd.read_sql("Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA where Code_Type is NULL",con=engine)

        d1 = udise.copy()
        d2 = value.copy()

        d2 = pd.read_sql(query,con=engine)

        d2['Institution Code'] = d2['Institution Code'].str.strip()
        d2 = d2[((d2['Institution Code'].str.len()>=10) & (d2['Institution Code'].str.len()<=11))]
        d2['Institution Code'] = np.where(d2['Institution Code'].str.len()==10,"0"+d2['Institution Code'],d2['Institution Code'])


        d1.columns = ['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat', 'Schtype',
               'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location', 'Pincode',
               'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude', 'Latitude',
               'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1', 'Sdtname',
               'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd', 'Ud_St_N',
               'Ud_Dt_N', 'Lgd_Dt_Ud', 'Relevant_Date', 'Runtime', 'Ip Address']

        d1['Institution Code'] = d1['Institution Code'].str.strip()
        d2['Institution Code'] = d2['Institution Code'].str.strip()

        d1['Institution Code'] = np.where(d1['Institution Code'].str.len()==10,"0"+d1['Institution Code'],d1['Institution Code'])
        #d1['Institution Code'] = d1['Institution Code'].astype(float)
        #d2['Institution Code'] = d2['Institution Code'].astype(float)

        output = d1.merge(d2 , on = ['Institution Code'])

        output = output[['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat',
               'Schtype', 'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location',
               'Pincode', 'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude',
               'Latitude', 'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1',
               'Sdtname', 'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd',
               'Ud_St_N', 'Ud_Dt_N', 'Lgd_Dt_Ud','Ip Address_y', 'Raw_Code', 'Code_Type',
               'Relevant_Date_y', 'Runtime_y']]
        output.columns = ['Objectid', 'Institution Code', 'Schname', 'Schcat', 'School_Cat',
               'Schtype', 'School_Typ', 'Schmgt', 'Management', 'Rururb', 'Location',
               'Pincode', 'Dtname', 'Udise_Stco', 'Stname', 'Vilname', 'Longitude',
               'Latitude', 'Stcode11', 'Dtcode11', 'Sdtcode11', 'Stname_1', 'Dtname_1',
               'Sdtname', 'Udise_Dtco', 'Udise_Vico', 'Dist_Lgd', 'State_Lgd',
               'Ud_St_N', 'Ud_Dt_N', 'Lgd_Dt_Ud','Ip_Address', 'Raw_Code', 'Code_Type',
               'Relevant_Date', 'Runtime']


        value = output.copy()

        import urllib
        for i,row in value.iterrows():

            try:
                ip = str(row['Ip_Address'])
                print(i)
                time.sleep(1)
                df = get_geodata(ip)
                cols = df.columns
                cols = ["Ip_"+x for x in cols]
                df.columns = cols
                d = pd.DataFrame(row).T.reset_index()
                a = pd.concat([df,d],axis=1)
                a.to_sql(name="CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPAPI_UDISE",con=engine,if_exists='append',index=False)

            except:

                print(str(i)+"  NA")
                time.sleep(3)
                continue


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
