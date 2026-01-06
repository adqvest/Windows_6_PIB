# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 12:32:28 2021

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
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')

headers = {
   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
   'Content-Type': 'application/json;charset=UTF-8'
   }


value = pd.read_sql("UDISE_SCHOOL_QUERY_4_STATES",con=engine)#Select * from AdqvestDB.CAG_VILLAGE_SHORTLISTED
# stuff = pd.read_sql("CAG_VILLAGE_SHORTLISTED",con=engine)
#
# value = value[value['villageName'].str.title().isin(stuff['Village'].to_list())]


school_name = []
village_name = []
district_name = []
block_name = []
state_name = []
elec1 = []
elec2 = []
total = []

url = "https://src.udiseplus.gov.in/NewReportCard/PdfReportSchId"
header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}

for i,value in value.iterrows():
    time.sleep(1)
    url = "https://src.udiseplus.gov.in/NewReportCard/PdfReportSchId"
    header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}

    school_name = []
    village_name = []
    district_name = []
    block_name = []
    state_name = []
    elec1 = []

    print(value)
    try:
        r = requests.post(url, headers=header, data={'schoolId':str(value['schoolId'])})
    except:
        time.sleep(4)
        r = requests.post(url, headers=header, data={'schoolId':str(value['schoolId'])})
        print(r.status_code,value['districtName'],value[ 'blockName'])
    try:
        if r.status_code == 500 or r.status_code == 404 or r.status_code == 405 or r.status_code == 400:

            print('#############STATUS CODE ERROR###########')

            school_name = value['schoolName']
            village_name = value['villageName']
            district_name = value[ 'districtName']
            block_name = value[ 'blockName']
            state_name = value['stateName']
            avail1 = "NA"
            avail2 = "NA"
            elec1.append = avail1
            elec2.append = avail2
            total = 0
        else:
            f = io.BytesIO(codecs.decode(r.content, "base64"))
            reader = PyPDF2.PdfFileReader(f)
            contents1 = reader.getPage(0).extractText().split('\n')
            contents2 = reader.getPage(1).extractText().split('\n')
            elec = contents1[0].split()
            nos = contents2[0].split()
            matching = [s for s in elec if "Electricity" in s]
            indices = elec.index(matching[0])
            avail1 = matching
            avail2 = elec[indices+1]
            school_name = value['schoolName']
            village_name = value['villageName']
            udise_code = contents1[0].split("UDISE Code")[1].split("School")[0]
            udise_code = udise_code.replace("  ","")
            district_name = value['districtName']
            block_name = value['blockName']
            state_name = value['stateName']
            elec1 = avail1
            elec2 = avail2
            total1 = [s for s in nos if "Minority" in s][0].split("Minority")[0]
            total2 = [s for s in nos if "Source" in s][0].split("Source")[0]
            elec = contents1[0].split()
            try:
                avail = re.findall(r'[1-9]{1}[0-9]{5}|[1-9]{1}[0-9]{3}\\s[0-9]{3}',[s for s in elec if "Pincode" in s][0])[0]
            except:
                avail = 0

            avail = int(avail)
            print(avail)

            try:
                if int(total1[-3:])==int(total2[-3:]):
                  total = total1[-3:]
                elif int(total1[-4:])==int(total2[-4:]):
                  total = total1[-4:]
                elif int(total1[-5:])==int(total2[-5:]):
                  total =  total1[-5:]
                else:
                    total = 0
            except:
                total.append(0)
    #                output= pd.DataFrame(list(school_name, village_name,district_name,block_name,state_name,elec2,total),
    #                       columns =['School_Name', 'Village_Name','District_Name','Block_Name','State_Name','Electricity_Availability','Total_Students'])
        output = pd.DataFrame({'School_Name':school_name, 'Village_Name':village_name,'District_Name':district_name,'Block_Name':block_name,'State_Name':state_name,'Pincode':avail,'Electricity_Availability':elec2,'Total_Students':total,"UDISE_Code":udise_code}, index=[0])
    #        output = pd.concat([output,df])
        output['Relevant_Date'] = datetime.date(2020,3,31)
        output['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        print(output.head())

        output['Electricity_Availability'] = output['Electricity_Availability'].str.split("Availability").str[1].str[0]
        output['Electricity_Availability'] = np.where(output['Electricity_Availability'].str.contains("1")|output['Electricity_Availability'].str.contains("Y"),"Yes","No")
        output = output[['School_Name',"UDISE_Code",'State_Name', 'District_Name', 'Block_Name','Village_Name','Pincode', 'Electricity_Availability','Total_Students',"Relevant_Date","Runtime"]]
        print('UPLOADED')
        output.to_sql(name='CAG_SCHOOLS_VILLAGE_LEVEL_DATA_UDISE_CODE',con=engine,if_exists='append',index=False)
        del output

    except:
        pass
