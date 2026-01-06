# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 16:40:17 2020

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
import PyPDF2
from pytz import timezone
import sys
import warnings
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
import codecs
st_1 = ["PASHCHIM CHAMPARAN","PURBA CHAMPARAN","SITAMARHI"]
st_2 = ["CHATRA","PALAMU","PASHCHIMI SINGHBHUM"]
st_3 = ["KORAPUT","MAYURBHANJ","NABARANGPUR"]
st_4 = ["ALLAHABAD","GONDA","PRATAPGARH"]    
main = st_1+st_2+st_2+st_3+st_4
url1 = "http://src.udiseplus.gov.in/locateSchool/state/6"
url2 = "http://src.udiseplus.gov.in/locateSchool/getDistrict"
url3 = "http://src.udiseplus.gov.in/locateSchool/getBlock?"

r = requests.get(url1)
res = r.text.strip('][').split(', ')
src = u"[%s]" % res[0]
states = json.loads(src)
states = [{k: states[k] for k in ('stateName','stateId', 'yearId')} for states in states]
reqd1 = []
for st in states:
    for key, value in st.items():
        if ((value == 'Bihar') or (value == 'Jharkhand') or (value == 'Odisha') or (value == 'Uttar Pradesh')):
            reqd1.append(st)    
dist_level = []
#for st in reqd1:
r = requests.post(url2,data=reqd1[0])
res = r.text.strip('][').split(', ')
src = u"[%s]" % res[0]
district = json.loads(src)
district = [{k: dist[k] for k in ('districtName','districtId', 'yearId')} for dist in district]

reqd2 = []
for dt in district:
    for key, value in dt.items():
        print(value)
        if ((value == main[0]) or (value == main[1]) or (value == main[2])):
            reqd2.append(dt)    
reqd3 = []
for values in reqd2:
    print(values['districtId'],values['yearId'])
    main_url = "http://src.udiseplus.gov.in/locateSchool/getBlock?districtId="+str(values['districtId'])+"&yearId="+str(values['yearId'])+""
    print(main_url)    
    r = requests.get(main_url)
    res = r.text.strip('][').split(', ')
    src = u"[%s]" % res[0]
    block = json.loads(src)
    block = [{k: blck[k] for k in ('eduBlockId', 'yearId')} for blck in block]    
    reqd3.append(block)

sample1 = reqd1[0]
sample2 = reqd2
sample3 = reqd3
for values in sample2:
    values.update(sample1)
    print(values)    
    
for values in sample3[0]:
    for y in sample2:
        values.update(y)
#do it for remaining places
final = []
for vals in sample3[0]:
    sample = {"year": vals["yearId"],
    "stateName": vals['stateId'],
    "districtName": vals['districtId'],
    "blockName": vals['eduBlockId'],
    "villageName":"" ,
    "clusterName": "",
    "categoryName": 0,
    "managementName": 0,
    "Search": "search"}
    final.append(sample)
    
data1 = []    
for vals in final:
    result_url = "http://src.udiseplus.gov.in/locateSchool/searchSchool"
    r = requests.post(result_url,data=vals)
    #r = requests.get(main_url)
    res = r.text.strip('][')
#    src = u"[%s]" % res[0]
    result = json.loads(res)
    result = [{k: rslt[k] for k in ('schoolId', 'schoolName','villageName','districtName','blockName', 'stateName')} for rslt in result['list']] 
    data1.append(result)
    

url = "http://src.udiseplus.gov.in/NewReportCard/PdfReportSchId"
header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}

school_name = []
village_name = []
district_name = []
block_name = []
state_name = []
elec1 = []
output = pd.DataFrame()
for vals in data1:
    print(vals)
    school_name = []
    village_name = []
    district_name = []
    block_name = []
    state_name = []
    elec1 = []
    elec2 = []
    total = []
    for value in vals:
        r = requests.post(url, headers=header, data={'schoolId':str(value['schoolId'])})
        print(r.status_code,value['districtName'],value[ 'blockName'])
        if r.status_code == 500 or r.status_code == 404 or r.status_code == 405 or r.status_code == 400:
            school_name.append(value['schoolName'])
            village_name.append(value['villageName'])
            district_name.append(value[ 'districtName'])
            block_name.append(value[ 'blockName'])
            state_name.append(value['stateName'])     
            avail1 = "NA"
            avail2 = "NA"
            elec1.append(avail1)
            elec2.append(avail2)
            total.append(0)
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
            school_name.append(value['schoolName'])
            village_name.append(value['villageName'])
            district_name.append(value['districtName'])
            block_name.append(value['blockName'])
            state_name.append(value['stateName']) 
            elec1.append(avail1)
            elec2.append(avail2)
            total1 = [s for s in nos if "Minority" in s][0].split("Minority")[0]
            total2 = [s for s in nos if "Source" in s][0].split("Source")[0]
            try:
                if int(total1[-3:])==int(total2[-3:]):
                  total.append(total1[-3:]) 
                elif int(total1[-4:])==int(total2[-4:]):
                  total.append(total1[-4:])  
                elif int(total1[-5:])==int(total2[-5:]):
                  total.append(total1[-5:])    
                else:
                    pass
            except:
                total.append(0)
        bihar = pd.DataFrame(list(zip(school_name, village_name,district_name,block_name,state_name,elec1,elec2,total)), 
               columns =['School_Name', 'Village_Name','District_Name','Block_Name','State_Name','Electricity_Availability_1','Electricity_Availability_2','Total_Students'])
    
    output = pd.concat([output,bihar])
output.to_csv("Sample Data1.csv",index=False)
output = output.drop(['Electricity_Availability_1'],axis=1)
output['Electricity_Availability_2'] = output['Electricity_Availability_2'].str.split("Availability").str[1].str[0]
output['Electricity_Availability_2'] = np.where(output['Electricity_Availability_2'].str.contains("1")|output['Electricity_Availability_2'].str.contains("Y"),"Yes","No")
output = output[['School_Name','State_Name', 'District_Name', 'Block_Name','Village_Name', 'Electricity_Availability_2','Total_Students']]
output.to_csv("Sample Data2.csv",index=False)