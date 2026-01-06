# -*- coding: utf-8 -*-
"""
Created on Mon May  3 14:06:49 2021

@author: abhis
"""

import json
import urllib.request
from haversine import haversine, Unit
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


con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')

headers = {
   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
   'Content-Type': 'application/json;charset=UTF-8'
   }

ipapi1 = pd.read_sql('Select * from AdqvestDB.CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPAPI',con=engine)
ipapi2 = pd.read_sql('Select * from AdqvestDB.CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPAPI_UDISE',con=engine)
ipstack1 = pd.read_sql('Select * from AdqvestDB.CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPSTACK',con=engine)
ipstack2 = pd.read_sql('Select * from AdqvestDB.CAG_PAN_INDIA_IP_ADDRESS_LOCATOR_IPSTACK_UDISE',con=engine)
#%%
df1 = ipapi1.copy()
df2 = ipapi2.copy()

d1 = ipstack1.copy()
d2 = ipstack2.copy()



'''

ipapi

'''


df1 = df1[['Ip_status', 'Ip_country', 'Ip_countryCode', 'Ip_region',
       'Ip_regionName', 'Ip_city', 'Ip_zip', 'Ip_lat', 'Ip_lon', 'Ip_timezone',
       'Ip_isp', 'Ip_org', 'Ip_as', 'Ip_query','State', 'City', 'District',
       'District_Code', 'Inst_Id', 'Inst_Latitude', 'Inst_Longitude',
       'Inst_Type', 'Institution_Code', 'Institution_Name', 'Ip_Address',
       'Location', 'Pincode', 'Website','Relevant_Date', 'Runtime']]

df2 = df2[['Ip_status', 'Ip_country', 'Ip_countryCode', 'Ip_region',
       'Ip_regionName', 'Ip_city', 'Ip_zip', 'Ip_lat', 'Ip_lon', 'Ip_timezone',
       'Ip_isp', 'Ip_org', 'Ip_as', 'Ip_query',
       'Institution Code',  'Location', 'Pincode', 'Latitude','Longitude',
       'Stname_1', 'Dtname_1','Vilname','Schname' ,'Ip_Address','Code_Type',
       'Relevant_Date', 'Runtime']]

df1.columns = ['Ip_Status', 'Ip_Country', 'Ip_Country_Code', 'Ip_Region',
       'Ip_Region', 'Ip_City', 'Ip_Zip', 'Ip_Latitude', 'Ip_Longitude', 'Ip_timezone',
       'Ip_Isp', 'Ip_Org', 'Ip_As', 'Ip_Query','State', 'City_Or_Village', 'District',
       'District_Code', 'Inst_Id', 'Inst_Latitude', 'Inst_Longitude',
       'Inst_Type', 'Institution_Code', 'Institution_Name', 'Ip_Address',
       'Location', 'Pincode', 'Website','Relevant_Date', 'Runtime']

df2.columns = ['Ip_Status', 'Ip_Country', 'Ip_Country_Code', 'Ip_Region',
       'Ip_Region', 'Ip_City', 'Ip_Zip', 'Ip_Latitude', 'Ip_Longitude', 'Ip_timezone',
       'Ip_Isp', 'Ip_Org', 'Ip_As', 'Ip_Query',
       'Institution_Code',  'Location', 'Pincode', 'Inst_Latitude', 'Inst_Longitude',
       'State', 'District','City_Or_Village','Institution_Name',
        'Ip_Address','Inst_Type',
       'Relevant_Date', 'Runtime']


df1 = df1[['Ip_Status', 'Ip_Country', 'Ip_Country_Code', 'Ip_Region',
       'Ip_Region', 'Ip_City', 'Ip_Zip', 'Ip_timezone',
       'Ip_Isp', 'Ip_Org', 'Ip_As', 'Ip_Query','Ip_Latitude', 'Ip_Longitude','Inst_Latitude', 'Inst_Longitude','City_Or_Village','State','District','Pincode','Inst_Type','Institution_Name','Institution_Code','Relevant_Date', 'Runtime']]

df2 = df2[['Ip_Status', 'Ip_Country', 'Ip_Country_Code', 'Ip_Region',
       'Ip_Region', 'Ip_City', 'Ip_Zip','Ip_timezone',
       'Ip_Isp', 'Ip_Org', 'Ip_As', 'Ip_Query','Ip_Latitude', 'Ip_Longitude','Inst_Latitude', 'Inst_Longitude','City_Or_Village','State','District','Pincode','Inst_Type','Institution_Name','Institution_Code','Relevant_Date', 'Runtime']]



output = pd.concat([df1,df2])


output[['Inst_Latitude','Inst_Longitude']] = output[['Inst_Latitude','Inst_Longitude']].apply(pd.to_numeric,errors='coerce')
output['Inst_Type'] = np.where(output['Inst_Type'].isnull(),'UDISE',output['Inst_Type'])

def get_distance(x1,y1,x2,y2):
    school = (x1,y1) # (lat, lon)
    ip = (x2, y2)

    dist = haversine(school, ip)

    return round(dist,2)


output['Haversine_Distance'] = output.apply(lambda x : get_distance(x['Inst_Latitude'],x['Inst_Longitude'],x['Ip_Latitude'],x['Ip_Longitude']),axis=1)
output_ipapi = output.copy()

#%%


df1 = ipstack1.copy()
df2 = ipstack2.copy()



'''

istack

'''


df1 = df1[['Ip_ip', 'Ip_type', 'Ip_continent_code', 'Ip_continent_name',
       'Ip_country_code', 'Ip_country_name', 'Ip_region_code',
       'Ip_region_name', 'Ip_city', 'Ip_zip', 'Ip_latitude', 'Ip_longitude',
       'Ip_location', 'Ip_time_zone', 'Ip_currency', 'Ip_connection','State', 'City', 'District',
       'District_Code', 'Inst_Id', 'Inst_Latitude', 'Inst_Longitude',
       'Inst_Type', 'Institution_Code', 'Institution_Name', 'Ip_Address',
       'Location', 'Pincode', 'Website','Relevant_Date', 'Runtime']]

df2 = df2[['Ip_ip', 'Ip_type', 'Ip_continent_code', 'Ip_continent_name',
       'Ip_country_code', 'Ip_country_name', 'Ip_region_code',
       'Ip_region_name', 'Ip_city', 'Ip_zip', 'Ip_latitude', 'Ip_longitude',
       'Ip_location', 'Ip_time_zone', 'Ip_currency', 'Ip_connection',
       'Institution Code',  'Location', 'Pincode', 'Latitude','Longitude',
       'Stname_1', 'Dtname_1','Vilname','Schname' ,'Ip_Address','Code_Type',
       'Relevant_Date', 'Runtime']]

df1.columns = ['Ip_Sent', 'Ip_Type', 'Ip_Continent_Code', 'Ip_Continent_Came',
       'Ip_Country_Code', 'Ip_Country_Name', 'Ip_Region_Code',
       'Ip_Region_name', 'Ip_City', 'Ip_Zip', 'Ip_Latitude', 'Ip_Longitude',
       'Ip_Location', 'Ip_Time_Zone', 'Ip_Currency', 'Ip_Connection','State', 'City_Or_Village', 'District',
       'District_Code', 'Inst_Id', 'Inst_Latitude', 'Inst_Longitude',
       'Inst_Type', 'Institution_Code', 'Institution_Name', 'Ip_Address',
       'Location', 'Pincode', 'Website','Relevant_Date', 'Runtime']

df2.columns = ['Ip_Sent', 'Ip_Type', 'Ip_Continent_Code', 'Ip_Continent_Came',
       'Ip_Country_Code', 'Ip_Country_Name', 'Ip_Region_Code',
       'Ip_Region_name', 'Ip_City', 'Ip_Zip', 'Ip_Latitude', 'Ip_Longitude',
       'Ip_Location', 'Ip_Time_Zone', 'Ip_Currency', 'Ip_Connection',
       'Institution_Code',  'Location', 'Pincode', 'Inst_Latitude', 'Inst_Longitude',
       'State', 'District','City_Or_Village','Institution_Name',
        'Ip_Address','Inst_Type',
       'Relevant_Date', 'Runtime']


df1 = df1[['Ip_Sent', 'Ip_Type', 'Ip_Continent_Code', 'Ip_Continent_Came',
       'Ip_Country_Code', 'Ip_Country_Name', 'Ip_Region_Code',
       'Ip_Region_name', 'Ip_City', 'Ip_Zip',
       'Ip_Location', 'Ip_Time_Zone', 'Ip_Currency', 'Ip_Connection','Ip_Latitude', 'Ip_Longitude','Inst_Latitude', 'Inst_Longitude','City_Or_Village','State','District','Pincode','Inst_Type','Institution_Name','Institution_Code','Relevant_Date', 'Runtime']]

df2 = df2[['Ip_Sent', 'Ip_Type', 'Ip_Continent_Code', 'Ip_Continent_Came',
       'Ip_Country_Code', 'Ip_Country_Name', 'Ip_Region_Code',
       'Ip_Region_name', 'Ip_City', 'Ip_Zip',
       'Ip_Location', 'Ip_Time_Zone', 'Ip_Currency', 'Ip_Connection','Ip_Latitude', 'Ip_Longitude','Inst_Latitude', 'Inst_Longitude','City_Or_Village','State','District','Pincode','Inst_Type','Institution_Name','Relevant_Date','Institution_Code', 'Runtime']]



output = pd.concat([df1,df2])


output[['Inst_Latitude','Inst_Longitude']] = output[['Inst_Latitude','Inst_Longitude']].apply(pd.to_numeric,errors='coerce')
output[['Ip_Latitude','Ip_Longitude']] = output[['Ip_Latitude','Ip_Longitude']].apply(pd.to_numeric,errors='coerce')
output['Inst_Type'] = np.where(output['Inst_Type'].isnull(),'UDISE',output['Inst_Type'])

def get_distance(x1,y1,x2,y2):
    school = (x1,y1) # (lat, lon)
    ip = (x2, y2)

    dist = haversine(school, ip)

    return round(dist,2)


output['Haversine_Distance'] = output.apply(lambda x : get_distance(x['Inst_Latitude'],x['Inst_Longitude'],x['Ip_Latitude'],x['Ip_Longitude']),axis=1)
output_ipstack = output.copy()

print(output_ipapi['Inst_Type'].value_counts())
print(output_ipstack['Inst_Type'].value_counts())

#%%
output_ipstack = output_ipstack[['City_Or_Village', 'State',
       'District', 'Pincode', 'Inst_Type', 'Institution_Name',
       'Institution_Code','Ip_Sent', 'Ip_Type', 'Ip_Continent_Code', 'Ip_Continent_Came',
       'Ip_Country_Code', 'Ip_Country_Name', 'Ip_Region_Code',
       'Ip_Region_name', 'Ip_City', 'Ip_Zip', 'Ip_Location', 'Ip_Time_Zone',
       'Ip_Currency', 'Ip_Connection', 'Ip_Latitude', 'Ip_Longitude',
       'Inst_Latitude', 'Inst_Longitude', 'Haversine_Distance',  'Relevant_Date', 'Runtime']]

output_ipapi = output_ipapi[['City_Or_Village', 'State',
       'District', 'Pincode', 'Inst_Type', 'Institution_Name',
       'Institution_Code','Ip_Status', 'Ip_Country', 'Ip_Country_Code', 'Ip_Region', 'Ip_City', 'Ip_Zip', 'Ip_timezone', 'Ip_Isp',
       'Ip_Org', 'Ip_As', 'Ip_Query', 'Ip_Latitude', 'Ip_Longitude',
       'Inst_Latitude', 'Inst_Longitude','Haversine_Distance', 'Relevant_Date', 'Runtime' ]]

output_ipstack = output_ipstack.loc[:,~output_ipstack.columns.duplicated()]
output_ipapi = output_ipapi.loc[:,~output_ipapi.columns.duplicated()]
output_ipstack.to_csv(r"C:\Users\Administrator\AdQvestDir\codes\One time run\PAN_INDIA_IPSTACK_ANALYSIS.csv",index=False)
output_ipapi.to_csv(r"C:\Users\Administrator\AdQvestDir\codes\One time run\PAN_INDIA_IPAPI_ANALYSIS.csv",index=False)
#%%
ip_stack = output_ipstack.copy()
ipapi = output_ipapi.copy()
data = pd.read_sql("Select * from AdqvestDB.CAG_PAN_INDIA_IP_ANALYSIS_CLEAN_DATA",con=engine)
value = data.copy()
value = value.drop_duplicates("Ip Address")
#%%
ip_data = value.copy()
all_ips = list(value["Ip Address"].unique())
check = pd.DataFrame()
for ips in all_ips:

#    d1 = ip2location[ip2location['Ip_Address']==ips]['Ip_State']
    d2 = ipapi[ipapi['Ip_Query']==ips]['Ip_City']
    d3 = ip_stack[ip_stack['Ip_Sent']==ips]['Ip_City']

    cond = ((d1.empty==False)&(d2.empty==False)&(d3.empty==False))

    if cond==True:
        df = {
#                'IP2Location': d1.unique()[0],
                'IPAPI.com':d2.unique()[0],
                'IPStack':d3.unique()[0],
                'IP_Address':ips
                }

        df = pd.DataFrame(df,index=[0])
        check = pd.concat([check,df])

#%%
matches = check.copy()

matches['Y/N'] = np.where((
                           (matches['IPAPI.com']==matches['IPStack'])),1,0)


overall = data.copy()

obt_ips = list(matches[matches['Y/N']==1]['IP_Address'].unique())

overall1 = overall[overall['Ip Address'].isin(obt_ips)]

print("######## IPs Analysed (Unique Add from given data = "+str(len(data['Ip Address'].unique()))+")########")
print("*After Matching UDISE Codes and IPs")
#print("IP2Location IPs :"+str(len(ip2location['Ip_Address'].unique())))
print("IPAPI IPs :"+str(len(ipapi['Ip_Query'].unique())))
print("IPStack :"+str(len(ip_stack['Ip_Sent'].unique())))

print("########All 3 Sources Have same States########")
print("Size of Matched DataFrame",len(matches))
print("1 = Yes & 0 = No")
print(matches['Y/N'].value_counts())



print("########Result From overall Data########")
print("Total Size of Given Data :",len(data))
print("Data with Where all 3 analysis matched :",len(overall1))

#%%

main_result = ip_stack[ip_stack['Ip_Sent'].isin(obt_ips)]#C:\Users\Administrator\AdQvestDir\codes\One time run

#main_result.to_csv(r"C:\Adqvest\CAG\All_India_IP\CAG_IP_ANALYSIS.csv",index=False)
#matches.to_csv(r"C:\Adqvest\CAG\All_India_IP\CAG_IP_ANALYSIS_Matches.csv",index=False)

main_result.to_csv(r"C:\Users\Administrator\AdQvestDir\codes\One time run\CAG_IP_ANALYSIS.csv",index=False)
matches.to_csv(r"C:\Users\Administrator\AdQvestDir\codes\One time run\CAG_IP_ANALYSIS_Matches.csv",index=False)
