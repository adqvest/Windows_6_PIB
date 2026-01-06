# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 18:03:47 2020

@author: abhis
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
#os.chdir(r"C:\Adqvest\Ministry of HRD")
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
import numpy as np
import csv
import calendar
import pdb
import calendar
#import adqvest_db
import time
import json

con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'}
#df = pd.DataFrame()
head = 'https://www.npci.org.in'
links_df = pd.read_sql("CAG_SCHOOLS_VILLAGE_LEVEL_ATM_DATA",con=engine)
for _,rows in links_df.iterrows():
        df = pd.DataFrame()
        print(rows["Final_link"])
        url = rows["Final_link"]
        time.sleep(1)
        r = requests.get(url,headers=headers,verify=False)
        print(r.status_code)
#        links1.append(r.status_code)
        a = pd.read_html(r.content)[1]
        if a.iloc[:,0].str.contains("Note")[0]==True:
            print("No Data for Avail")
        else:
            a['District'] = rows['districtName']
            a['State'] = rows['stateName']
            df = pd.concat([df,a])

            soup = BeautifulSoup(r.content,"html")
            pages = soup.find_all(class_="pager-item")
            sub_pages = []
            for li in pages:
                sub_pages.append(li.a.get('href'))
            if sub_pages != []:
                print("Presence of Additional Pages")
                try:
                    sub_pages = [head+x for x in sub_pages]
                    sub_links = []
                    for linkz in sub_pages:
                        sub_links.append(linkz)
                        time.sleep(1)
                        r = requests.get(linkz,headers=headers,verify=False)
                        print(r.status_code,linkz)
                        b = pd.read_html(r.content)[1]
                        b['District'] = rows['districtName']
                        b['State'] = rows['stateName']
                        df = pd.concat([df,a])
                except:
                     pass
    #df.columns
            cols = ['Bank', 'Address', 'Landmark', 'Pin_Code',
                   'City', 'Metro_Or_Non_Metro', 'District','State']
            df.columns = cols
            india_time = timezone('Asia/Kolkata')
            today = datetime.datetime.now(india_time)
            yesterday = datetime.datetime.now(india_time)
            #df['District'] = df['District'].str.replace("%20"," ")
            #states = ["Bihar","Jharkhand","Odisha","Uttar Pradesh"]
            #df['State'] = np.where(df['District'].str.contains(main[0][0])|df['District'].str.contains(main[0][1])|df['District'].str.contains(main[0][2]),states[0],np.where(df['District'].str.contains(main[1][0])|df['District'].str.contains(main[1][1])|df['District'].str.contains(main[1][2]),states[1],np.where(df['District'].str.contains(main[2][0])|df['District'].str.contains(main[2][1])|df['District'].str.contains(main[2][2]),states[2],np.where(df['District'].str.contains(main[3][0])|df['District'].str.contains(main[3][1])|df['District'].str.contains(main[3][2]),states[3],0))))

            df['Relevant_Date'] = today.date()
            df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            df['Last_Modified'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

            print(df)

            df.to_sql(name='CAG_DISTRICT_LEVEL_ATM_DATA_NETC',con=engine,if_exists='append',index=False)
