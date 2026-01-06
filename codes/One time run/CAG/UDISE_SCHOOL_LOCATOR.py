# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 15:26:32 2021

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
os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run\CAG")
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

value = pd.read_sql("CAG_SCHOOLS_VILLAGE_LEVEL_DATA_UDISE_CODE",con=engine)
main = pd.DataFrame()
for i,row in value.iterrows():
    start_url = 'https://geoportal.nic.in/nicgis/rest/services/ESRINIC/schoolCode_AddressLocator/GeocodeServer/findAddressCandidates?SingleLine='+row['UDISE_Code']+'&f=json&outSR=%7B%22wkid%22%3A102100%7D&outFields=*&distance=50000&location=%7B%22x%22%3A9174069.140658302%2C%22y%22%3A3172937.4373815856%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&maxLocations=6'
    time.sleep(0.5)

    r = requests.get(start_url)

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

        print(df)

        df.to_sql(name= 'CAG_UDISE_VILLAGE_LOCATOR',con=engine,if_exists='append',index=False)

        print('Uploaded')
        main = pd.concat([main,df])
    except:
        pass

main.columns = [x.title() for x in list(main.columns)]
main['Relevant_Date'] = datetime.date(2020,3,31)
main['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))

main.to_csv("INPUT_COORDINATES_UDISE.csv",index=False)


    #%%
