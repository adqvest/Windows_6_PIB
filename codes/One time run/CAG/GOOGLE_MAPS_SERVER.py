# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 17:26:16 2020

@author: abhis
"""

import sys
import re
import os
from io import StringIO
import sqlalchemy
from dateutil import parser
import io
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
from pytz import timezone
import time
import json
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
from datetime import datetime, timedelta
import codecs
warnings.filterwarnings('ignore')
import numpy as np
from io import BytesIO
import gzip
import csv
import calendar
import pdb
import json
import calendar
import time
import os
import pgeocode
from dateutil import parser

import googlemaps
from datetime import datetime
key = 'AIzaSyAj-NV0-ICVMIVh515v2JOyWHqmdk3c-CQ'
gmaps = googlemaps.Client(key=key)

os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run\CAG")

con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')


def get_matched(df):
    df[['Google_Village1']] = df['address'].str.split(',',expand=True)[0]
    try:
        df[['Village','District','State']] = df['Place'].str.split(',',expand=True)
        df['District'] = df['District'].apply(lambda x : x.title())
    except:
        try:
            df[['Village','State']] = df['Place'].str.split(',',expand=True)
#            df['S'] = df['District'].apply(lambda x : x.title())
        except:
            try:
                df[['Village','Block','District','State']] = df['Place'].str.split(',',expand=True)
            except:
                df[['Village','State','Pincode',]] = df['Place'].str.split(',',expand=True)
    try:
        df['Village'] = df['Village'].apply(lambda x : x.title())
    except:
        pass
    df['State'] = df['State'].apply(lambda x : x.title())
    #df['Village_Check'] = np.where (df['Google_Village1'].str.contains(df['Village']),0,1)

    from fuzzywuzzy import fuzz
    df['partial_ratio'] = df.apply(lambda x: fuzz.partial_ratio(x['Google_Village1'], x['Village']), axis=1)
    df['Village_Check2'] = np.where (df['partial_ratio']>60,1,0)

    print(df['Village_Check2'] .value_counts())

    check = df[df['Village_Check2']==1]
    check2 = df[df['Village_Check2']==0]


    return [check,check2,df]

def geocode(s):
    GOOGLE_MAPS_API_URL = 'https://maps.googleapis.com/maps/api/geocode/json'

    add = s
    params = {
        'address': add.title(),
        'sensor': 'false',
        'region': 'india',
        'key' : key
    }
    print(params)

    # Do the request and get the response data
    req = requests.get(GOOGLE_MAPS_API_URL, params=params)
    res = req.json()
    try:
        # Use the first result
        result = res['results'][0]

        geodata = dict()
        geodata['lat'] = result['geometry']['location']['lat']
        geodata['lng'] = result['geometry']['location']['lng']
        geodata['address'] = result['formatted_address']

        data = pd.DataFrame(geodata,index=[0])
        data['Place'] = s
        if s.split(",")[-1].title() not in result['formatted_address']:
            data = pd.DataFrame()
        else:
            print('{address}. (lat, lng) = ({lat}, {lng})'.format(**geodata))
            pass

    except:
        data = pd.DataFrame()
        pass


    return data

def find_dup(lst):
     seen = set()
     it = iter(lst)
     for item in it:
         if item not in seen:
             seen.add(item)
             yield item
         else:
             yield from it


#%%
main_1 = pd.read_sql("select * from AdqvestDB.CAG_SCHOOLS_VILLAGE_LEVEL_DATA_Abhi",con=engine)
main_2 = pd.read_sql("select * from AdqvestDB.CAG_SCHOOLS_VILLAGE_LEVEL_DATA_MAIN_2",con=engine)
op = pd.concat([main_1,main_2])
#%%

#dfp = pd.read_sql("select * from AdqvestDB.CAG_SCHOOLS_VILLAGE_LEVEL_DATA_Abhi",con=engine)
df1 = pd.read_csv("CAG_NEW_SAT_IMAGES_V2.csv")
df2 = pd.read_csv("Villages_Coordinates.csv")
df2 = df2[list(df1.columns)]
#dfp.to_csv("CAG_MAIN.csv",index=False)

dist1 = list(df1['District_Name'].unique())
dist2 = list(df2['District_Name'].unique())
new  = dist1+dist2
#main_dist = list(set(dist1)^set(dist2))
main_dist = list(set(new))

op_df = op[op['District_Name'].isin(main_dist)]

df1 = pd.concat([df1,df2])

main_df = df1[df1['District_Name'].isin(main_dist)]
main_df['State_Name'] = main_df['State_Name'].apply(lambda x : x.title())
#main_df['Pincode'] = main_df['Pincode'].apply(lambda x : int(x))

main_df = main_df[(main_df['Village_Name'].str.contains("WARD")==False)|(main_df['Village_Name'].str.contains("WORD")==False)|(main_df['Village_Name'].str.contains("WARD ")==False)]

cols = list(main_df.columns) + ['Latitude','Longitude']
new_rows=[]

main = pd.DataFrame(columns=cols)

main_df['Input_Parameter1'] = main_df['Village_Name']+','+main_df['State_Name']
main_df['Input_Parameter2'] = main_df['Village_Name']+','+main_df['District_Name']+','+main_df['State_Name']
main_df['Input_Parameter3'] = main_df['Village_Name']+','+main_df['Block_Name']+','+main_df['State_Name']
main_df['Input_Parameter4'] = main_df['Village_Name']+','+main_df['Block_Name']+','+main_df['District_Name']+','+main_df['State_Name']
main_df['Input_Parameter0'] = main_df['Village_Name']+','+main_df['State_Name']+','+ main_df['Pincode'].astype(str)

#sample = main_df.sample(n = 20)

sample1 = main_df['Input_Parameter1'].to_list()
sample2 = main_df['Input_Parameter2'].to_list()
sample3 = main_df['Input_Parameter3'].to_list()
sample4 = main_df['Input_Parameter4'].to_list()
#sample = sample[1895:len(main_df)]
samples = [sample1,sample2,sample3,sample4]
main1 = pd.DataFrame()
#main_df = main_df.sample(n=100)
unmatched = []
main_df['Village_Name'] = main_df['Village_Name'].apply(lambda x : x.title())
for i,row in main_df.iterrows():

    s = row['Input_Parameter4']
    data = geocode(s)
    try:
        data = get_matched(data)[0]
        data['Parse'] = 'Parse 1'
    except:
        pass
    if data.empty:
       s = row['Input_Parameter3']
       data = geocode(s)
       try:
           data = get_matched(data)[0]
           data['Parse'] = 'Parse 2'
       except:
           pass
       if data.empty:
           s = row['Input_Parameter2']
           try:
               data = geocode(s)
               data = get_matched(data)[0]
               data['Parse'] = 'Parse 3'
           except:
               pass
           if data.empty:
               s = row['Input_Parameter1']
               try:
                   data = geocode(s)
                   data = get_matched(data)[0]
                   data['Parse'] = 'Parse 4'
               except:
                   pass
               if data.empty:
                   s = row['Input_Parameter0']
                   try:
                       data = geocode(s)
                       data = get_matched(data)[0]
                       data['Parse'] = 'Parse 5'
                   except:
                        pass
                   if data.empty:
                        print("Unmatched")
                        unmatched.append(s)
                        pass

    main1 = pd.concat([main1,data])

main1.to_csv("Villages_PR60.csv",index=False)
df = pd.DataFrame({'Unmatched_Villages':unmatched})
df.to_csv("Unmatched_Villages.csv",index=False)
main_df.to_csv("Input_DataFrame.csv",index=False)
op_df.to_csv("Consolidated_Schools.csv",index=False)
