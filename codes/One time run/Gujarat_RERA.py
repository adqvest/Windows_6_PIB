# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 10:59:53 2023

@author: Abdulmuizz
"""

import requests
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import pandas as pd

engine = adqvest_db.db_conn()


def getInventory(ID):
    print(ID)
    
    r = requests.get('https://gujrera.gujarat.gov.in/dashboard/get-all-view-unit-details-by-id/' + ID)
    
    data = json.loads(r.content)

    # inventory = data['data']['dev'][0]['internalDev'][0]['noOfInventory']
    try:
        inventory = int(data['data'][0]['totunit'])
    except:
        inventory = 0
    
    
    return inventory

headers = {'Host': 'gujrera.gujarat.gov.in',
           'Referer': 'https://gujrera.gujarat.gov.in/',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.54'}

r = requests.get("https://gujrera.gujarat.gov.in/dashboard/get-district-wise-projectlist/0/0/all/all/all")

#%%

import json

data = json.loads(r.content)

#%%

req = data['data']

#%%
df = pd.DataFrame(req)

new_names = {
    'promoterName' : 'Promoter_Name',
    'approvedOn' : 'Approved_On',
    'projectRegId' : 'Project_Reg_Id',
    'wfoid' : 'wfoid',
    'projectCost' : 'Project_Cost',
    'projectType' : 'Project_Type',
    'districtName' : 'District_Name',
    'districtType' : 'District_Type',
    'promoterAddress' : 'Promoter_Address',
    'regFee' : 'Reg_Fee',
    'extDate' : 'Proj_Extended_End_Date',
    'projOrgFDate' : 'Proj_Original_End_Date',
    'startDate' : 'Start_Date',
    'endDate' : 'End_Date',
    'projectName' : 'Project_Name',
    'regNo' : 'Reg_No'
}


df.rename(columns = new_names, inplace = True)

df = df[['Project_Name', 'Promoter_Name','Project_Type', 'Project_Cost', 'Reg_No','Start_Date', 'End_Date', 'Approved_On','Project_Reg_Id', 'District_Name', 'District_Type',
       'Promoter_Address', 'Reg_Fee', 'Proj_Original_End_Date', 'Proj_Extended_End_Date','wfoid']]

df['Total_Inventory'] = [getInventory(x) for x in df['wfoid']]

#%%
import numpy as np
import datetime

df['Project_Cost'] = df['Project_Cost'].astype(float)
df['Reg_Fee'] = df['Reg_Fee'].astype(float)

for col in ['Start_Date', 'End_Date', 'Approved_On']:
    df[col] = np.where(df[col].isna(), None, df[col])
    df[col] = [datetime.datetime.strptime(x,'%d-%B-%Y').date() if x != None else None for x in df[col]] 

for col in ['Proj_Original_End_Date', 'Proj_Extended_End_Date']:
    df[col] = [datetime.datetime.strptime(x[:-2],'%Y-%m-%d %H:%M:%S').date() if x != None else None for x in df[col]] 

df = df.replace('NA',None)

df = df[['Reg_No','Project_Reg_Id', 'Reg_Fee','Project_Name', 'Promoter_Name','Project_Type', 'Project_Cost', 'District_Name', 'District_Type', 'Start_Date', 'End_Date', 'Approved_On',
       'Promoter_Address', 'Proj_Original_End_Date', 'Proj_Extended_End_Date','Total_Inventory']]

df['Relevant_Date'] = datetime.date(2023,4,4)
df['Runtime'] = datetime.datetime(2023, 4, 4, 11, 34, 52)

df.to_sql(name='GUJARAT_RERA_DISTRICT_WISE_WEEKLY_DATA_ONE_TIME',con=engine,if_exists='append',index=False)
