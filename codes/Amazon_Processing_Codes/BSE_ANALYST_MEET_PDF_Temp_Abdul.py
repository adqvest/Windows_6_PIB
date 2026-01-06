# -*- coding: utf-8 -*-
"""
Created on Fri Feb  3 16:31:22 2023

@author: Abdulmuizz
"""

import camelot
# import time
import pandas as pd
import boto3
from botocore.config import Config
from collections import defaultdict
import os
import re
# from dateutil.parser import parse
# from IPython.core import display as ICD
import PyPDF2
import datetime
import sys
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_s3
import adqvest_db

# os.chdir("C:/Users/Abdulmuizz/Desktop/ADQVest/BSE NSE")

# final_df = pd.read_excel(r"C:\Users\Abdulmuizz\Desktop\ADQVest\BSE NSE\Analyst_Meet.xlsx")

engine = adqvest_db.db_conn()

ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
BUCKET_NAME = 'adqvests3bucket'


s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s3v4',region_name = 'ap-south-1')

)

my_bucket = s3.Bucket('adqvests3bucket')

direc = "C:/Users/Administrator/AdQvestDir/Analyst_Meet/"
os.chdir(direc)
all_dfs = defaultdict(list)

list_of_files = []

for idx, row in final_df.iterrows():
    
    print(idx)
    # obj = my_object.key
    # if obj == 'BSE_Analyst_Meet/':
    #     continue
    file_name = row['File_Name']
    path = direc + file_name
    list_of_files.append(file_name)

    r =  requests.get(row['File_Link'],headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70"})
    limit = 0
    while True:
        try:
            with open(path,'wb') as f:
                f.write(r.content)
                f.close()
            break
        except:
            limit += 1
            if limit > 10:
                break

    if limit > 10: 
        continue
    
    # s3.Object('adqvests3bucket',obj).download_file(path)
    
    limit = 0
    while True:
        try:
            with open(path,'rb') as file:
                pdfReader = PyPDF2.PdfFileReader(file)
                total_pages = pdfReader.numPages

            break
        except:
            time.sleep(30)
            limit += 1
            if limit > 3:
                break

    if limit > 3:
        continue   
            
    if total_pages > 2:
        os.remove(file_name)
        continue
    d = []
    try:
        d = camelot.read_pdf(path, pages = 'all')
    except:
        os.remove(file_name)
        continue
        
        
    try:
        if len(d) == 0:
            d = camelot.read_pdf(path, flavor = 'lattice',line_scale = 70, pages = 'all')
    except:
        pass
    
    for i in range(len(d)):
        df = d[i].df
        if df.iloc[0].str.lower().str.contains('date').any():
            all_dfs[file_name].append(df)
    
    os.remove(file_name)
    
    
months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december","jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
keywords = ['date','time','day','timing','venue', 'location', 'mode','type','medium','place','discuss','link','no.','detail','host','nature','\d+'] +months    

final_df = pd.DataFrame()
for key,value in zip(all_dfs.keys(),all_dfs.values()):
    mydf = value[0].copy()
    mydf.columns = mydf.iloc[0]
    mydf= mydf.drop(0)
    mydf = mydf.reset_index(drop = True)
    mydf = mydf.drop(list(mydf.loc[:, mydf.columns.str.lower().str.contains('|'.join(keywords))].columns), axis = 1)
    if mydf.empty:
        continue
        
    if len(mydf.columns) > 2:
        continue
    
    mydf.columns = list(range(len(mydf.columns)))
    numbers = False
    for x in mydf.columns:
        if mydf[x].str.lower().str.contains('number').any():
            numbers = True
    if numbers == True:
        continue
    
    if len(mydf.columns) == 0:
        continue
    elif len(mydf.columns) == 1:
        mydf.insert(1,'New_Col','')
        mydf.columns = ['Fund_house_1','Fund_house_2']
    elif len(mydf.columns) == 2:
        mydf.columns = ['Fund_house_1','Fund_house_2']
        
    mydf['Fund_house_1'] = mydf['Fund_house_1'].apply(lambda x: re.sub('\n',' ',x))
    mydf['Fund_house_2'] = mydf['Fund_house_2'].apply(lambda x: re.sub('\n',' ',x))
            
    mydf.insert(0,'Company_Name',key.split('%%')[0].replace('_',' '))
    mydf.insert(1,'File_Name_AWS',key)
    mydf['Relevant_Date'] = datetime.datetime.strptime(key.split('%%')[1], '%Y-%m-%d').date()
    
    final_df = pd.concat([final_df,mydf])
    
final_df.to_sql(name = "BSE_ANALYST_MEET_PDF_Temp_Abdul",con = engine,if_exists = 'append',index = False)
    
    

