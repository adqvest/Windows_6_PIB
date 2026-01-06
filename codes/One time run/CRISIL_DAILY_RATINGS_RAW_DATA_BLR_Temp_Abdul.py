# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 14:18:28 2022

@author: Abdulmuizz
"""

import os
import requests
import json
from bs4 import BeautifulSoup
#import tabula
import pdfplumber

from time import sleep
import random
import camelot
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import zipfile
import sys
import time
#import tabula
import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db

# =============================================================================
# import adqvest_s3
# import rpy2.robjects as ro
# import boto3
# from botocore.config import Config
# =============================================================================


engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

os.chdir('C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER')

def prepare_column(col):
    col = col.replace('(', ' ').replace(')', ' ').replace('.', ' ')
    col = re.sub(r'  +', ' ', col).strip()
    col = col.title()
    col = col.replace(' ', '_')
    return col

def pre_process_html(path):

    crisil = pd.read_html(path)
    index_ = []
    for i in range(len(crisil)):
        cond = crisil[i].apply(lambda row: row.astype(str).str.lower().str.contains('current facilities').any(), axis=1).any()
        if(cond):
            index_.append(i)

    crisil = crisil[index_[-1]]
    start_index = crisil[crisil[0].str.lower().str.contains("current")].index[0]
    end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]
    crisil = crisil.iloc[start_index:end_index]
    columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()
    columns = [prepare_column(col) for col in columns]
    crisil = crisil.iloc[2:]
    crisil.columns = columns
    return crisil

def pre_process_html_new(path):

    try:
        crisil = pd.read_html(path)
        remarks=crisil[0].iloc[:,0]
        remarks.dropna(inplace=True)
        try:
            a=remarks.iloc[1][remarks.iloc[1].index(';')+1:].strip()
        except:
            a=remarks.iloc[1]
        a=a.replace('Ratings','').replace('Rating','').strip().title()
    
        index_ = []
    
        for i in range(len(crisil)):
            cond = crisil[i].apply(lambda row: row.astype(str).str.lower().str.contains('details of bank lenders & facilities').any(), axis=1).any()
            if(cond):
                index_.append(i)
    
        for i in range(len(crisil)):
            cond = crisil[i].apply(lambda row: row.astype(str).str.lower().str.contains('details of various bank facilities').any(), axis=1).any()
            if(cond):
                index_.append(i)
    
        crisil = crisil[index_[-1]]
        crisil.reset_index(drop=True,inplace=True)
        print(crisil)
        try:
            crisil.dropna(inplace = True)
            crisil.reset_index(drop=True,inplace=True)
            start_index = crisil[crisil[2].str.lower().str.contains("rating")].index[0]
        except:
            crisil.dropna(inplace = True)
            crisil.reset_index(drop=True,inplace=True)
            start_index = crisil[crisil[3].str.lower().str.contains("rating")].index[0]
        #end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]
    
        crisil = crisil.iloc[start_index:]
        columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()
        
        crisil = crisil.iloc[1:,[0,1,-1]]
        crisil.columns=['Current_Facilities_Facility','Current_Facilities_Amount_Rs_Crore','Current_Facilities_Rating']
        crisil['Remarks']=a
    except:
        try:
            crisil = pd.read_html(path)
            remarks=crisil[0].iloc[:,0]
            remarks.dropna(inplace=True)
            try:
                a=remarks.iloc[1][remarks.iloc[1].index(';')+1:].strip()
            except:
                a=remarks.iloc[1]
            a=a.replace('Ratings','').replace('Rating','').strip().title()
            index_ = []
            for i in range(len(crisil)):
                cond = crisil[i].apply(lambda row: row.astype(str).str.lower().str.contains('details of bank lenders & facilities').any(), axis=1).any()
                if(cond):
                    index_.append(i+1)
    
            crisil = crisil[index_[-1]]
            crisil.reset_index(drop=True,inplace=True)
            print(crisil)
            try:
                crisil.dropna(inplace = True)
                crisil.reset_index(drop=True,inplace=True)
                start_index = crisil[crisil[2].str.lower().str.contains("rating")].index[0]
            except:
                crisil.dropna(inplace = True)
                crisil.reset_index(drop=True,inplace=True)
                start_index = crisil[crisil[3].str.lower().str.contains("rating")].index[0]
            #end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]
    
            crisil = crisil.iloc[start_index:]
            columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()
    
            crisil = crisil.iloc[1:,[0,1,-1]]
            crisil.columns=['Current_Facilities_Facility','Current_Facilities_Amount_Rs_Crore','Current_Facilities_Rating']
            crisil['Remarks']=a
        except:
            crisil = pd.read_html(path)
            remarks=crisil[0].iloc[:,0]
            remarks.dropna(inplace=True)
            try:
                a=remarks.iloc[1][remarks.iloc[1].index(';')+1:].strip()
            except:
                a=remarks.iloc[1]
            a=a.replace('Ratings','').replace('Rating','').strip().title()
            for i in range(len(crisil)):
                if crisil[i].iloc[:,0][0] == 'Facility':
                    #
                    crisil= crisil[i]
                    break
                else:
                    continue
            # crisil = crisil[index_[-1]]
            crisil.reset_index(drop=True,inplace=True)
            print(crisil)
            try:
                crisil.dropna(inplace = True)
                crisil.reset_index(drop=True,inplace=True)
                start_index = crisil[crisil[2].str.lower().str.contains("rating")].index[0]
            except:
                crisil.dropna(inplace = True)
                crisil.reset_index(drop=True,inplace=True)
                start_index = crisil[crisil[3].str.lower().str.contains("rating")].index[0]
            #end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]
    
            for i in list(crisil):
                if crisil[i].str.lower().str.contains('name').any():
                    crisil.drop(i, axis = 1, inplace = True)
    
            crisil = crisil.iloc[start_index:]
            #columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()
    
            #columns = [prepare_column(col) for col in columns]
            crisil = crisil.iloc[1:,[0,1,-1]]
            crisil.columns=['Current_Facilities_Facility','Current_Facilities_Amount_Rs_Crore','Current_Facilities_Rating']
            
    
    return crisil

def pre_process_pdf(path):
    df = camelot.read_pdf(path, pages='all', line_scale=60)
    crisil = []
    for i in range(df.n):
        crisil.append(df[i].df)

    index_ = []
    for i in range(len(crisil)):
        cond = crisil[i].apply(lambda row: row.astype(str).str.lower().str.contains('current facilities').any(), axis=1).any()
        if(cond):
            index_.append(i)

    crisil = crisil[index_[-1]]
    crisil = crisil.apply(lambda col: col.str.replace('\n',' ').str.strip(), axis=0)

    crisil[crisil==''] = None
    crisil.iloc[0] = crisil.iloc[0].ffill()
    start_index = crisil[crisil[0].str.lower().str.contains("current")].index[0]
    end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]
    crisil = crisil.iloc[start_index:end_index]
    columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()
    columns = [prepare_column(col) for col in columns]
    crisil = crisil.iloc[2:]
    crisil.columns = columns
    return crisil

def pre_process_pdf_new(path):

    df = camelot.read_pdf(path, pages='all', line_scale=60)
    crisil = []
    for i in range(df.n):
        crisil.append(df[i].df)

    for i in range(len(crisil)):
        if crisil[i].iloc[:,0].str.lower().str.contains('facility').any():
            #
            crisil= crisil[i]
            break
        else:
            continue

    # crisil = crisil[index_[-1]]
    crisil.reset_index(drop=True,inplace=True)
    print(crisil)
    try:
        crisil.dropna(inplace = True)
        crisil.reset_index(drop=True,inplace=True)
        start_index = crisil[crisil[2].str.lower().str.contains("rating")].index[0]
    except:
        crisil.dropna(inplace = True)
        crisil.reset_index(drop=True,inplace=True)
        start_index = crisil[crisil[3].str.lower().str.contains("rating")].index[0]
    #end_index = crisil[crisil[0].str.lower().str.contains("total")].index[-1]

    for i in list(crisil):
        if crisil[i].str.lower().str.contains('name').any():
            crisil.drop(i, axis = 1, inplace = True)

    crisil = crisil.iloc[start_index:]
    columns = (crisil.iloc[0] + " " + crisil.iloc[1]).to_list()

    #columns = [prepare_column(col) for col in columns]
    crisil = crisil.iloc[1:,[0,1,-1]]
    crisil.columns=['Current_Facilities_Facility','Current_Facilities_Amount_Rs_Crore','Current_Facilities_Rating']
    return crisil

def detect_table_blr_pdf(path):
    #text = textract.process(filename = path )
    #text = text.decode("utf-8")
    text=''
    with pdfplumber.open(path) as pdf:
        for i in range(len(pdf.pages)):
            text+= pdf.pages[i].extract_text()
    found='No'
    if("annexure - details of various bank facilities" in text.lower()):
        print("Relevant Table present in pdf")
        found='Yes'
        #return True
    else:
        print("Relevant Table not present in pdf")
        #return False
    if("annexure - details of bank lenders & facilities" in text.lower()):
        print("Relevant Table present in pdf")
        found='Yes'
        #return True
    else:
        print("Relevant Table not present in pdf")
        #return False

    if found=="Yes":
        return True
    else:
        return False
    
def detect_table_blr_html(path):
    with open(path) as f:
        soup = BeautifulSoup(f)
    found1='no'
    if(soup.body.findAll(text=re.compile('Annexure - Details of various bank facilities')) != []):
        print("Table 1 present")
        found1='yes'
        #return True
    else:
        print("Table 1 not present")
        #return False
    if(soup.body.findAll(text=re.compile('Annexure - Details of Bank Lenders & Facilities')) != []):
        print("Table 1 present")
        found1='yes'
        #return True
    else:
        print("Table 1 not present")
        #return False
    if found1=="yes":
        return True
    else:
        return False
    
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"}
query = "select * from AdqvestDB.CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul where Relevant_Date > '2021-07-31' and Status_BLR = 'Failed' and Is_BLR_Table_Present = 'Yes'"
crisil_link = pd.read_sql(query, con=engine)
files_in_drive = os.listdir()


if(crisil_link.shape[0]>0):
    crisil_link['Mon_Day'] = crisil_link['Relevant_Date'].apply(lambda x:x.strftime("%m-%d"))
    links_df = pd.DataFrame()
    for _, df in crisil_link.groupby('Mon_Day'):
        links_df = pd.concat([links_df, df])

    for _, row in links_df.iterrows():
        print(row['Rating_File_Name'])
        try:
            if(row['Rating_File_Name'] in files_in_drive):
                print('file available locally')
                path = "C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER/" + row['Rating_File_Name']
            else:
                url = row['Rating_File_Link']
                #no_of_ping += 1
                print('Going to download')
                #url=url.replace(' ','%20')
                print(url)
                #ro.r('download.file("'+url+'", destfile = "/home/ubuntu/crisil_data/' + row['Rating_File_Name'] + '", mode="wb")')
                r =  requests.get(url,verify = False,headers={"User-Agent": "XY"})
                time.sleep(7)
                with open(row['Rating_File_Name'],'wb') as f:
                    f.write(r.content)
                    f.close()
                print('Downloaded')
                path = "C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER/" + row['Rating_File_Name']
                
            if('.pdf' in row['Rating_File_Name']):
                print('processing pdf')
                try:
                    crisil = pre_process_pdf(path)
                except:
                    crisil = pre_process_pdf_new(path)
            else:
                print('processing html')
                print(row['Rating_File_Link'])
                try:
                    crisil = pre_process_html(path)
                except:
                    crisil = pre_process_html_new(path)
            crisil['Industry_Name'] = row['Industry_Name']
            crisil['Company_Code'] = row['Company_Code']
            crisil['Heading'] = row['Heading']
            crisil['Relevant_Date'] = row['Relevant_Date']
            crisil["Rating_File_Link"] = row["Rating_File_Link"]
            crisil["Company_Name"] = row["Company_Name"]
            crisil["Issue_Type"] = "BLR"
            crisil["Source"] = "BLR Data"
            crisil['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            try:
                crisil.to_sql("CRISIL_DAILY_RATINGS_RAW_DATA_Temp_Abdul", if_exists='append', index=False, con=engine)
            except:
                new_col_1 = crisil.columns.to_list()[:6]
    #             print(new_col_1)
                new_col_2 = [col.replace('_', '').lower() for col in new_col_1]
    #             print(new_col_2)
                crisil = crisil.rename(columns={new_col_1[0]:new_col_2[0],
                                                 new_col_1[1]:new_col_2[1],
                                                 new_col_1[2]:new_col_2[2],
                                                 new_col_1[3]:new_col_2[3],
                                                 new_col_1[4]:new_col_2[4],
                                                 new_col_1[5]:new_col_2[5]})
                crisil = crisil.rename(columns={'currentfacilitiesfacility':'Current_Facilities_Facility',
                                                 'currentfacilitiesamountrscrore':'Current_Facilities_Amount_Rs_Crore',
                                                 'currentfacilitiesrating':'Current_Facilities_Rating',
                                                 'previousfacilitiesfacility':'Previous_Facilities_Facility',
                                                 'previousfacilitiesamountrscrore':'Previous_Facilities_Amount_Rs_Crore',
                                                 'previousfacilitiesrating':'Previous_Facilities_Rating'})
    #             print(crisil.columns)
                crisil["Rating_File_Link"] = row["Rating_File_Link"]
                crisil["Company_Name"] = row["Company_Name"]
                crisil["Issue_Type"] = "BLR"
                crisil["Source"] = "BLR Data"
                crisil.to_sql("CRISIL_DAILY_RATINGS_RAW_DATA_Temp_Abdul", if_exists='append', index=False, con=engine)

            query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Status_BLR ='Succeeded', Is_BLR_Table_Present = null where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
            connection.execute(query)
            connection.execute('commit')
            print(row['Rating_File_Name'])
            print("*************************")

            time.sleep(6)
        except:
            print('failed -> ', row['Rating_File_Name'] )
            query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Status_BLR='Failed', Is_BLR_Table_Present = null where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
            connection.execute(query)
            connection.execute('commit')
            time.sleep(4)
            print("*************************")
            error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
            error_msg = str(sys.exc_info()[1])
            print(error_type)
            print(error_msg)
            continue

else:
    print("no links with status null")
print("coming out from program")


#################################TABLE DETECTION###################3

query_df = "select * from AdqvestDB.CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul where Relevant_Date > '2021-07-31' and Is_BLR_Table_Present is null"
crisil_link = pd.read_sql(query_df, con=engine)

if(crisil_link.shape[0]>0):
    crisil_link['Mon_Day'] = crisil_link['Relevant_Date'].apply(lambda x:x.strftime("%m-%d"))
    links_df = pd.DataFrame()
    for _, df in crisil_link.groupby('Mon_Day'):
        links_df = pd.concat([links_df, df])
    links_df = links_df.sort_values(by = "Relevant_Date",ascending = False)
    for _, row in links_df.iterrows():
        try:
            if(row['Rating_File_Name'] in files_in_drive):
                print('file available locally')
                path = "C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER/" + row['Rating_File_Name']
            else:
                url = row['Rating_File_Link']
                #no_of_ping += 1
                #ro.r('download.file("'+url+'", destfile = "/home/ubuntu/crisil_data/' + row['Rating_File_Name'] + '", mode="wb")')
                r =  requests.get(url,verify = False,headers={"User-Agent": "XY"})
                time.sleep(3)
                with open(row['Rating_File_Name'],'wb') as f:
                    f.write(r.content)
                    f.close()
                path = "C:/Users/Administrator/AdQvestDir/CRISIL_DOWNLOAD_FOLDER/" + row['Rating_File_Name']
            if('.pdf' in row['Rating_File_Name']):
                print('processing pdf')

                try:
                    status = detect_table_blr_pdf(path)
                except:
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present='Not Detected' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')

                if(status == True):
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present='Yes' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                else:
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present = 'No' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')




            else:
                print('processing html')
                print(row['Rating_File_Link'])

                try:
                    status_1 = detect_table_blr_html(path)
                except:
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present='Not Detected' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')

                if(status_1 == True):
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present='Yes' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')
                else:
                    query = "update CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present = 'No' where Relevant_Date='" + row['Relevant_Date'].strftime('%Y-%m-%d') + "' and Pr_Id=" + str(row['Pr_Id'])
                    print(query)
                    connection.execute(query)
                    connection.execute('commit')




            print(row['Rating_File_Name'])
            print("*************************")

            # time.sleep(2)
        except:
            print('failed -> ', row['Rating_File_Name'] )

            print("*************************")


else:
    print("no links with status null")
print("coming out from program")

query = "update AdqvestDB.CRISIL_FILES_LINKS_DAILY_DATA_Temp_Abdul set Is_BLR_Table_Present = 'Not Detected' where Is_BLR_Table_Present is null and Status_BLR is not null"
connection.execute(query)
connection.execute('commit')