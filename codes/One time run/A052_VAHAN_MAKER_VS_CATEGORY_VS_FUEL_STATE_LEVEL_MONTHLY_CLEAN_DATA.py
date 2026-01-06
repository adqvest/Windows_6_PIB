import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
os.chdir(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
import requests
import json
from bs4 import BeautifulSoup
from dateutil import parser

from time import sleep
import random

import re
import ast
from datetime import timedelta
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
import zipfile
import sys
import time
from lxml import etree
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import warnings
warnings.filterwarnings('ignore')
import numpy as np
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# from clickhouse_driver import Client
# client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com', user='default', password='@Dqu&TP@ssw0rd', database='AdqvestDB', port=9000)
import ClickHouse_db



def clean_table(df):
    #df['Maker_Name_Clean']=df['Maker_Name'].str.title()
    df['Maker_Clean']=df['Maker'].str.replace('LTD',' ').str.replace('LIMITED',' ').str.replace('LTD\.,',' ').str.replace('LTD\.',' ').str.replace('LTD\*',' ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('M/S',' ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('PVT',' ').str.replace('PVT\.,',' ').str.replace('PVT\.',' ').str.replace('PRIVATE',' ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('\sI\s',' ').str.replace('\(I\)',' ').str.replace('\sP\s',' ').str.replace('\sL\s',' ').str.replace('\sP\.',' ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('&',' AND ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('\.',' ').str.replace(',',' ')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('\s\s+',' ').str.strip()
    df['Maker_Clean']=df['Maker_Clean'].str.replace('TATA MOTORS PASSENGER VEHICLES','TATA MOTORS')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('GURU NANAK AGRI ENGG WORKS','GURU NANAK AGRI WORKS')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('NK INDUSTRIES','N K INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('PREEET AGRO INDUSTRIES','PREET AGRO INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('CHAANY AGRO INDUSTRIES','CHANNY AGRO INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('HERO ELECTRIC VEHICLE','HERO ELECTRIC VEHICLES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('HERO ELECTRIC VEHICLESS','HERO ELECTRIC VEHICLES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('KS AGROTECH','K S AGROTECH')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('SHRI RAM AUTO TECH','SHRIRAM AUTO TECH')
    return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

        ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTHLY_CLEAN_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        if(today.day in [24]):

            max_date_1=pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA', engine)['Max'][0]
            max_date_2=pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_TRACTOR_MONTHLY_DATA', engine)['Max'][0]

            if max_date_1!=max_date_2:
                raise Exception("Latest data hasnt been captured")

            max_date_1 = max_date_1 - timedelta(days=90)
            max_date_2 = max_date_2 - timedelta(days=90)
            max_date_1 = max_date_1.strftime('%Y-%m-%d')

            df=pd.read_sql('SELECT  Maker,Total,Vehicle_Category,Fuel,State,RTO_Office_Raw,RTO_Date,RTO_Code,RTO_Office,Relevant_Date FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_DATA where Relevant_Date > "' + max_date_1 + '"', engine)
            df1=pd.read_sql('SELECT  Maker,Total,Vehicle_Category,Fuel,State,RTO_Office_Raw,RTO_Date,RTO_Code,RTO_Office,Relevant_Date FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_TRACTOR_MONTHLY_DATA where Relevant_Date > "' + max_date_1 + '"', engine)

            # merged_df=df.merge(df1,on=['Maker','Vehicle_Category','Fuel','State','Relevant_Date'],how='left')
            merged_df=df.merge(df1,on=['Maker','Vehicle_Category','Fuel','State','RTO_Office_Raw','RTO_Date','RTO_Code','RTO_Office','Relevant_Date'],how='left')
            merged_df['Total_y']=merged_df['Total_y'].fillna(0)
            merged_df['Total']=merged_df['Total_x']-merged_df['Total_y']
            merged_df['Total']=merged_df['Total'].apply(lambda x:int(x))



            df1['Vehicle_Category']=df1['Vehicle_Category'].apply(lambda x:x+'-Tractor')
            df1['Vehicle_Segment']='Tractors'


            merged_df.drop(['Total_x','Total_y'],axis=1,inplace=True)

            mapped={'3WT':'3W','3WN':'3W','2WN':'2W','2WT':'2W','2WIC':'2W',
           '4WIC':'4W','LMV':'4W','HGV':'M&HCV','HMV':'M&HCV',
           'MGV':'M&HCV','MMV':'M&HCV','LGV':'LCV',
           'LPV':'Small Buses','MPV':'Buses','HPV':'Buses','OTH':'Others'}

            mapped_2={'MAHINDRA & MAHINDRA LIMITED (TRACTOR)':'MAHINDRA & MAHINDRA','MAHINDRA & MAHINDRA LIMITED (SWARAJ DIVISION)':'MAHINDRA & MAHINDRA',
            'EICHER TRACTORS':'TAFE LIMITED', 'MAHINDRA & MAHINDRA LTD FARM MACHINERY DIVISION':'MAHINDRA & MAHINDRA'}

            merged_df['Vehicle_Segment']=merged_df['Vehicle_Category'].map(mapped)



            merged_df=merged_df.append(df1)
            merged_df['Maker']=merged_df['Maker'].replace(mapped_2)
            merged_df=clean_table(merged_df)


            merged_df['Runtime']=datetime.datetime.now()
            merged_df=merged_df[merged_df['Total']>0]
            merged_df[merged_df['Maker'].str.contains(pat='MAHINDRA')]['Maker_Clean'] = 'MAHINDRA AND MAHINDRA'
            merged_df['Maker_Clean'] = ['AMPERE VEHICLES' if x.startswith('GREAVES') else x for x in merged_df['Maker_Clean']]

            date_vahan=merged_df['Relevant_Date'].min()
            date_vahan = date_vahan.strftime('%Y-%m-%d')

            print(merged_df.head(5))


            query = "delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_CLEAN_DATA where Relevant_Date >='" + date_vahan + "'"
            connection.execute(query)
            connection.execute('commit')

            merged_df.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_FUEL_RTO_LEVEL_MONTHLY_CLEAN_DATA",index = False,con = engine,if_exists = "append")

        else:
            print("Not today")





        log.job_end_log(table_name,job_start_time, no_of_ping)
#

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
