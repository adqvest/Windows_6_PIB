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
import cleancompanies
import JobLogNew as log
import MySql_To_Clickhouse as MySql_CH
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
    df['Maker_Clean']=df['Maker_Clean'].str.replace('TATA PASSENGER ELECTRIC MOBILITY','TATA MOTORS')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('GURU NANAK AGRI ENGG WORKS','GURU NANAK AGRI WORKS')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('NK INDUSTRIES','N K INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('PREEET AGRO INDUSTRIES','PREET AGRO INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('CHAANY AGRO INDUSTRIES','CHANNY AGRO INDUSTRIES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('HERO ELECTRIC VEHICLE','HERO ELECTRIC VEHICLES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('HERO ELECTRIC VEHICLESS','HERO ELECTRIC VEHICLES')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('KS AGROTECH','K S AGROTECH')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('SHRI RAM AUTO TECH','SHRIRAM AUTO TECH')
    df['Maker_Clean']=df['Maker_Clean'].str.replace('EICHER TRACTORS','TAFE')

    return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

        ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # if(today.day in [1,8,15,22,29]):   #1,4,11,12,16,22,27

        max_date_1=pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTHLY_DATA_MP', engine)['Max'][0]
        max_date_2=pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_TRAC_MONTHLY_DATA_MP', engine)['Max'][0]
        print(max_date_1, max_date_2)
        if max_date_1!=max_date_2:
            raise Exception("Latest data hasnt been captured")

        # clean_max_date = pd.read_sql('SELECT max(Relevant_Date) as Max FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP', engine)['Max'][0]
        if 1==1:
            print('New data')
            df=pd.read_sql('SELECT Maker,Total,Vehicle_Category,Fuel,State,Relevant_Date, Runtime FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTHLY_DATA_MP', engine)
            df1=pd.read_sql('SELECT Maker,Total,Vehicle_Category,Fuel,State,Relevant_Date, Runtime FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_TRAC_MONTHLY_DATA_MP', engine)

            merged_df=df.merge(df1,on=['Maker','Vehicle_Category','Fuel','State','Relevant_Date'],how='left')
            merged_df['Total_y']=merged_df['Total_y'].fillna(0)
            merged_df['Total']=merged_df['Total_x']-merged_df['Total_y']
            merged_df['Total']=merged_df['Total'].apply(lambda x:int(x))



            df1['Vehicle_Category']=df1['Vehicle_Category'].apply(lambda x:x+'-Tractor')
            df1['Vehicle_Segment']='Tractors'


            merged_df.drop(['Total_x','Total_y', 'Runtime_y'],axis=1,inplace=True)
            merged_df = merged_df.rename(columns = {'Runtime_x' : 'Runtime'})

            mapped={'3WT':'3W','3WN':'3W','2WN':'2W','2WT':'2W','2WIC':'2W',
           '4WIC':'4W','LMV':'4W','HGV':'M&HCV','HMV':'M&HCV',
           'MGV':'M&HCV','MMV':'M&HCV','LGV':'LCV',
           'LPV':'Small Buses','MPV':'Buses','HPV':'Buses','OTH':'Construction Equipment'}

            mapped_2={'MAHINDRA & MAHINDRA LIMITED (TRACTOR)':'MAHINDRA & MAHINDRA','MAHINDRA & MAHINDRA LIMITED (SWARAJ DIVISION)':'MAHINDRA & MAHINDRA',
            'EICHER TRACTORS':'TAFE LIMITED', 'MAHINDRA & MAHINDRA LTD FARM MACHINERY DIVISION':'MAHINDRA & MAHINDRA'}

            merged_df['Vehicle_Segment']=merged_df['Vehicle_Category'].map(mapped)



            merged_df=merged_df.append(df1)
            # merged_df['Maker_Clean']=merged_df['Maker'].replace(mapped_2)
            merged_df=clean_table(merged_df)
            merged_df['RTO_Office_Raw']=np.nan
            merged_df['RTO_Date']=np.nan
            merged_df['RTO_Code']=np.nan
            merged_df['RTO_Office']=np.nan

            date=datetime.date(2023,6,30)


            merged_df['Act_Runtime']=datetime.datetime.now()
            merged_df=merged_df[merged_df['Total']>0]
            merged_df['Maker_Clean'][merged_df['Maker'].str.contains(pat='MAHINDRA')] = 'MAHINDRA AND MAHINDRA'
            merged_df['Maker_Clean'] = ['AMPERE VEHICLES' if x.startswith('GREAVES') else x for x in merged_df['Maker_Clean']]

            # date_vahan=merged_df['Relevant_Date'].max()
            # date_vahan=date_vahan-timedelta(days=90)
            temp_df=merged_df[(merged_df['Vehicle_Category']=='HGV') & (merged_df['Maker_Clean']=='VE COMMERCIAL VEHICLES') & (merged_df['Relevant_Date']==date)]
            # temp_df.to_csv('eicher1.csv')
            # merged_df=merged_df[merged_df['Relevant_Date']>=date_vahan]
            # date_vahan = date_vahan.strftime('%Y-%m-%d')



            temp_df=merged_df[(merged_df['Vehicle_Category']=='HGV') & (merged_df['Maker_Clean']=='VE COMMERCIAL VEHICLES') & (merged_df['Relevant_Date']==date)]
            # temp_df.to_csv('eicher2.csv')
            print(temp_df.head(5))
            merged_df,unmapped = cleancompanies.comp_clean(merged_df, 'Maker_Clean', 'vahan', 'Maker_Clean_Name', table_name)
            merged_df.drop('Maker_Clean', axis = 1, inplace=True)
            merged_df.rename(columns = {'Maker_Clean_Name':'Maker_Clean'}, inplace = True)


            # query = "delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP where Relevant_Date>='" + date_vahan + "'"
            # # query = "delete from VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP"
            # connection.execute(query)
            # connection.execute('commit')

            # time.sleep(10)

            merged_df.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP",index = False,con = engine,if_exists = "append")

            # MySql_CH.ch_truncate_and_insert(table_name)
            # if len(unmapped) > 0:
            #     raise Exception(f'Company Clean Mapping not available for few companies for VAHAN_MAKER_VS_CATEGORY_VS_FUEL_STATE_LEVEL_MONTH_CLEAN_DATA_MP Please check GENERIC_COMPANY_UNMAPPED_TABLE for more details')


        else:
            print("Not today")





        log.job_end_log(table_name,job_start_time, no_of_ping)
#

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
