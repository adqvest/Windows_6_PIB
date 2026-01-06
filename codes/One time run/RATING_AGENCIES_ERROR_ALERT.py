# -*- coding: utf-8 -*-
"""
Created on Thu Apr 14 13:53:50 2022

@author: Abdulmuizz
"""

from bs4 import BeautifulSoup
import PyPDF2
import pandas as pd
import numpy as np
import requests
import datetime as datetime
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
from pandas.core.common import flatten
import os
import re
import csv
import time
import io
import os
#os.chdir(r'D:\Adqvest\ncdex')
from sqlalchemy import insert
import sqlalchemy
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db


engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


def check_for_error(total_files, error_files, table_name):

    threshold = 0.05
    try:
        error_pct = error_files/total_files

        if error_pct > threshold:
            raise Exception(f'Errors more than 5% in {table_name}')
        else:
            print('Errors below 5%')
            return error_pct
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_type = 'ALERT'
        #stmt = insert('LOG_TABLE_NEW_TEST')
        query = "Insert into LOG_TABLE_NEW_TEST value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        my_data = (table_name, 'RATING_AGENCIES_ERROR_CHECK', '', 'Adqvest_Bot', str(today.strftime('%Y-%m-%d %H:%M:%S')), None,None,None,None,error_type, error_msg, None, None,'','','', str(today.strftime('%Y-%m-%d')), str(today.strftime('%Y-%m-%d %H:%M:%S')))
        print(query)
        connection.execute(query, my_data)
        connection.execute('commit')


#------------------------------------------ CARE RATINGS ------------------------------------------------------------------

care_table_name = 'CARE_RATINGS_DAILY_FILES_LINKS'

care_total_files = pd.read_sql("select count(*) as Total_files from CARE_RATINGS_DAILY_FILES_LINKS", engine)['Total_files'][0]

care_error_files = pd.read_sql("select count(*) as Error_files from CARE_RATINGS_DAILY_FILES_LINKS where Status_Ratings = 'Failed' and Is_Ratings_Table_Present = 'Yes'", engine)['Error_files'][0]

check_for_error(care_total_files, care_error_files, care_table_name)


#------------------------------------------ CRISIL RATINGS ------------------------------------------------------------------

crisil_table_name = 'CRISIL_FILES_LINKS_DAILY_DATA'

crisil_total_files = pd.read_sql("select count(*) as Total_files from CRISIL_FILES_LINKS_DAILY_DATA", engine)['Total_files'][0]

crisil_error_files = pd.read_sql("select count(*) as Error_files from CRISIL_FILES_LINKS_DAILY_DATA where Status_BLR = 'Failed' and Is_BLR_Table_Present = 'Yes'", engine)['Error_files'][0]

check_for_error(crisil_total_files, crisil_error_files, crisil_table_name)


#------------------------------------------ ICRA RATINGS ------------------------------------------------------------------

icra_table_name = 'ICRA_DAILY_FILES_LINKS'

icra_total_files = pd.read_sql("select count(*) as Total_files from ICRA_DAILY_FILES_LINKS", engine)['Total_files'][0]

icra_error_files = pd.read_sql("select count(*) as Error_files from ICRA_DAILY_FILES_LINKS where Data_Scrap_Status = 'No'", engine)['Error_files'][0]

check_for_error(icra_total_files, icra_error_files, icra_table_name)
