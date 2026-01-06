from selenium import webdriver
from selenium.webdriver.support.select import Select
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
import time
import re
import itertools
import requests
import sqlalchemy
from pandas.io import sql
import os
from bs4 import BeautifulSoup
import sys
import warnings
warnings.filterwarnings('ignore')
import csv
import calendar
import pdb
import json
from dateutil.relativedelta import relativedelta
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
import adqvest_db
import JobLogNew as log
from tqdm import tqdm

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    start_time = timeit.default_timer()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "RAIL DHRISTI"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        df=pd.read_sql("SELECT * FROM AdqvestDB.VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL", engine)
        print('READ')
        final_df_all=pd.DataFrame()
        final_df_tractors=pd.DataFrame()
        for rto in tqdm(df['RTO_Office_Raw'].unique()):
            temp_df=df[df['RTO_Office_Raw']==rto]
            for date in temp_df['Relevant_Date'].unique():
                temp_df1=temp_df[temp_df['Relevant_Date']==date]
                a=[x - temp_df1.index[i - 1] for i, x in enumerate(temp_df1.index)][1:]
                b=np.array(a)
                try:
                    temp_df2=temp_df1.iloc[np.where(b> 1)[0][0]+1:]
                    combined=temp_df1.iloc[:np.where(b> 1)[0][0]+1]
                    #combined = temp_df1.append(temp_df2)
                    #combined=combined[~combined.index.duplicated(keep=False)]
                    final_df_tractors=pd.concat([final_df_tractors,temp_df2])
                    final_df_all=pd.concat([final_df_all,combined])
                    #print('yo')
                except:
                    final_df_all=pd.concat([final_df_all,temp_df1])
                    #print('yoyo')

        final_df_all.to_csv('VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_CHECK.csv')
        final_df_tractors.to_csv('VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_TRACTORS.csv')

        for date in final_df_all['Relevant_Date'].unique():
            temp_df=final_df_all[final_df_all['Relevant_Date']==date]
            temp_df.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_CHECK",con = engine,index = False,if_exists = 'append')

        for date in final_df_tractors['Relevant_Date'].unique():
            temp_df=final_df_tractors[final_df_tractors['Relevant_Date']==date]
            temp_df.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_TRACTORS",con = engine,index = False,if_exists = 'append')

        #final_df_all.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_CHECK",con = engine,index = False,if_exists = 'append')
        #final_df_tractors.to_sql(name = "VAHAN_MAKER_VS_CATEGORY_VS_RTO_LEVEL_DATA_FINAL_TRACTORS",con = engine,index = False,if_exists = 'append')

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
