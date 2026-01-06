import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import os
import io
import re
import ast
import csv
import time
import glob
import json
import random
import zipfile
import calendar
import warnings
import requests
import sqlalchemy
import xlsxwriter
import adqvest_db
import ClickHouse_db
import JobLogNew as log

import numpy as np
import pandas as pd
import datetime as datetime

from time import sleep
from pandas.io import sql
from pytz import timezone
from dateutil import parser

from adqvest_robotstxt import Robots
robot = Robots(__file__)

import psycopg2
import math

postgres_test_con = psycopg2.connect(
host="ec2-3-108-253-129.ap-south-1.compute.amazonaws.com",
database="adqvest",
user="postgres",
password="@Thur&TPa@##123",
port=5432)
cursor_test = postgres_test_con.cursor()

postgres_prod_con = psycopg2.connect(
host="3.109.104.45",
database="adqvest_thurro",
user="postgres",
password="@Thur&PRod@##123",
port=5432)
cursor_prod = postgres_prod_con.cursor()

os.chdir('C:/Users/Administrator/AdQvestDir')
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

def AD(Id):
    flag_AD = 0
    try:
        print("Checking For Widget ID :",str(Id))
        # DATA PREPERATION

        query = "select query_param FROM widgets_new WHERE widget_id in ('"+str(Id)+"')"
        df = pd.read_sql(query, postgres_test_con)
        
        x = str(df["query_param"][0])
        if 'vbar' in str(x):
            return flag_AD

        try :
            x = str(df["query_param"][0])
            x = x.split("'query': '")[1]
            x = x.split("',")[0]
            x = x.replace("@@period@@","12")
        except:
            x = str(df["query_param"][0])
            x = x.split('"')[1]
            x = x.replace("@@period@@","12")
            x = x.replace("@@periodQtr@@","12")

        datax = client.execute(x)

        try:
            datax = pd.DataFrame(datax)
            datax.columns = ["Date","Value","Value2"]
        except:
            datax = pd.DataFrame(datax)
            datax.columns = ["Date","Value"]

        datax = datax.loc[datax['Value'] != np.inf]
        datax = datax.loc[datax['Value'] != np.nan]
        datax = datax.loc[datax['Value'].isna() == False]
        #datax["Value"] = abs(datax['Value'])
        
        datax["Date"] = [pd.to_datetime(x.strftime("%Y-%m-%d %H:%M:%S")) for x in datax["Date"]]
        datax = datax.set_index('Date')
        
        # ANAMOLY DETECTION
        
        flag = 0
        flag1 = 4
            
        # Inter Quartile Range Anomaly Detection
        
        try :
            
            from adtk.detector import InterQuartileRangeAD
            iqr_ad = InterQuartileRangeAD(c=2.5) # higher the value the less sensitive it is
            anomalies = iqr_ad.fit_detect(datax)
            
            anomalies = anomalies.loc[anomalies["Value"] == True]
            anomalies['Date_Anomalies'] = anomalies.index
            anomalies = anomalies.loc[anomalies["Date_Anomalies"] >= '2023-06-01']
            
            if anomalies.empty:
                ("NO ANOMALIES IN IQR")
            else:
                flag += 1
            
        except :
            
            flag1 -= 1
            print("PROBLEM IN IQR")

        # Generalized Estimated Standard Deviation Anomaly Detection
        
        try :
            
            from adtk.detector import GeneralizedESDTestAD
            esd_ad = GeneralizedESDTestAD(alpha=0.05)
            anomalies = esd_ad.fit_detect(datax)
            
            anomalies = anomalies.loc[anomalies["Value"] == True]
            anomalies['Date_Anomalies'] = anomalies.index
            anomalies = anomalies.loc[anomalies["Date_Anomalies"] >= '2023-06-01']
            
            if anomalies.empty:
                ("NO ANOMALIES IN GESD")
            else:
                flag += 1
        
        except :
            
            flag1 -= 1
            print("PROBLEM IN GESD")
            
            
        # Level Shift Anomaly Detection
        
        try :
            
            from adtk.detector import LevelShiftAD
            level_shift_ad = LevelShiftAD(c=1.0, side='both', window=1)
            anomalies = level_shift_ad.fit_detect(datax)
            
            anomalies = anomalies.loc[anomalies["Value"] == True]
            anomalies['Date_Anomalies'] = anomalies.index
            anomalies = anomalies.loc[anomalies["Date_Anomalies"] >= '2023-06-01']
            
            if anomalies.empty:
                ("NO ANOMALIES IN Level Shift")
            else:
                flag += 1
                
        except :
            
            flag1 -= 1
            print("PROBLEM IN Level Shift")
                
        # Volatility Shift Anomaly Detection
        
        try :
            
            from adtk.detector import VolatilityShiftAD
            volatility_shift_ad = VolatilityShiftAD(c=3.0, side='both', window=2)
            anomalies = volatility_shift_ad.fit_detect(datax)
            
            anomalies = anomalies.loc[anomalies["Value"] == True]
            anomalies['Date_Anomalies'] = anomalies.index
            anomalies = anomalies.loc[anomalies["Date_Anomalies"] >= '2023-06-01']
            
            if anomalies.empty:
                ("NO ANOMALIES IN Volatility Shift")
            else:
                flag += 1
        
        except :
            
            flag1 -= 1
            print("PROBLEM IN Volatility Shift")

                
        if flag == 0:
            print("NO ANOMALY")
            
        elif flag1 - flag <=1 :
            print(Id,"HAS ANOMALIES PRESENT")
            flag_AD = Id
            
        else :
    #             print(Id,"HAS AN ANOMALY")
    #             print("ANOMALIES IN",flag,"OUT OF",flag1,"DETECTORS")
            print("FEW ANOMALIES PRESENT")
        
    except:
        print("ERROR IN FUNCTION")

    print("--------------------------------------")
    return flag_AD

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    run_time = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'ANOMALY_DETECTION_ONE_TIME'

    postgres_test_con = psycopg2.connect(
    host="ec2-3-108-253-129.ap-south-1.compute.amazonaws.com",
    database="adqvest",
    user="postgres",
    password="@Thur&TPa@##123",
    port=5432)
    cursor_test = postgres_test_con.cursor()

    postgres_prod_con = psycopg2.connect(
    host="3.109.104.45",
    database="adqvest_thurro",
    user="postgres",
    password="@Thur&PRod@##123",
    port=5432)
    cursor_prod = postgres_prod_con.cursor()

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # SELECTING ALL THE REQUIRED CHART IDS

        query = "select widget_id FROM widgets_new WHERE chart_category NOT LIKE '%Mix%' AND mark_delete = 'False' order by chart_value"
        df = pd.read_sql(query, postgres_test_con)

        # RUNNING ANOMLAY FUNCTION

        today = datetime.datetime.now(india_time)
        print(pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S")))

        anomaly_list = list()
        i = 0
        for x in df["widget_id"]:
            an_list = AD(x)
            anomaly_list.append(an_list)
            anomaly_list = [i for i in anomaly_list if i != 0]
            i += 1
            print("Iteration No : ",i)
        today = datetime.datetime.now(india_time)
        print(pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S")))

        # NOW DELETING AND ADDING ALL ANOMALIES

        postgres_test_con = psycopg2.connect(
        host="ec2-3-108-253-129.ap-south-1.compute.amazonaws.com",
        database="adqvest",
        user="postgres",
        password="@Thur&TPa@##123",
        port=5432)
        cursor_test = postgres_test_con.cursor()

        postgres_prod_con = psycopg2.connect(
        host="3.109.104.45",
        database="adqvest_thurro",
        user="postgres",
        password="@Thur&PRod@##123",
        port=5432)
        cursor_prod = postgres_prod_con.cursor()

        query = "DELETE FROM dashboard_charts WHERE dashboard_id = '694';"
        cursor_test.execute(query)
        postgres_test_con.commit()

        i = 0
        for x in anomaly_list :
            query = "INSERT INTO dashboard_charts(dashboard_id, widget_id, created_by, seq) VALUES (694," +str(x)+ ", 269,"+str(i)+");"
            #print(query)
            cursor_test.execute(query)
            postgres_test_con.commit()
            i += 1

        query = "DELETE FROM anomaly_widgets WHERE widget_id > '1';"
        cursor_test.execute(query)
        postgres_test_con.commit()

        i = 0
        for x in anomaly_list :
            query = "INSERT INTO anomaly_widgets(widget_id) VALUES ('"+str(x)+"');"
            #print(query)
            cursor_test.execute(query)
            postgres_test_con.commit()
            print("INSERTED")
            i += 1
        
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except :
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
