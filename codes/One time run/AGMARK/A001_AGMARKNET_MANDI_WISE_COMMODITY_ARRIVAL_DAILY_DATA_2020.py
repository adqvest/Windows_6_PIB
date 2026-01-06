# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 13:53:25 2025

@author: Santonu
"""

import re
import ssl
import sys
import time
import warnings
import os

import camelot
import numpy as np
import pandas as pd
import regex as re
import requests
from bs4 import BeautifulSoup
from pytz import timezone
from datetime import datetime as dt
import datetime
from pandas.tseries.offsets import MonthEnd
warnings.filterwarnings('ignore')
from datetime import timedelta
from fiscalyear import *
import fiscalyear
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from concurrent.futures import ThreadPoolExecutor, as_completed 
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')

import JobLogNew as log
import adqvest_db
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import MySql_To_Clickhouse as MySql_CH
# from  geoclean import geo_clean
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#     #****   Date Time *****

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

WORKING_TABLE='AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA'

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df


def Upload_Data_MySQL(df_out):
    df_out=drop_duplicates(df_out)
    chunk_size = 100000
    smaller_dfs = [df_out[i:i+chunk_size] for i in range(0, len(df_out), chunk_size)]
    for i in range(len(smaller_dfs)):
        print(i)
        dfx=smaller_dfs[i]
        dfx.to_sql(name="AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA_STAGING", con=engine, if_exists='append',index=False)
    
def update_ch_condition_based(table_name,year_mon):
    try:
        query = f"select * from AdqvestDB.AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA_STAGING where CONCAT(YEAR(Relevant_Date),'-',MONTH(Relevant_Date))='{year_mon}';"
        df = pd.read_sql(query,engine)
        
        chunk_size = 100000
        smaller_dfs = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
        for i in range(len(smaller_dfs)):
            print(i)
            dfx=smaller_dfs[i]
            client.execute("INSERT INTO AdqvestDB.AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA VALUES",dfx.values.tolist())
            print("Data uploded in Click_house")
        
        engine.execute(f"Delete from AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA_STAGING Where CONCAT(YEAR(Relevant_Date),'-',MONTH(Relevant_Date))='{year_mon}';")
        
    except:
         error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
         error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
         print(error_msg)

def fetch_one_date(date_str, df_mandi_info, headers):
    COLUMN_MAP = {
        "stateName": "State",
        "commodityName": "Product",
        "marketCenter": "Market",
        "arrivals": "Arrivals_String",
        "arrivals": "Arrivals",
        "grade":"Grade",
        "unitOfArrivals": "Unit_of_Arrivals",
        "variety": "Variety",
        "minimumPrice": "Minimum_Prices",
        "maximumPrice": "Maximum_Prices",
        "modalPrice": "Modal_Prices",
        "unitOfPrice": "Unit_of_Price",
    }
                    
    rows = []
    state_ids = sorted(df_mandi_info["State_Id"].unique())

    for st_id in state_ids:
        print(st_id)
        State_mandi = (
            df_mandi_info[df_mandi_info["State_Id"] == st_id]["Market_Id"]
            .astype(int)
            .tolist()
        )

        json_data = {
            "date": date_str,
            "stateIds": [int(st_id)],
            "marketIds": State_mandi,
            "includeExcel": False
        }

        try:
            r = requests.post(
                "https://api.agmarknet.gov.in/v1/prices-and-arrivals/market-report/daily",
                headers=headers,
                json=json_data,
                timeout=60
            )
            r.raise_for_status()
            markets = r.json().get("states", [{}])[0].get("markets", [])
            for mkt in markets:
                for variety in mkt.get("commodities", []):
                    if variety.get("data"):
                        df = pd.DataFrame(variety["data"])
                        df["commodityName"] = variety["commodityName"]
                        df["Commodity_Id"] = variety["commodityId"]
                        df["Total_Arrivals"] = variety["total_arrivals"]
                        df["Market_Id"] = mkt["marketId"]
                        df["State_Id"] = st_id
                        df["Relevant_Date"] = date_str
                        df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                        df.rename(columns=COLUMN_MAP,inplace=True)
                        rows.append(df)

        except Exception as e:
            print(f"{date_str} | State {st_id} | {e}")

    if not rows:
        print(f"No data for {date_str}")
        return

    day_df = pd.concat(rows, ignore_index=True)
    return day_df

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/Adqvest_Function')
    # os.chdir('C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
    # os.chdir('/home/ubuntu/AdQvestDir/Adqvest_Function')

    job_start_time = today
    table_name = WORKING_TABLE
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
            if(run_by == 'Adqvest_Bot'):
                log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
            else:
                log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            #%% 
            # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")
            url="https://covid19dashboard.mohfw.gov.in/"
            robot.add_link(url)
            
            # df_mandi_info=pd.read_sql("Select * from AGMARKNET_MANDI_WISE_STATE_DISTRICT_STATIC_TABLE_2",con=engine)
            df_mandi_info=pd.read_sql("Select * from AGMARKNET_ENAM_MANDI_STATE_DISTRICT_STATIC_TABLE where Source='AGMARKNET'",con=engine)

            df_group_info=pd.read_sql("Select * from AGMARKNET_COMMODITY_WISE_GROUPS_STATIC_TABLE",con=engine)
            # engine.execute('Delete from AGMARKNET_MANDI_WISE_COMMODITY_ARRIVAL_DAILY_DATA_STAGING')

            
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json',
                'origin': 'https://agmarknet.gov.in',
                'priority': 'u=1, i',
                'referer': 'https://agmarknet.gov.in/',
                'sec-ch-ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
            }
            COLUMN_MAP = {
                "stateName": "State",
                "commodityName": "Product",
                "marketCenter": "Market",
                "arrivals": "Arrivals",
                "grade":"Grade",
                "unitOfArrivals": "Unit_of_Arrivals",
                "variety": "Variety",
                "minimumPrice": "Minimum_Prices",
                "maximumPrice": "Maximum_Prices",
                "modalPrice": "Modal_Prices",
                "unitOfPrice": "Unit_of_Price",
            }

            
            #-----------------------------------------------------------------------------------------------
            START_DATE = "2018-01-01"
            END_DATE   = "2018-12-31"
            
            dates = (
                pd.date_range(start=pd.to_datetime(START_DATE, format='%Y-%m-%d').date(), 
                              end=pd.to_datetime(END_DATE, format='%Y-%m-%d').date(),
                              freq="D")
                  .strftime("%Y-%m-%d")
                  .tolist()[::-1]
            )
            
            month_groups = {}
            for d in dates:
                d=pd.to_datetime(d, format='%Y-%m-%d').date()
                key = d.strftime("%Y-%m")
                month_groups.setdefault(key, []).append(d.strftime("%Y-%m-%d"))
            #------------------------------------------------------------------------------------------------
            MAX_WORKERS = 5
            for ym, date_list in month_groups.items():
                print(f"\nProcessing month: {ym}")
                month_dfs = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {
                        executor.submit(fetch_one_date, d, df_mandi_info, headers): d
                        for d in date_list
                    }

                    for future in as_completed(futures):
                        df_day = future.result()
                        if df_day is not None and not df_day.empty:
                            month_dfs.append(df_day)

                if not month_dfs:
                    print(f"No data for {ym}")
                    continue

                month_df = pd.concat(month_dfs, ignore_index=True)
                month_df.rename(columns=COLUMN_MAP,inplace=True)
                
                month_df=month_df.merge(df_mandi_info[['Market_Id','District_Clean', 'State_Id', 'State_Name', 'State_Clean']],
                                        on=['State_Id','Market_Id'],how='left')
                
                month_df=month_df.merge(df_group_info[['Commodity_Id','Group']],on=['Commodity_Id'],how='left')
                month_df=month_df[['State_Name','State_Clean','District_Clean','Market','Group','Product','Variety',  
                                   'Arrivals', 'Total_Arrivals','Unit_of_Arrivals','Grade',
                                   'Minimum_Prices', 'Maximum_Prices', 'Modal_Prices', 'Unit_of_Price',
                                   'Commodity_Id', 'Market_Id', 'State_Id','Relevant_Date', 'Runtime']]
                
                month_df.rename(columns={'State_Name':'State'},inplace=True)
                Upload_Data_MySQL(month_df)
                update_ch_condition_based(WORKING_TABLE,ym)
                print(f"Uploaded {len(month_df):,} rows for {ym}")
            #%%
            log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
            try:
                connection.close()
            except:
                pass
            error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
            print(error_msg)

            log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
                run_program(run_by = 'manual')
#%%
