# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 14:02:29 2025
@author: Santonu
"""

import pandas as pd
import os
import asyncio
import re
from dataclasses import dataclass
import logging
import sys

from datetime import timedelta  
from  datetime import datetime
from pytz import timezone

india_time = timezone('Asia/Kolkata')
today      = datetime.now()
days       = timedelta(1)
yesterday = today - days

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db

working_dir=r"C:\Users\Santonu\Adqvest_Crawler"
# # working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"

# os.chdir(working_dir)
@dataclass
class Connections:
    mysql: any = None
    clickhouse: any = None
    
    def __post_init__(self):
        if self.mysql is None:
            self.mysql = adqvest_db.db_conn()
        if self.clickhouse is None:
            self.clickhouse = ClickHouse_db.db_conn()


class Table_Info:
    # def __init__(self):
    
    @classmethod
    async def get_input_data(cls,input_type=None):
        try:
            conn = Connections()
            if len(input_type)==1:
                if re.search(r"pincode",input_type[0],flags=re.IGNORECASE):
                   req_col=('Pincode','Pincode_Rank')
    
                elif re.search(r"city",input_type[0],flags=re.IGNORECASE):
                   req_col=('City','City_Rank')
    
                input_df=pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC group by {req_col[1]} order by {req_col[1]} asc;",conn.mysql)
                # os.chdir(self.working_dir)
                return input_df[req_col[0]].to_list()
    
    
            if len(input_type)>1:
                column_string = ", ".join(input_type)
                query = f"select Distinct {column_string} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where State is not NULl or City is not Null group by Pincode_Rank order by Pincode_Rank asc;"
                input_df=pd.read_sql(query,conn.mysql)
                input_df['Serach']=input_df[input_type[0]]+' '+input_df[input_type[1]]+' '+input_df[input_type[2]]
                # os.chdir(self.working_dir)
                return input_df['Serach'].to_list()
    
            
        except Exception as e:
            print(f"Error processing: {cls._log_strategy_error('_get_input_data')}")
    
    @staticmethod
    async def Upload_Data(brand: str,data):
      
        conn = Connections()
        db_max_date = pd.read_sql(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}';",conn.mysql)["Max"][0]
        if db_max_date==None:
           db_max_date=yesterday.date()

        data=data.loc[data['Relevant_Date'] > db_max_date]
        data.to_sql('STORE_LOCATOR_WEEKLY_DATA', con=conn.mysql, if_exists='append', index=False)
        print(data.info())

        click_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}'")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

        query =f"select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand ='{brand}' and Relevant_Date >'{click_max_date}'"
        df = pd.read_sql(query, conn.mysql)
        conn.clickhouse.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
        print(f'To CH: {len(df)} rows')
        print(f"Data uploaded to SQL & Clickhouse------>{brand}")
        
       
    @classmethod
    async def get_data_info(cls, brand: str=None, monthly_info=False,weekly_info=False):
        try:
            conn = Connections()
            query="""SELECT Brand, Relevant_Date,count(*) as Week_Count,Rank
                    FROM (
                        SELECT 
                            Brand,
                            Relevant_Date,
                            DENSE_RANK() OVER (PARTITION BY Brand ORDER BY Relevant_Date DESC) as Rank
                        FROM AdqvestDB.STORE_LOCATOR_WEEKLY_DATA
                        WHERE Brand IS NOT NULL AND Brand != ''
                    ) ranked
                    group by Brand, Relevant_Date,Rank
                    ORDER BY Brand,Relevant_Date DESC;
                            """

            data = conn.clickhouse.execute(query)
            df = pd.DataFrame(data, columns=["Brand", "Relevant_Date", "Week_Count", "Rank"])

            df_prev_col=df[df['Rank']==2]
            df_cur=df[df['Rank']==1]
            df_cur=df_cur.merge(df_prev_col,on='Brand')
            df_cur['Pct_Change_Count']=round(((df_cur['Week_Count_x']/df_cur['Week_Count_y'])-1)*100)
            
            df_cur=df_cur[['Brand', 'Relevant_Date_x', 'Week_Count_x','Week_Count_y', 'Pct_Change_Count']]
            df_cur.columns=['Brand','Relevant_Date','Current_Week_Count','Prev_Week_Count','Pct_Change_Count']
            df_cur=df_cur[df_cur['Pct_Change_Count']!=0]
            df_cur=df_cur.sort_values('Pct_Change_Count',ascending=False)
            #%%
            df_monthly=df.copy()
            df_monthly['Relevant_Date'] = pd.to_datetime(df_monthly['Relevant_Date'])
            df_monthly['YearMonth'] = df_monthly['Relevant_Date'].dt.to_period('M').dt.to_timestamp()
            # Group by brand and year-month
            df_monthly = (df_monthly.groupby(['Brand', 'YearMonth'])['Relevant_Date'].max().reset_index())
            df_monthly['Relevant_Date']=df_monthly['Relevant_Date'].dt.date
            df_monthly=df.merge(df_monthly,on=['Brand','Relevant_Date'])
            df_monthly['Rank'] = df_monthly.groupby('Brand')['Relevant_Date'].rank(ascending=False)
            
            df_monthly=df_monthly[df_monthly['Rank']==1].merge(df_monthly[df_monthly['Rank']==2],on='Brand')
            df_monthly['Pct_Change_Count_Month']=round(((df_monthly['Week_Count_x']/df_monthly['Week_Count_y'])-1)*100)
            
            df_monthly=df_monthly[['Brand', 'Relevant_Date_x', 'Week_Count_x','Week_Count_y', 'Pct_Change_Count_Month']]
            df_monthly.columns=['Brand','Relevant_Date','Current_Month_Count','Prev_Month_Count','Pct_Change_Count_Month']
            df_monthly=df_monthly[df_monthly['Relevant_Date']>today.date()-timedelta(32)]
            df_monthly=df_monthly[df_monthly['Pct_Change_Count_Month']!=0]
            df_monthly=df_monthly.sort_values('Pct_Change_Count_Month',ascending=False)
            
            if brand!=None:
                df_cur=df_cur[df_cur['Brand']==brand]
                return df_cur
            
            elif monthly_info:
                return df_monthly
            
            elif weekly_info:
                return df_cur
            
        except:
            print(f"Error processing: {cls._log_strategy_error('_get_data_info')}")
    
    @staticmethod
    def _log_strategy_error(strategy_name:str):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        print(f"Error in strategy '{strategy_name}':\nError Message: {error_msg}\nLine: {line_no}\n")
            