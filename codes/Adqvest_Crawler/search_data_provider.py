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
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_Main/Adqvest_Function')
# sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Windows_3/Adqvest_Function')
import adqvest_db
import ClickHouse_db

# working_dir = r"C:\Users\Santonu\Adqvest_Crawler"
working_dir=r"C:\Users\Administrator\AdQvestDir\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_Main\codes\Adqvest_Crawler"
# working_dir=r"C:\Users\Administrator\AdQvestDir\Windows_3\codes\Adqvest_Crawler"

os.chdir(working_dir)

@dataclass
class Connections:
    mysql: any = None
    clickhouse: any = None
    
    def __post_init__(self):
        if self.mysql is None:
            self.mysql = adqvest_db.db_conn()
        if self.clickhouse is None:
            self.clickhouse = ClickHouse_db.db_conn()


class InputDataSource:
    # def __init__(self):
    
    @classmethod
    async def get_input_data(cls,input_type=None,brand=None):
        try:
            conn = Connections()
            query = """
                    SELECT UPPER(City) AS City,Pincode, count(*) AS Cnt, RANK() OVER (ORDER BY count(*) DESC) AS City_Rank
                    FROM AdqvestDB.STORE_LOCATOR_WEEKLY_CLEAN_DATA_NEW DATA 
                    WHERE City IS NOT NULL AND City != '' and Pincode!='' and Pincode is not Null and Relevant_Date >= date_sub(Month,8,(SELECT MAX(Relevant_Date) FROM STORE_LOCATOR_WEEKLY_CLEAN_DATA_NEW)) 
                    GROUP BY City,Pincode
                    ORDER BY Cnt DESC;
                    """
            data = conn.clickhouse.execute(query)
            data = pd.DataFrame(data, columns=["City", "Pincode", "Cnt", "City_Rank"])
            if len(input_type)==1:
                if re.search(r"pincode",input_type[0],flags=re.IGNORECASE):
                    req_col=('Pincode','Pincode_Rank')
                    input_df=data.drop_duplicates(subset='Pincode', keep='first')
                    input_df = input_df.sort_values(by='City_Rank', ascending=True)
                    
                    if brand in ['Asian Paints','Apollo Pharmacy','Berger Paints','Caratlane','Manyavar']:
                       input_df = pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and {req_col[0]} is not NULL;",conn.mysql)
                       

                    elif len(input_df)==0:
                        # input_df=pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where {req_col[0]} is not NULL group by {req_col[1]} order by {req_col[1]} asc;",conn.mysql)
                        # input_df=pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where {req_col[0]} is not NULL group by {req_col[1]} order by {req_col[1]} asc;",conn.mysql)
                        input_df = pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}';",conn.mysql)


                elif re.search(r"city",input_type[0],flags=re.IGNORECASE):
                   req_col=('City','City_Rank')
                   # input_df=pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where {req_col[0]} is not NULL group by {req_col[1]} order by {req_col[1]} asc;",conn.mysql)
                   input_df=data.drop_duplicates(subset='City', keep='first')
                   input_df = input_df.sort_values(by='City_Rank', ascending=True)

                   if brand in ['Medplus']:
                       input_df = pd.read_sql(f"select Distinct {req_col[0]} from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and {req_col[0]} is not NULL;",conn.mysql)
                       # print(input_df)


                elif re.search(r"district",input_type[0],flags=re.IGNORECASE):
                   req_col=[input_type[0]]
                   input_df=pd.read_sql(f"select Distinct {input_type[0]} from INDIA_POST_PINCODE_DATA_ONE_TIME where {input_type[0]} is not NULL group by {input_type[0]} order by {input_type[0]} asc;",conn.mysql)
                
                elif re.search(r"state",input_type[0],flags=re.IGNORECASE):
                   req_col=[input_type[0]]
                   input_df=pd.read_sql(f"select Distinct {input_type[0]} from INDIA_POST_PINCODE_DATA_ONE_TIME where {input_type[0]} is not NULL group by {input_type[0]} order by {input_type[0]} asc;",conn.mysql)

                os.chdir(working_dir)
                return input_df[req_col[0]].to_list()
    
            if len(input_type)>1:
                column_string = ", ".join(input_type)
                query = f"select Distinct {column_string} from STORE_LOCATOR_WEEKLY_DATA_INPUT_TABLE_STATIC where State is not NULl or City is not Null group by Pincode_Rank order by Pincode_Rank asc;"
                input_df=pd.read_sql(query,conn.mysql)
                input_df['Serach']=input_df[input_type[0]]+' '+input_df[input_type[1]]+' '+input_df[input_type[2]]
                os.chdir(working_dir)
                return input_df['Serach'].to_list()
    
            
        except Exception as e:
            print(f"Error processing: {cls._log_strategy_error('_get_input_data')}")
    
    @staticmethod
    async def Upload_Data(brand: str,data,**kwargs):
        conn = Connections()
        if kwargs:
            if kwargs['Sub_Category_1']:
                db_max_date = pd.read_sql(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_1='{kwargs['Sub_Category_1']}';",conn.mysql)["Max"][0]
                
                
            elif kwargs['Sub_Category_2']:
                db_max_date = pd.read_sql(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_2='{kwargs['Sub_Category_2']}';",conn.mysql)["Max"][0]

            else:
                db_max_date = pd.read_sql(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee';",conn.mysql)["Max"][0]
    
        
        data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], format='%Y-%m-%d')
        data['Relevant_Date']=data['Relevant_Date'].dt.date

        
        if db_max_date==None:
               db_max_date=yesterday.date() 
               data.to_sql('STORE_LOCATOR_WEEKLY_DATA', con=conn.mysql, if_exists='append', index=False)
        else:
            data=data.loc[data['Relevant_Date'] >= db_max_date]
            data.to_sql('STORE_LOCATOR_WEEKLY_DATA', con=conn.mysql, if_exists='append', index=False)

        print(data.info())
        #-----------------------------------------------------------------------------------------------------------------------------------------
        if kwargs:
            if kwargs['Sub_Category_1']:
                ch_db_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_1='{kwargs['Sub_Category_1']}';")
                df = pd.read_sql(f"select * from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_1='{kwargs['Sub_Category_1']}' and Relevant_Date >'{str([a_tuple[0] for a_tuple in ch_db_max_date][0])}';",conn.mysql)
                
            elif kwargs['Sub_Category_2']:
                ch_db_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_2='{kwargs['Sub_Category_2']}';")
                df = pd.read_sql(f"select * from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Sub_Category_2='{kwargs['Sub_Category_2']}' and Relevant_Date >'{str([a_tuple[0] for a_tuple in ch_db_max_date][0])}';",conn.mysql)

            else:
                ch_db_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee';")
                df = pd.read_sql(f"select * from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}' and Crawler='Crawlee' and Relevant_Date >'{str([a_tuple[0] for a_tuple in ch_db_max_date][0])}';",conn.mysql)

        #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # click_max_date = conn.clickhouse.execute(f"select max(Relevant_Date) from STORE_LOCATOR_WEEKLY_DATA where Brand = '{brand}'")
        # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])

        # query =f"select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand ='{brand}' and Relevant_Date >'{click_max_date}'"
        # df = pd.read_sql(query, conn.mysql)
        conn.clickhouse.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
        print(f'To CH: {len(df)} rows')
        print(f"Data uploaded to SQL & Clickhouse------>{brand}")
        os.chdir(working_dir)
          
    @classmethod
    async def get_data_info(cls, brand: str=None, monthly_info=False,weekly_info=False,not_collecting=False,**kwargs):
        try:
            conn = Connections()
            query="""SELECT Brand, Relevant_Date,Sub_Category_1,Sub_Category_2,count(*) as Week_Count,Rank
                    FROM (
                        SELECT 
                            Brand,Relevant_Date,Sub_Category_1,Sub_Category_2,
                            DENSE_RANK() OVER (PARTITION BY Brand ORDER BY Relevant_Date DESC) as Rank
                        FROM AdqvestDB.STORE_LOCATOR_WEEKLY_DATA
                        WHERE Brand IS NOT NULL AND Brand != '' and Crawler='Crawlee'
                    ) ranked
                    group by Brand, Relevant_Date,Sub_Category_1,Sub_Category_2,Rank
                    ORDER BY Brand,Relevant_Date DESC;"""

            data = conn.clickhouse.execute(query)
            df = pd.DataFrame(data, columns=["Brand", "Relevant_Date","Sub_Category_1","Sub_Category_2", "Week_Count", "Rank"])

            df_prev_col=df[df['Rank']==2]
            df_cur=df[df['Rank']==1]
            df_cur=df_cur.merge(df_prev_col,on=['Brand',"Sub_Category_1","Sub_Category_2"])
            df_cur['Pct_Change_Count']=round(((df_cur['Week_Count_x']/df_cur['Week_Count_y'])-1)*100)
            
            df_cur=df_cur[['Brand', 'Relevant_Date_x',"Sub_Category_1","Sub_Category_2",'Week_Count_x','Week_Count_y', 'Pct_Change_Count']]
            df_cur.columns=['Brand','Relevant_Date',"Sub_Category_1","Sub_Category_2",'Current_Week_Count','Prev_Week_Count','Pct_Change_Count']
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
            
            df_monthly=df_monthly[df_monthly['Rank']==1].merge(df_monthly[df_monthly['Rank']==2],on=['Brand',"Sub_Category_1","Sub_Category_2"])
            df_monthly['Pct_Change_Count_Month']=round(((df_monthly['Week_Count_x']/df_monthly['Week_Count_y'])-1)*100)
            
            df_monthly=df_monthly[['Brand', 'Relevant_Date_x',"Sub_Category_1","Sub_Category_2", 'Week_Count_x','Week_Count_y', 'Pct_Change_Count_Month']]
            df_monthly.columns=['Brand','Relevant_Date',"Sub_Category_1","Sub_Category_2",'Current_Month_Count','Prev_Month_Count','Pct_Change_Count_Month']
            df_monthly=df_monthly[df_monthly['Relevant_Date']>today.date()-timedelta(32)]
            df_monthly=df_monthly.sort_values('Pct_Change_Count_Month',ascending=False)
            df_monthly=df_monthly.sort_values('Relevant_Date',ascending=False)
            os.chdir(working_dir)
            if brand!=None:
                # print(df_cur)
                if kwargs['Sub_Category_1']:
                   df_cur=df_cur[df_cur['Sub_Category_1']==kwargs['Sub_Category_1']]

                if kwargs['Sub_Category_2']:
                   df_cur=df_cur[df_cur['Sub_Category_2']==kwargs['Sub_Category_2']]

                df_cur=df_cur[df_cur['Brand']==brand]

                df_cur=df_cur.sort_values('Relevant_Date',ascending=False)
                return df_cur
            
            elif monthly_info:
                df_monthly=df_monthly[df_monthly['Pct_Change_Count_Month']!=0]
                return df_monthly
            
            elif weekly_info:
                df_cur=df_cur[df_cur['Pct_Change_Count']!=0]
                return df_cur

            elif not_collecting:
                 df_cur['Valid']=df_cur['Relevant_Date'].apply(lambda x: 'Yes' if ((today.date() - x) > timedelta(8)) else 'No')
                 df_cur=df_cur[df_cur['Valid']=='Yes']
                 df_cur=df_cur.sort_values('Relevant_Date',ascending=False)
                 return df_cur
        except:
            print(f"Error processing: {cls._log_strategy_error('_get_data_info')}")
    
    @staticmethod
    def _log_strategy_error(strategy_name: str, context= None, additional_info = None) -> str:
    
        # Get exception information
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(exc_obj)
        line_no = exc_tb.tb_lineno if exc_tb else 'unknown'
        
        formatted_error = f"{'='*60}\n"
        formatted_error += f"ERROR IN STRATEGY:{strategy_name}\n"
        
        # Exception details
        formatted_error += f"EXCEPTION DETAILS:\n"
        formatted_error += f"Error Type: {exc_type.__name__ if exc_type else 'Unknown'}\n"
        formatted_error += f"Error Message: {error_msg}\n"
        formatted_error += f"Line Number: {line_no}\n"
        
        # Context information (if provided)
        if context:
            formatted_error += f"\nCOLLECTION CONTEXT:\n"
            
            if context.state:
                formatted_error += f"State: {context.state}\n"
            
            if context.city:
                formatted_error += f"City: {context.city}\n"
            
            if context.locality:
                formatted_error += f"Locality: {context.locality}\n"
            
            if context.sub_brand:
                formatted_error += f"Sub Brand: {context.sub_brand}\n"
            
            if hasattr(context, 'page') and context.page:
                try:
                    current_url = getattr(context.page, 'url', 'Unknown')
                    formatted_error += f"Current URL: {current_url}\n"
                except:
                    formatted_error += f"Current URL: Unable to retrieve\n"
            
            # Add location hierarchy summary
            location_parts = []
            if context.state: location_parts.append(context.state)
            if context.city: location_parts.append(context.city)
            if context.locality: location_parts.append(context.locality)
            
            if location_parts:
                formatted_error += f"Location Path: {' → '.join(location_parts)}\n"
        
        else:
            formatted_error += f"NO CONTEXT PROVIDED\n"
        
        # Additional information
        if additional_info:
            formatted_error += f"\nADDITIONAL INFO:\n"
            formatted_error += f"{additional_info}\n"
        
        formatted_error += f"{'='*60}\n"
        print(formatted_error)
        return formatted_error