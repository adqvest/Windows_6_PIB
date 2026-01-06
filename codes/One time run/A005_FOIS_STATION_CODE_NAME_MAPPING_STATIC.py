# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 15:15:19 2024

@author: Santonu
"""
import numpy as np
import pandas as pd
import re
import os
import sys
import warnings
import boto3
import glob
from pytz import timezone
import requests
from bs4 import BeautifulSoup

#%%
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')


import sqlalchemy
import camelot
from dateutil import parser
import JobLogNew as log
import adqvest_db
import adqvest_s3
import ClickHouse_db
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

import datetime
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
#%%
def clean_location(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'_',' ',text) # replace & with space
    print(text)
    # text = re.sub(r'.',' ',text) # replace & with space

    # text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = text.upper()
    return text

def clean_location1(location):
    cleaned_string = re.sub(r'\s*\.\s*', '. ', location).strip()
    cleaned_string = re.sub(r'\s+', ' ', cleaned_string)
    unique_parts = list(dict.fromkeys([part.strip() for part in cleaned_string.split(',')]))
    print(unique_parts)
    return ', '.join(unique_parts)

def get_data(stn):
      headers = {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cache-Control': 'max-age=0',
      'Connection': 'keep-alive',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': 'https://www.fois.indianrail.gov.in',
      'Referer': 'https://www.fois.indianrail.gov.in/FOISWebPortal/pages/FWP_SttnHelp.jsp',
      'Sec-Fetch-Dest': 'iframe',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
      'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Microsoft Edge";v="126"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Windows"',
  }
      data = {
      'Qry': 'CRT_LOCN',
      'txtSttnCode': stn,
      'txtSttnName': '',
      'hidreset': 'A',
      }
  
      r = requests.post(
          'https://www.fois.indianrail.gov.in/FOISWebPortal/FWP_SttnHelp',
         
          headers=headers,
          data=data,
      )
      
      df=pd.read_html(r.content)
      return df

   
def process_df(df,stn):
    df=[i for i in df if len(i)>3][0]
    name=df.columns[0]
    df.columns=[i for i in range(0,len(df.columns))]
    df=df.T
    df.columns=df.iloc[0,:]
    df=df.iloc[1:,:]
    df.reset_index(drop=True,inplace=True)
    df['Section']= df['Section/ Division/ Zone'].apply(lambda x:x.split('/')[0])
    df['Division']= df['Section/ Division/ Zone'].apply(lambda x:x.split('/')[1])
    df['Zone']= df['Section/ Division/ Zone'].apply(lambda x:x.split('/')[2])
    

    df['Division']=df['Division'].apply(lambda x:re.findall(r'\((.*?)\)', x)[0])
    df['Division_Code']=df['Division'].apply(lambda x:x.split('-')[0])
    df['Division_Name']=df['Division'].apply(lambda x:x.split('-')[1])
    
    
    df.drop(columns=['Division','Section/ Division/ Zone'],inplace=True)
    df.columns=[i.replace(' ','_').title() for i in df.columns]
    
    df['Stn_Name']=name
    df['Stn_Name_Clean']=df['Stn_Name'].apply(lambda x:re.findall(r'\((.*?\))', x)[0].split('(')[0])
    df['Station_Code']=stn
    
    df['Serving_Station']=df['Numeric_Code'].apply(lambda x:re.findall('[A-Za-z]+', x.replace('\n','')) if pd.notnull(x) else x)
    df['Serving_Station']=df['Serving_Station'].apply(lambda x:' '.join(x).upper().replace('SERVING STATION','').strip() if pd.notnull(x) else x)
    df['Serving_Station']=np.where(df['Serving_Station']=='',np.nan,df['Serving_Station'])

    
    df['Numeric_Code']=df['Numeric_Code'].str.extract('(\d+.\d+|\d+)', expand=False)
    df['Relevant_Date'] =today.date()
    df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
    
    return df
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name, data,ty:str, db: list):

    query=f"select distinct Relevant_Date as Relevant_Date from {table_name} where Station_Code='{ty}';"
    # query=f"select distinct Relevant_Date as Relevant_Date from {table_name};"
    print(query)
    db_max_date = pd.read_sql(query,engine)   
    data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], format='%Y-%m-%d')

    data=data.loc[data['Station_Code']==ty]
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    # print(data.info())
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        # print(f'Done for --->{db_max_date,ty}')
        print(data.info())
        
#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
    # os.chdir('/home/ubuntu/AdQvestDir')
   
    #     #****   Date Time *****
   

    job_start_time = datetime.datetime.now(india_time)
    table_name = "FOIS_STATION_CODE_NAME_MAPPING_STATIC"
    scheduler = ''
    no_of_ping = 0

    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)
            
        #%%
        # station_codes_col = pd.read_sql("select distinct Station_Code as Station_Code  from FOIS_STATION_CODE_NAME_MAPPING_STATIC", engine)
        # station_code=[i for i in station_code if i not in station_codes_col.Station_Code.to_list()]
        # station_code=station_code+serving_station

        #%%
        url='https://www.fois.indianrail.gov.in/FOISWebPortal/pages/FWP_SttnHelp.jsp'
      
        
        #%%
        ##------Layer 1:from FOIS_MI_DAILY_DATA collect all Station code information----------------------------
        station_codes1 = pd.read_sql("select distinct Sttn_From as Sttn_From  from FOIS_MI_DAILY_DATA", engine)
        station_codes2 = pd.read_sql("select distinct Dstn  as Dstn from FOIS_MI_DAILY_DATA", engine)
        station_code=list(set(station_codes1.Sttn_From.to_list()+station_codes2.Dstn.to_list()))
        station_code=[i for i in station_code if i!='']
        
        final_df=pd.DataFrame()
        for stn in station_code:
            print(f'Station code--->{stn}---')
            
            # stn='GBG'
            try:
                df=get_data(stn)
                df=process_df(df,stn)
                print(df)
                final_df=pd.concat([final_df,df])
            except:
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg)
       

        if len(final_df)>0:
             final_df=drop_duplicates(final_df)
             for j in final_df.Station_Code.unique().tolist():
                  Upload_Data('FOIS_STATION_CODE_NAME_MAPPING_STATIC',final_df,j,['MySQL'])  
        
        #%%
        ##------Layer 2:from FOIS_STATION_CODE_NAME_MAPPING_STATIC collect all Serving_Station information----------------------------
        serving_station_col = pd.read_sql("select distinct Serving_Station as Serving_Station  from FOIS_STATION_CODE_NAME_MAPPING_STATIC where Serving_Station not in (select distinct Station_Code as Station_Code  from FOIS_STATION_CODE_NAME_MAPPING_STATIC)", engine)
        station_code=[i for i in serving_station_col.Serving_Station.to_list() if i not in ['',None]]
        # serving_station=[ i for i in serving_station if i not in station_codes_col.Station_Code.to_list()]

        
        final_df=pd.DataFrame()
        for stn in station_code:
            print(f'Station code--->{stn}---')
            
            # stn='GBG'
            try:
                df=get_data(stn)
                df=process_df(df,stn)
                print(df)
                final_df=pd.concat([final_df,df])
            except:
                error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                print(error_msg)
       

        if len(final_df)>0:
             final_df=drop_duplicates(final_df)
             for j in final_df.Station_Code.unique().tolist():
                  Upload_Data('FOIS_STATION_CODE_NAME_MAPPING_STATIC',final_df,j,['MySQL'])  
          
                

        #%%
        df1 = pd.read_sql("Select Station_Code, Serving_Station, Stn_Name_Clean from AdqvestDB.FOIS_STATION_CODE_NAME_MAPPING_STATIC group by Station_Code;", engine)
        df2 = pd.read_sql("Select Station_Code as Serving_Station, Stn_Name_Clean as Serving_Station_Clean from AdqvestDB.FOIS_STATION_CODE_NAME_MAPPING_STATIC where Station_Code IN (Select  distinct Serving_Station from AdqvestDB.FOIS_STATION_CODE_NAME_MAPPING_STATIC) group by Station_Code;", engine)

        df3=pd.merge(df1,df2,on='Serving_Station',how='left')
        important_col=['Stn_Name_Clean','Serving_Station_Clean']
        df3[important_col]=df3[important_col].applymap(lambda x:clean_location(x.replace('.','').strip()) if pd.notnull(x) else x)
        
        df3['Stn_Name_Clean_With_Location']=np.where(df3['Serving_Station'].isna(),df3.Stn_Name_Clean,df3.Stn_Name_Clean+', '+df3.Serving_Station_Clean)
        df3['Stn_Name_Clean_With_Location']=np.where(df3['Stn_Name_Clean_With_Location'].isna(),df3.Stn_Name_Clean,df3['Stn_Name_Clean_With_Location'])
        df3['Stn_Name_Clean_With_Location'] = df3['Stn_Name_Clean_With_Location'].apply(lambda x: clean_location1(x))
        #%%
        update_query = f"""DELETE from FOIS_STATION_Static_temp_santonu"""
        engine.execute(update_query)
        df3.to_sql('FOIS_STATION_Static_temp_santonu', con=engine, if_exists='append', index=False)
        #%%
        query1="""update AdqvestDB.FOIS_STATION_CODE_NAME_MAPPING_STATIC t1 inner join (Select * from AdqvestDB.FOIS_STATION_Static_temp_santonu) t2
                 on t2.Station_Code = t1.Station_Code set t1.Stn_Name_Clean_With_Location = t2.Stn_Name_Clean_With_Location;"""

        engine.execute(query1)
        print('All Done')
        
        engine.execute("Drop table FOIS_STATION_Static_temp_santonu")
        #%%
        
        
        log.job_end_log(table_name, job_start_time, no_of_ping)


    except:
            try:
                connection.close()
            except:
                pass
            error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
            error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
            print(error_msg)
       
            log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)

        
if (__name__ == '__main__'):
    run_program(run_by='manual')

