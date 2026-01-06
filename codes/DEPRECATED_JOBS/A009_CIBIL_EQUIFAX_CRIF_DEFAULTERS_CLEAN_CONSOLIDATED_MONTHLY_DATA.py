import pandas as pd
import datetime as datetime
import numpy as np
from pytz import timezone
import glob
import time
import re
import requests
import os
from bs4 import BeautifulSoup
import pdfplumber
import sys
import textdistance
import boto3
import warnings
warnings.filterwarnings('ignore')
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

#%%
# sys.path.insert(0, 'D:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
from Cleaner_cibil_crif_equifax import full_clean
from Cleaner_cibil_crif_equifax import clean_company
from Cleaner_cibil_crif_equifax import clean_location
from Cleaner_cibil_crif_equifax import clean_bnk_br_st_ad
#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df


def general_cleaning(df):
  df=df[pd.to_numeric(df['State'], errors='coerce').isna()]
  df=df[pd.to_numeric(df['Credit_Institution'], errors='coerce').isna()]
  df=df[pd.to_numeric(df['Branch'], errors='coerce').isna()]
  df=df[pd.to_numeric(df['Party'], errors='coerce').isna()]
  df=df[pd.to_numeric(df['Registered_Address'], errors='coerce').isna()]
  df.reset_index(drop=True,inplace=True)
  df['Outstanding_Amt_Lacs']=df['Outstanding_Amt_Lacs'].apply(lambda x:clean_values(str(x)))
  return df
      
my_dictionary={
     'lacs':{'cat':'25 Lacs and above'},
     'crore':{'cat':'1 Cr and above'}
     }
sources={'CIBIL':'CIBIL_DEFAULTERS_MONTHLY_RAW_DATA','EQUIFAX':'EQUIFAX_DEFAULTERS_MONTHLY_RAW_DATA','CRIF':'CRIF_DEFAULTERS_MONTHLY_RAW_DATA'}

def clean_values(x):
    
    if 'Cr' in x:
        x=float(str(x).replace(',','').replace('Cr',''))*100
    else:
        x=float(str(x).replace(',',''))
    return x

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)


    job_start_time = datetime.datetime.now(india_time)
    table_name = "CIBIL_EQUIFAX_CRIF_DEFAULTERS_CLEAN_CONSOLIDATED_MONTHLY_DATA"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        #%%
        os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
        #%%       
        
        #%%
        bank_lookup= pd.read_sql('Select distinct Credit_Institution,Credit_Institution_Clean from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MAPPING_STATIC where Credit_Institution is not null',engine)
        branch_lookup= pd.read_sql('Select distinct Branch,Branch_Clean from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MAPPING_STATIC where Branch is not null',engine)
        state_lookup= pd.read_sql('Select distinct State,State_Clean,State_Mapped from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MAPPING_STATIC where State is not null',engine)
        party_lookup= pd.read_sql('Select distinct Party,Party_Clean from CIBIL_EQUIFAX_CRIF_DEFAULTERS_MAPPING_STATIC where Party is not null',engine)


        for src,tbl in sources.items():
            for ct in my_dictionary.keys():
                # src='CIBIL'
                # ct='lacs'
                #%%
                max_dt_tbl = pd.read_sql(f"select max(Relevant_Date) as Relevant_Date from CIBIL_EQUIFAX_CRIF_DEFAULTERS_CLEAN_CONSOLIDATED_MONTHLY_DATA where Category ='{my_dictionary[ct]['cat']}'",engine)['Relevant_Date'][0]
    
                raw_df = pd.read_sql(f"select * from {sources[src]} where  Category ='{my_dictionary[ct]['cat']}' and Relevant_Date>'{max_dt_tbl}'",engine)
                raw_df['Source']=src
                raw_df = raw_df[['Credit_Institution', 'Branch','State','Party','Registered_Address','Outstanding_Amt_Lacs','Category', 'Relevant_Date', 'Runtime']]
    
                raw_df=drop_duplicates(raw_df)
                raw_df=general_cleaning(raw_df)
                
                raw_df=pd.merge(raw_df, bank_lookup[['Credit_Institution','Credit_Institution_Clean']],on='Credit_Institution',how='left')
                raw_df['Credit_Institution_Clean']=np.where((raw_df['Credit_Institution_Clean'].isna()),raw_df['Credit_Institution'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['Credit_Institution_Clean'])

                
                
                raw_df=pd.merge(raw_df, branch_lookup[['Branch','Branch_Clean']],on='Branch',how='left')
                raw_df['Branch_Clean']=np.where((raw_df['Branch_Clean'].isna()),raw_df['Branch'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['Branch_Clean'])
                
                raw_df=pd.merge(raw_df, state_lookup[['State','State_Clean','State_Mapped']],on='State',how='left')
                raw_df['State_Clean']=np.where((raw_df['State_Clean'].isna()),raw_df['State'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['State_Clean'])
                # raw_df['State_Mapped']=np.where((raw_df['State_Mapped'].isna()),raw_df['State_Clean'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['State_Mapped'])
    
    
                raw_df=pd.merge(raw_df, party_lookup[['Party','Party_Clean']],on='Party',how='left')
                raw_df['Party_Clean']=np.where((raw_df['Party_Clean'].isna()),raw_df['Party'].apply(lambda x:clean_bnk_br_st_ad(str(x))),raw_df['Party_Clean'])
                
                raw_df['Registered_Address_Clean']=raw_df['Registered_Address'].apply(lambda x:clean_bnk_br_st_ad(str(x)))
                raw_df = raw_df[['Credit_Institution','Credit_Institution_Clean','Branch','Branch_Clean','State','State_Clean','State_Mapped','Party','Party_Clean','Registered_Address',
                                 'Registered_Address_Clean','Outstanding_Amt_Lacs','Source','Category','Relevant_Date','Runtime']]
                raw_df=drop_duplicates(raw_df)
                #%%
                
                raw_df.to_sql('CIBIL_EQUIFAX_CRIF_DEFAULTERS_CLEAN_CONSOLIDATED_MONTHLY_DATA', con=engine, if_exists='append', index=False)
                print('Data loded in mysql')
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
