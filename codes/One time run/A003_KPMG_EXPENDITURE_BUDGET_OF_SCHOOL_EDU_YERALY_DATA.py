# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 10:12:07 2024

@author: Santonu
"""
import datetime
import os
import re
import sys
import warnings
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from playwright.sync_api import sync_playwright
from pytz import timezone
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
from selenium.webdriver.support.ui import Select
import pdfplumber
from fiscalyear import *
import fiscalyear
warnings.filterwarnings('ignore')
os.chdir(r"C:/Users/Santonu/Desktop/ADQvest/Error files/today error/KPMG/UNION_BUDGET")
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
#%%
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days

#%%
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df
def row_col_index_locator(df,l1,take_min=False):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    touple_list=[]
    dict1={}
    for j in l1:
        tpl=()
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                tpl=(i,row_index)
                break
        touple_list.append(tpl)
   
    
    if take_min==True:
        touple_list = [t for t in touple_list if t]
        min_tuple = min(touple_list, key=lambda x: x[0])
        return min_tuple
    else:
        return index2
def get_request_session(url):
    
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r=session.get(url)
    return r

def extract_table_using_plumber(pdf_file,search_str):
    
   
    df_list=[]
    page_list=[]
    with pdfplumber.open(pdf_file) as pdf:
         for i in range(len(pdf.pages)):
             # print(i)
             i=6
             page_text=pdf.pages[i].extract_text()
             if search_str.lower() in page_text.lower():
                 page_list.append(i)
                 break
             
    with pdfplumber.open(pdf_file) as pdf:                    
         for i in range(len(pdf.pages)):
             if i in page_list:
                 table = pdf.pages[i].extract_tables()
                 df = pd.DataFrame(table[0],columns=table[0][0])
    
    return df

def Column_ffill(df,row_index,forward=True):
    df.fillna('#', inplace=True)
    for c in range(df.shape[1]):
        if df.iloc[row_index,c]=='#':
            if forward==True:
               df.iloc[row_index,c]=df.iloc[row_index,c-1]
            else:
                 df.iloc[row_index,c]=np.where(((df.iloc[row_index,c+1]=='#')==False),df.iloc[row_index,c+1],df.iloc[row_index,c])
            print(df.iloc[row_index,c])
    
    df=df.replace('#',np.nan)
    return df
def get_financial_year(dt,prev_fiscal=False):
    fiscalyear.setup_fiscal_calendar(start_month=4)
    if dt.month<4:
        yr=dt.year
    else:
        yr=dt.year+1
        
    if prev_fiscal==True:
        yr=yr-1
        
    fy = FiscalYear(yr)
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    fy_year=str(fy.start.year)+'-'+str(fy.end.year)[-2:]
    return fy_year

def process_com_df(df2,category,sub_cat,sub_cat_2):
    df2_list=[]
    my_set=set(df2.columns.tolist())
    my_set.remove('Item')
    for i in my_set:
        df_x=df2.copy()
        df_x.drop(columns=[j for j in df_x.columns if j not in [i,'Item']], inplace=True)
        df_x= df_x.replace('#',np.nan)
        df_x.iloc[0,0]='Item'
        df_x.columns=df_x.iloc[0, :].astype(str)
        df_x=df_x.iloc[1:,:]
        df_x.reset_index(drop=True,inplace=True)
        df_x.dropna(axis=1,how='all',inplace=True)
        df_x.dropna(subset=[df_x.columns[0]], inplace=True)
        df_x.reset_index(drop=True,inplace=True)
        df_x['Type']=i
        
        df_x=row_filling(df_x,0,category,new_col='Category')
        df_x=row_filling(df_x,0,sub_cat,new_col='Sub_Category')
        
        df_x=row_filling(df_x,0,sub_cat_2,new_col='Sub_Category_2')
        df_x['Item_Clean']=np.where(df_x['Item'].str.lower().str.contains(r'\d', na=False),df_x['Item'],np.nan)
        condition=((df_x['Item'].str.lower().str.contains('total', na=False)) & (df_x['Item_Clean'].isna()))
        df_x['Item_Clean']=np.where(condition,df_x['Item'],df_x['Item_Clean'])
        df2_list.append(df_x)
    
    return df2_list

def get_date_from_fy(y):
    y=int(y.split('-')[1])
    fiscalyear.setup_fiscal_calendar(start_month=4)
    fy = FiscalYear(y)
    fyend = fy.end.strftime("%Y-%m-%d")
    return fyend

def row_filling(df,col_idx,result_list=[],new_col='New_col',fill_type='F',fill=True):
    df[new_col]=np.nan
    for i in result_list:
        row_index=df[df.iloc[:, col_idx].str.lower().str.contains(i.lower(),case=False,flags=re.IGNORECASE) ==True].index.to_list()
        if len(row_index)>0:
            df.loc[row_index[0],new_col]=df.iloc[row_index[0],col_idx]
    
    if fill!=False:
        if fill_type=='B':
            df[new_col]=df[new_col].bfill(axis=0)
        else:
            df[new_col]=df[new_col].ffill(axis=0)
   
        
    return df           
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "KPMG_EXPENDITURE_BUDGET_OF_SCHOOL_EDU_YERALY_DATA"
    scheduler = ''
    no_of_ping=0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
#%%            
        os.chdir(r'C:\Users\Administrator\KPMG')
        download=os.getcwd()
        delete_pdf=os.listdir(r"C:\Users\Administrator\KPMG")
        for file in delete_pdf:
                os.remove(file)
                
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from KPMG_EXPENDITURE_BUDGET_OF_SCHOOL_EDU_YERALY_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()+relativedelta(years=2)
        print(type(max_rel_date))
        financial_yr=get_financial_year(max_rel_date)
        print(financial_yr)
         

        #%%
        s3_buccket_download(f'KPMG/UNION_BUDGET','.xls',download_path='C:/Users/Administrator/KPMG/UNION_BUDGET')
        os.chdir(r"C:\Users\Administrator\KPMG\UNION_BUDGET")
        excel_files=glob.glob(os.path.join(os.getcwd(), "*.xls"))
        excel_files=[i.split('\\')[-1] for i in excel_files]
        print(excel_files)
        
        for file in excel_files:
            report_yr=file.split('KPMG_UNION_BUDGET_')[-1].split('.')[0].replace('_', '-')
        
        #%%
            # file='C:/Users/Santonu/Desktop/ADQvest/Error files/today error/KPMG/UNION_BUDGET/allsbe_23_24.xls'
            # file='C:/Users/Santonu/Desktop/ADQvest/Error files/today error/KPMG/UNION_BUDGET/KPMG_UNION_BUDGET_2024_25.xlsx'

            df=pd.read_excel(file)
            df.drop(columns=[col for col in df.columns if df[col].nunique() == 1], inplace=True)
            
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(axis=0,how='all',inplace=True)
            df.reset_index(drop=True,inplace=True)
            
            headings=row_col_index_locator(df,['Actuals'])[1]
            df_head=df.iloc[headings:headings+2,:]
            df_head2=df.iloc[headings:headings+2,:]
            df_head= df_head.replace('#',np.nan)
            df_head=Column_ffill(df_head,0)
            df_head= df_head.replace('#',np.nan)
            df_head1=Column_ffill(df_head,1,forward=False)
            
            
            si=row_col_index_locator(df,['Establishment Expenditure of the Centre'])
            ei=row_col_index_locator(df,['Note'])
            
            df=df.iloc[si[1]-1:ei[1],:]

           

            condition=(((df[df.columns[4]]=='#')==False) & ((df[df.columns[6]]=='#')==False))
            df[df.columns[4]]=np.where(condition,df[df.columns[4]]+' '+df[df.columns[6]],df[df.columns[4]])
            
            condition=(((df[df.columns[2]]=='#')==False) & ((df[df.columns[4]]=='#')==False))
            df[df.columns[2]]=np.where(condition,df[df.columns[2]]+' '+df[df.columns[4]],df[df.columns[2]])
            
            condition=(((df[df.columns[4]]=='#')==False) & ((df[df.columns[2]]=='#')==True))
            df[df.columns[2]]=np.where(condition,df[df.columns[4]],df[df.columns[2]])
            
            condition=(((df[df.columns[1]]=='#')==False) & ((df[df.columns[2]]=='#')==True))
            df[df.columns[2]]=np.where(condition,df[df.columns[1]],df[df.columns[2]])

            
            condition=(((df[df.columns[5]]=='#')==False) & ((df[df.columns[3]]=='#')==False))
            df[df.columns[5]]=np.where(condition,df[df.columns[3]].astype(str)+' '+df[df.columns[5]].astype(str),df[df.columns[5]])
            
            condition=(((df[df.columns[5]]=='#')==False) & ((df[df.columns[2]]=='#')==True))
            df[df.columns[2]]=np.where(condition,df[df.columns[5]],df[df.columns[2]])
            
            condition=(((df[df.columns[7]]=='#')==False) & ((df[df.columns[2]]=='#')==True))
            df[df.columns[2]]=np.where(condition,df[df.columns[7]],df[df.columns[2]])
            

            
            
            df.drop(df.columns[[0,1,3,4,5,6,7]], axis=1, inplace=True)
            df= df.replace('#',np.nan)
            df.dropna(axis=1,how='all',inplace=True)
          
            #%%
            si_2=row_col_index_locator(df,['B. Developmental Head'])[1]

            df1=df.iloc[:si_2,:]
            df1= df1.replace('#',np.nan)
            df1.drop(columns=[i for i in df1.columns if i not in df_head1.columns.tolist()], inplace=True)
            df_head1.drop(columns=[i for i in df_head1.columns if i not in df1.columns.tolist()], inplace=True)
            
            df1=pd.concat([df_head1,df1])
            df1.reset_index(drop=True,inplace=True)
            df1=Column_ffill(df1,0,forward=False)
            df1.reset_index(drop=True,inplace=True)
            df1= df1.replace(np.nan,'#')
            df1.iloc[0,0]='Item'
            df1.columns=df1.iloc[0, :].astype(str)
            df1=df1.iloc[1:,:]
            df1.reset_index(drop=True,inplace=True)
            print(df1)
            
            category=["CENTRE'S EXPENDITURE","TRANSFERS TO STATES/UTs"]
            sub_cat=["Establishment Expenditure of the Centre",'Central Sector Schemes/Projects',
                    'Other Central Sector Expenditure','Centrally Sponsored Schemes',
                    'Other Grants/Loans/Transfers']
            
            sub_cat_2=["Autonomous Bodies","Others","National Education Mission"]
            

            # df1=row_filling(df1,0,["CENTRE'S EXPENDITURE","TRANSFERS TO STATES/UTs"],new_col='Category')
            # df1=row_filling(df1,0,["Establishment Expenditure of the Centre",'Central Sector Schemes/Projects',
            #                                     'Other Central Sector Expenditure','Centrally Sponsored Schemes',
            #                                     'Other Grants/Loans/Transfers'],new_col='Sub_Category')
            
            # df1=row_filling(df1,0,["Autonomous Bodies","Others","National Education Mission"],new_col='Sub_Category_2')
            # item=_list=['Secretariat','Directorate of Adult Education','Total - Establishment Expenditure of the Centre',
            #             'National Award to Teachers','Pradhan Mantri Innovative Learning Programme (DHRUV)','National Means cum Merit Scholarship Scheme',
            #             'Total - Central Sector Schemes/Projects']
            
            # df1=row_filling(df1,0,["Autonomous Bodies","Others","National Education Mission"],new_col='Item_Clean')
            
            # df1['Item_Clean']=np.where(df1['Item'].str.lower().str.contains(r'\d', na=False),df1['Item'],np.nan)
            # condition=((df1['Item'].str.lower().str.contains('total', na=False)) & (df1['Item_Clean'].isna()))
            # df1['Item_Clean']=np.where(condition,df1['Item'],df1['Item_Clean'])


            #%%
            df1_list=process_com_df(df1,category,sub_cat,sub_cat_2)
            df1_final=pd.concat(df1_list)
            df1_final.reset_index(drop=True,inplace=True)

           
            for c in range(df_head2.shape[1]):
                if df_head2.iloc[0,c]=='#':
                    try:
                        df_head2.iloc[0,c]=df_head2.iloc[0,c+1]
                    except:
                        pass
            df_head2=Column_ffill(df_head2,1,forward=False)
            df_head2=Column_ffill(df_head2,0,forward=True)
                        
                      

            df2=df.iloc[si_2:,:]
            df2= df2.replace('#',np.nan)
            df2.drop(columns=[i for i in df2.columns if i not in df_head2.columns.tolist()], inplace=True)
            df_head2.drop(columns=[i for i in df_head2.columns if i not in df2.columns.tolist()], inplace=True)
            
            df2=pd.concat([df_head2,df2])
            df2.reset_index(drop=True,inplace=True)
            df2= df2.replace('#',np.nan)
            df2.dropna(axis=1,how='all',inplace=True)
            df2=Column_ffill(df2,0,forward=False)
            df2.reset_index(drop=True,inplace=True)
            df2= df2.replace(np.nan,'#')
            df2.iloc[0,0]='Item'

            df2.columns=df2.iloc[0, :].astype(str)
            df2=df2.iloc[1:,:]
            df2.reset_index(drop=True,inplace=True)
            print(df2)
            # df2 = pd.melt(df2, id_vars=['Item'], var_name='Commodity', value_name='Procurement_Lakh_MT')
            
            cat=["Developmental Head"]
            sub_cat=["Social Services","Others"]
            sub_cat_2=[]
            
            
            df2_list=process_com_df(df2,cat,sub_cat,sub_cat_2)

            
            # com_col=['Item', 'Revenue', 'Capital', 'Total', 'Type']
            # for j in range(0,len(df2_list)):
            #     d=df2_list[j]
            #     df_col=d.columns.tolist()
            #     missing_col=[i for i in com_col if i not in df_col]
            #     if len(missing_col)>0:
            #         for mc in missing_col:
            #             df2_list[j][mc]=np.nan

            def add_proxy_col(df2_list):
                com_col=['Item', 'Revenue', 'Capital', 'Total', 'Type','Category','Sub_Category', 'Sub_Category_2', 'Item_Clean']
                for j in range(0,len(df2_list)):
                    d=df2_list[j]
                    df_col=d.columns.tolist()
                    missing_col=[i for i in com_col if i not in df_col]
                    if len(missing_col)>0:
                        for mc in missing_col:
                            df2_list[j][mc]=np.nan
                            df2_list[j]=df2_list[j][com_col]
                return df2_list
            df2_list=add_proxy_col(df2_list)
            df2_final=pd.concat(df2_list)
            df2_final.reset_index(drop=True,inplace=True)
            #%%
            df_final=pd.concat([df1_final,df2_final])
            df_final.reset_index(drop=True,inplace=True)
            
            drop_index=df_final[((~df_final['Category'].isna()) & (df_final['Item_Clean'].isna()) & ((df_final['Item']=='Net')==False))].index.tolist()
            df_final=df_final.drop(index=drop_index)
            df_final.reset_index(drop=True,inplace=True)
            
            df_final['Item_Clean']=np.where(df_final['Item_Clean'].isna(),df_final['Item'],df_final['Item_Clean'])
            df_final['Item_Clean']=df_final['Item_Clean'].apply(lambda x:''.join([str(char) for char in x if not char.isdigit()]).replace('.','').replace('-',' ').strip())
            
            df_final['Sub_Item']=np.where(df_final['Item'].str.lower().str.contains('net'),'Net',np.nan)
            df_final['Sub_Item']=np.where(df_final['Item_Clean'].isin(['Support for Samagra Shiksha','EAP Component','Total   Samagra Shiksha']),df_final['Item_Clean'],df_final['Sub_Item'])
            df_final['Item_Clean']=np.where(df_final['Item_Clean'].isin(['Support for Samagra Shiksha','EAP Component','Total   Samagra Shiksha']),'Samagra Shiksha',df_final['Item_Clean'])
            
            df_final['Item_Clean']=np.where(df_final['Item_Clean'].isin(['Net']),np.nan,df_final['Item_Clean'])
            
            df_final['Sub_Item']=np.where(df_final['Item'].str.lower().str.contains('net'),'Net',df_final['Sub_Item'])
            df_final['Sub_Item']=np.where(df_final['Item'].str.lower().str.contains('01|02|03|04|05|06'),df_final['Item_Clean'],df_final['Sub_Item'])
            special_items=['Total   Samagra Shiksha',
                        'Total - Samagra Shiksha',
                      
                        'Total  Samagra Shiksha',
                        'Total  Sarva Shiksha Abhiyan',
                        'Total  Rashtriya Madhyamik Shiksha Abhiyan',
                        'Total   Teachers Training and Adult Education',
                        'Total  Teachers Training and Adult Education',
                        'Total   National Means cum Merit Scholarship Scheme',
                        'Total  National Means cum Merit Scholarship Scheme',
                        'Total   National Scheme for Incentive to Girl Child for Secondary Education',
                        'Total  National Scheme for Incentive to Girl Child for Secondary Education',
                        'Total   Kendriya Vidyalaya Sangathan (KVS)',
                        'Total  Kendriya Vidyalaya Sangathan (KVS)',
                        
                        'Total   Navodaya Vidyalaya Samiti (NVS)',
                        'Total  Navodaya Vidyalaya Samiti (NVS)',
                         
                        'Total   National Programme of Mid Day Meal in Schools',
                        'Total  National Programme of Mid Day Meal in Schools'
                        ]
     
            df_final['Sub_Item']=np.where(df_final['Item_Clean'].isin(special_items),df_final['Item_Clean'],df_final['Sub_Item'])
            
            
            
            df_final['Item_Digit']=df_final['Item'].apply(lambda x:re.findall(r'\d+.', x))
            df_final['Item_Digit']=df_final['Item_Digit'].apply(lambda x:x[0] if len(x)==1 else np.nan)
            df_final['Item_Digit'] = np.where(~(df_final['Item_Digit'].isna()), df_final['Item_Clean'], df_final['Item_Digit'])


            condition4=df_final['Item_Digit'].isna() & (~df_final['Sub_Item'].isin(['nan','Net']))
            df_final['Item_Digit'] = np.where(condition4,df_final['Item_Digit'].ffill(axis=0), df_final['Item_Digit'])
            df_final['Item_Digit'] = np.where(df_final['Item_Digit'].isna(),df_final['Item_Clean'], df_final['Item_Digit'])
            df_final.drop(columns=['Item_Clean'], inplace=True)
            df_final=df_final.rename(columns={'Item_Digit':'Item_Clean'})
            
            df_final['Item_Clean']=np.where(df_final['Item_Clean'].isin(['Net']),np.nan,df_final['Item_Clean'])
            
            condition1=((df_final['Sub_Item'].isin(['nan'])) & df_final['Item_Clean'].isin(['Samagra Shiksha','Sarva Shiksha Abhiyan',
                                                                                            'Rashtriya Madhyamik Shiksha Abhiyan',
                                                                                            'Teachers Training and Adult Education',
                                                                                            # 'National Means cum Merit Scholarship Scheme',
                                                                                            # 'National Scheme for Incentive to Girl Child for Secondary Education',
                                                                                            # 'Kendriya Vidyalaya Sangathan (KVS)',
                                                                                            # 'Navodaya Vidyalaya Samiti (NVS)',
                                                                                            # 'National Programme of Mid Day Meal in Schools'
                                                                                            ]))
            df_final=df_final[~(condition1)]
          
            df_final['Sub_Category_2']=np.where(df_final['Sub_Category'].isin(['Other Grants/Loans/Transfers']),np.nan,df_final['Sub_Category_2'])
            df_final['Category']=df_final['Category'].apply(lambda x:str(x).replace('B. Developmental Head','Developmental Head').strip())
            df_final['Sub_Category_2']=np.where(df_final['Item_Clean'].isin(['Total   Centrally Sponsored Schemes','Total Centrally Sponsored Schemes']),np.nan,df_final['Sub_Category_2'])
            
            condition_5=(df_final['Sub_Category_2'].isin(['Umbrella Programme for Development of Minorities','Umbrella Program for Development of Minorities']) & (df_final['Sub_Category'].isin(['Social Services'])))
            df_final['Sub_Category_2']=np.where(condition_5,np.nan,df_final['Sub_Category_2'])
            
            
            
            condition_5=(df_final['Sub_Category_2'].isin(['Scholarships']) & (df_final['Item_Clean'].isin(['Total Central Sector Schemes/Projects'])))
            df_final['Sub_Category_2']=np.where(condition_5,np.nan,df_final['Sub_Category_2'])
            condition_5=(df_final['Sub_Category_2'].isin(['17 National Programme of Mid Day Meal in Schools']) & (df_final['Item_Clean'].isin(['Transfer to Prarambhik Shiksha Kosh (PSK)','Amount met from Prarambhik Shiksha Kosh (PSK)'])))
            df_final['Sub_Category_2']=np.where(condition_5,'National Education Mission',df_final['Sub_Category_2'])
            df_final['Sub_Category_2']= df_final['Sub_Category_2'].replace({'17 National Programme of Mid Day Meal in Schools':'National Programme of Mid Day Meal in Schools'})
            
            df_final['Sub_Category_2']=np.where(df_final['Sub_Category'].isin(['Other Grants/Loans/Transfers']),np.nan,df_final['Sub_Category_2'])
            df_final['Category']=df_final['Category'].apply(lambda x:x.replace('B. Developmental Head','Developmental Head').strip())
            
            df_final['Category']=df_final['Category'].apply(lambda x:x.title())
            df_final['FY_Year']=df_final['Type'].apply(lambda x:x.split()[-1])
            df_final['Relevant_Date']=df_final['FY_Year'].apply(lambda x:get_date_from_fy(x))
            df_final['Type']=df_final['Type'].apply(lambda x:x.split()[0])
       
            df_final=df_final.rename(columns={'Item':'Item_Raw','Revenue':'Revenue_Cr','Capital':'Capital_Cr','Total':'Total_Cr'})
            df_final=df_final[['Item_Raw','Item_Clean','Sub_Item','Category','Sub_Category','Sub_Category_2','Type','FY_Year','Revenue_Cr', 'Capital_Cr', 'Total_Cr','Relevant_Date']]
            df_final= df_final.replace('... ',np.nan)
            df_final= df_final.replace('...',np.nan)
            df_final["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            df_final['Relevant_Date'] = pd.to_datetime(df_final['Relevant_Date'], format='%Y-%m-%d')
            df_final['Relevant_Date']=df_final['Relevant_Date'].dt.date
            
            df_final.reset_index(drop=True,inplace=True)
            df_final=drop_duplicates(df_final)
            
            df_final['Revenue_Cr']=df_final['Revenue_Cr'].astype(float)
            df_final['Capital_Cr']=df_final['Capital_Cr'].astype(float)
            df_final['Total_Cr']=df_final['Total_Cr'].astype(float)
            
            df_final[['Item_Clean','Sub_Item','Category','Sub_Category','Sub_Category_2']] = df_final[['Item_Clean','Sub_Item','Category','Sub_Category','Sub_Category_2']].applymap(lambda x:str(x).upper() if pd.notnull(x) else x)
            df_final['Sub_Category_2']=np.where(df_final['Item_Clean'].isin(['GRAND TOTAL']),np.nan,df_final['Sub_Category_2'])
            df_final['Sub_Category']=np.where(df_final['Item_Clean'].isin(['GRAND TOTAL']),'GRAND TOTAL',df_final['Sub_Category'])


            df_final.reset_index(drop=True,inplace=True)
            #%%
            for j in df_final.Type.unique().tolist():
                       Upload_Data('KPMG_EXPENDITURE_BUDGET_OF_SCHOOL_EDU_YERALY_DATA',df_final,j,['MySQL'])
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')