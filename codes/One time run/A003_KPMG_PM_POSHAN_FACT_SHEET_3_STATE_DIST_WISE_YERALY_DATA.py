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
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime
warnings.filterwarnings('ignore')
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
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
job_start_time = datetime.datetime.now(india_time)
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
#%%
def Upload_Data(table_name, data,ty:str, db: list):

    query=f"select distinct Relevant_Date as Relevant_Date from {table_name} where State='{ty}';"
    print(query)
    db_max_date = pd.read_sql(query,engine)
    print(db_max_date)
    data['Relevant_Date'] = pd.to_datetime(data['Relevant_Date'], format='%Y-%m-%d')

    data=data.loc[data['State']==ty]
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    # print(data.info())
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        # print(f'Done for --->{db_max_date,ty}')
        print(data.info())
        
        
def get_date_from_fy(y):
    try:
        y='20'+y.split('-')[1]
        y=re.findall(r'\d{4}',y)[0]
    except:
        y=int(y)
    
    # print(y)
    fiscalyear.setup_fiscal_calendar(start_month=4)
    fy = FiscalYear(int(y))
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    return fyend

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
def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    # print(pdf_list)
    matching = [s for s in pdf_list if key_word in s]
    # print('Matching')
    # print(matching)
                   
    
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=10000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto("https://www.ilovepdf.com/",timeout=30000*5)

        page.locator("//*[contains(text(),'Login')]").click()
        email = page.locator("//*[@id='loginEmail']")
        email.fill("kartmrinal101@outlook.com")
        password = page.locator("//*[@id='inputPasswordAuth']")
        password.fill("zugsik-zuqzuH-jyvno4")
        page.locator("//*[@id='loginBtn']").click()
        page.get_by_title("PDF to Excel").click()

        for i in matching:
            with page.expect_file_chooser() as fc_info:
                page.get_by_text("Select PDF file").click()
                file_chooser = fc_info.value
                file_chooser.set_files(i)
                if OCR_doc==True:
                    page.get_by_text("Continue without OCR").click()
                    
                page.locator("//*[@id='processTask']").click()
                with page.expect_download() as download_info:
                    page.get_by_text("Download EXCEL").click()
                # Wait for the download to start
                download = download_info.value
                # Wait for the download process to complete
                # print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    os.chdir('C:/Users/Administrator/KPMG/PM_POSHAN')
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
         's3',
         aws_access_key_id=ACCESS_KEY_ID,
         aws_secret_access_key=ACCESS_SECRET_KEY,
         config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
    data_s3 =  open(filename, 'rb')
    s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
    data_s3.close()
    

def read_link(link,file_name,s3_folder):
    os.chdir('C:/Users/Administrator/KPMG/PM_POSHAN')
    path=os.getcwd()
    # r=requests.get(link,headers =headers,verify=True)
    # r.raise_for_status()
    r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.xls"))
    # print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    return file



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

def s3_buccket_download(folder_name,file_extension,download_path):
    
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    os.chdir(download_path)
 
    s3_client =boto3.client('s3')
    s3_bucket_name='adqvests3bucket'
    s3 = boto3.resource('s3',
                        aws_access_key_id= ACCESS_KEY_ID,
                        aws_secret_access_key=ACCESS_SECRET_KEY)

    my_bucket=s3.Bucket(s3_bucket_name)
    bucket_list = []
    file_list=[i.key for i in my_bucket.objects.filter(Prefix =folder_name) if f'{file_extension}' in i.key ]
    print(f"Total item in bucket : {len(file_list)}")
    
    for file in file_list:
        print(file)
        try:
          downloded_file=s3.Bucket(s3_bucket_name).download_file(file,f'{file.split("/")[-1]}')
        except:
            pass

    print("Done")

def row_col_index_locator(df,l1,take_min=False):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    touple_list=[]
    dict1={}
    for j in l1:
        tpl=()
        for i in range(df.shape[1]):
            condition=df.iloc[:,i].str.lower().str.replace('(','').str.replace(')','').str.replace('  ', ' ').str.replace('  ', ' ').str.strip().str.contains(j.lower())
            # print(condition)
            if condition.any()==True:
                # print(f"Found--{j}")
                # print(f"Column--{i}")
                index2.append(i)
                
                row_index=df[condition == True].index[0]
                # print(f"Row--{row_index}")
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

def get_renamed_columns(df,col_dict):
    df.fillna('#', inplace=True)
    for k,v in col_dict.items():
        # print(k)
        try:
            ele=[item for item in df.columns if k.lower() in item.lower()]
            k1=df.columns.to_list().index(ele[0])
            df=df.rename(columns={f"{df.columns[k1]}":f"{col_dict[k]}"})
        except:
            pass
    
    df.reset_index(drop=True,inplace=True)
    df=df.replace('#',np.nan)
    return df

def row_modificator(df,l1,col_idx,row_del=False,keep_row=False,update_row=False):
  from collections import ChainMap
  keep_inx=[]
  # print(type(l1[0]))
  if isinstance(l1[0],dict):
      l1=dict(ChainMap(*l1))
  else:
      l1=dict.fromkeys(l1,np.nan)
    
  for i in l1.keys():
    df = df.reset_index(drop=True)
    r=df[df.iloc[:,col_idx].str.lower().str.contains(i.lower())==True].index.to_list()
    if (keep_row==True):
        keep_inx.append(r[0])
    
    if row_del==True:
        df.drop(index=r,inplace=True)
        df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
    else:
        if (update_row==True):
            for j in r:
                   # print(r)
                   df.iloc[j,col_idx]=l1[i]
                   df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
            
  if(keep_row==True):
        drop_list=[i for i in df.index if i not in keep_inx]
        df.drop(index=drop_list,inplace=True)
               
  df.reset_index(drop=True,inplace=True)    
  return df  

def process_df(df,sk:list,ek:list,del_col=['Diff', '% Diff', '#','% Diff(NC)','% Coverage against enrolment'],col_update1={}):
    df.dropna(axis=1,how='all',inplace=True)

    col_update={'Sl. No':'Sl_No','Enrolment':'Enrolment_No','Average number of children':'Avg_Number_Of_Students_Availed_Meals_No','Districts':'District'}
    col_update.update(col_update1)
    # sk='Coverage Chidlren vs. Enrolment Primary'
    # ek=['Coverage Chidlren vs. Enrolment Upper Primary','coverage chidlren vs. enrolment up pry']
    
    s1=row_col_index_locator(df,sk)
    e1=row_col_index_locator(df,ek)
    
    try:
        cat1=df.iloc[s1[1],s1[0]].split('Source data|Source')[0].split('Enrolment')[1].split(')')[0].split('(')[1].strip()
    except:
        try:
          cat1=df.iloc[s1[1],s1[0]].split('Source data')[0].split('children')[1].split(')')[0].split('(')[1].strip()

        except:
          try:
             cat1=df.iloc[s1[1],s1[0]].split('Source data')[0].split('Chidlren')[1].split(')')[0].split('(')[1].strip()
          except:
             cat1=df.iloc[s1[1],s1[0]].split('Children')[1].split(')')[0].split('(')[1].strip()

              
    try:
        state=df.iloc[row_col_index_locator(df,['state'])[1],row_col_index_locator(df,['state'])[0]].split(':')[1].strip()

    except:
        state=np.nan
        
    fy=df.iloc[row_col_index_locator(df,['MDM PAB Approval for'])[1],row_col_index_locator(df,['MDM PAB Approval for'])[0]].lower().split('for')[1].strip()
    fy=get_date_from_fy(fy)
    # print(cat1)    
    # print(fy)
    df1=df.iloc[s1[1]:e1[1],:]
    
    col_ind_1=row_col_index_locator(df1,['Districts'])
    df1.columns=df1.iloc[col_ind_1[1],:]
    df1=df1.iloc[col_ind_1[1]+2:,:]
    
    df1.drop(columns=[col for col in df1.columns if col  in del_col], inplace=True)
    df1=get_renamed_columns(df1,col_update)
    
    df1=row_modificator(df1,['Total'],1,row_del=True)
    df1.dropna(axis=0,how='all',inplace=True)
    
    df1['District']=df1['District'].apply(lambda x:re.findall('[A-Za-z]+', x.replace('\n','')))
    df1['District']=df1['District'].apply(lambda x:' '.join(x).title())
    
    df1['Head']=cat1
    df1['State']=state
    df1["Relevant_Date"]=fy
    
    
    return df1

def process_df(df,sk:list,ek:list,del_col=['Diff', '% Diff', '#','% Diff(NC)','% Coverage against enrolment','Average Pry (I-V)', 'Average NCLP', 'Total'],col_update1={},enrole=True):
    # df=pd.read_excel(xl_lnk)
    df.dropna(axis=1,how='all',inplace=True)

    col_update={'Sl. No':'Sl_No','Enrolment':'Enrolment_No','Enrollment':'Enrolment_No','Average number of children':'Avg_Number_Of_Students_Availed_Meals_No','Districts':'District','Avg. number of children availing MDM':'Avg_Number_Of_Students_Availed_Meals_No'}
    col_update.update(col_update1)
    # sk=a1
    # ek=b1
    
    s1=row_col_index_locator(df,sk)
    e1=row_col_index_locator(df,ek)
    
    try:
        cat1=df.iloc[s1[1],s1[0]].split('Source data|Source')[0].split('Enrolment')[1].split(')')[0].split('(')[1].strip()
    except:
        try:
          cat1=df.iloc[s1[1],s1[0]].split('Source data')[0].split('children')[1].split(')')[0].split('(')[1].strip()
        except:
          try:
             cat1=df.iloc[s1[1],s1[0]].split('Source data')[0].split('Chidlren')[1].split(')')[0].split('(')[1].strip()
          except:
            try:
                cat1=df.iloc[s1[1],s1[0]].split('Children')[1].split(')')[0].split('(')[1].strip()
            except:
                cat1=df.iloc[s1[1],s1[0]].split('Source data')[0].split('children')[-1].split(')')[0].split('(')[1].strip()



              
    try:
        state=df.iloc[row_col_index_locator(df,['state'])[1],row_col_index_locator(df,['state'])[0]].split(':')[1].strip()
        if row_col_index_locator(df,['state'])[1]>60:
            state=np.nan
            
    except:
        state=np.nan
    
    try:
        fy=df.iloc[row_col_index_locator(df,['MDM PAB Approval for'])[1],row_col_index_locator(df,['MDM PAB Approval for'])[0]].lower().split('for')[1].strip()
        fy=get_date_from_fy(fy)
    except:
        fy=df.iloc[row_col_index_locator(df,['MDM SCHEME DURING'])[1],row_col_index_locator(df,['MDM SCHEME DURING'])[0]].lower().split('during')[1].strip().split('(')[0].strip()
        fy=get_date_from_fy(fy)
        
        
    # print(cat1)    
    # print(fy)

    kwd=df.iloc[s1[1],s1[0]].lower().replace('(','').replace(')','').replace('  ', ' ').replace('  ', ' ').strip()
    # if ((len(s1)>0) and ('no. of children primary'  in kwd)| ('no. of children upper primary'  in kwd)):
    if ((len(s1)>0) and ('no. of children primary'  in kwd)| ('no. of children upper primary'  in kwd) |('coverage of children primary'  in kwd)|('coverage of children upper primary'  in kwd)):

        if enrole==True:
            s2=row_col_index_locator(df,['Enrollment','Enrolment'])
            if s2[1]>s1[1]:
                df=df.iloc[s1[1]+1:,:]
                df.reset_index(drop=True,inplace=True)
                s1=row_col_index_locator(df,sk)
            
            else:
                 s2=row_col_index_locator(df,['per Enrolment'])
                 df=df.iloc[s2[1]-5:,:]
                 df.reset_index(drop=True,inplace=True)
                 s1=row_col_index_locator(df,sk)

        else:
            s2=row_col_index_locator(df,['PAB Approval'])
            if s2[1]>s1[1]:
                df=df.iloc[s1[1]+1:,:]
                df.reset_index(drop=True,inplace=True)
                s1=row_col_index_locator(df,sk)
            
    
    if len(s1)>0:
        df=df.iloc[s1[1]:,:]
        df.reset_index(drop=True,inplace=True)
        s1=row_col_index_locator(df,sk)
        e1=df[df.iloc[:,1].str.lower().str.contains('total') == True].index[0]
        if e1>80:
            e1=df[df.iloc[:,0].str.lower().str.contains('total') == True].index[0]
            if e1>100:
                e1=s1[1]+5
        
    
    df1=df.iloc[s1[1]:e1,:]
    col_ind_1=row_col_index_locator(df1,['Districts'])
    dpt=False
    if len(col_ind_1)==0:
        col_ind_1=row_col_index_locator(df1,['Department'])
        df1=df1.rename(columns={df1.columns[1]:'Districts'})
        dpt=True
        
    df1.columns=df1.iloc[col_ind_1[1],:]
    if dpt==True:
        df1=df1.rename(columns={df1.columns[1]:'Districts'})

    df1=get_renamed_columns(df1,{'Department':'Districts'})
    df1=df1.iloc[col_ind_1[1]+1:,:]
    df1=df1.iloc[:,:5]
  
    df1['Districts'] = df1['Districts'].astype(str)
    df1 = df1[~df1['Districts'].str.isdigit()]
        
    df1.reset_index(drop=True,inplace=True)
    df1=df1.iloc[:,:5]
    
    
    df1.drop(columns=[col for col in df1.columns if col  in del_col], inplace=True)
    df1=get_renamed_columns(df1,col_update)
    df1.dropna(subset=['District'], inplace=True)
    
    # df1=row_modificator(df1,['Total'],1,row_del=True)
    df1.dropna(axis=0,how='all',inplace=True)
    df1 = df1[df1.nunique(axis=1) > 1]


    
    df1['District']=df1['District'].apply(lambda x:re.findall('[A-Za-z]+', x.replace('\n','')))
    df1['District']=df1['District'].apply(lambda x:' '.join(x).title())
    
    df1['Head']=cat1
    df1['State']=state
    df1["Relevant_Date"]=fy
    
    
    return df1
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = "KPMG_PM_POSHAN"
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
        os.chdir(r'C:\Users\Administrator\KPMG\PM_POSHAN')
        download=os.getcwd()
        delete_pdf=os.listdir(r"C:\Users\Administrator\KPMG\PM_POSHAN")
        # for file in delete_pdf:
        #         os.remove(file)
                
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from KPMG_PM_POSHAN_DIST_COVERAGE_temp_santonu", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()+relativedelta(years=1)
        print(type(max_rel_date))
        financial_yr=get_financial_year(max_rel_date)
        print(financial_yr)

        df_st_alpha=pd.read_sql("select *  from INDIA_STATE_N_STATE_ALPHA_CODE_GST_CODE_MAPPING;", con=engine)
        col_st=pd.read_sql("Select distinct State,PM_POSHAN_Report_Yr from KPMG_PM_POSHAN_DIST_COVERAGE_temp_santonu;", con=engine)


        #%%
        # os.chdir(r"C:/Users/Santonu/Desktop/ADQvest/Error files/today error/KPMG/PM_POSHAN")
        # download_path=os.getcwd()
        
        #%%
        # link=f"https://pmposhan.education.gov.in/PAB%20{financial_yr.split('-')[0]}-20{financial_yr.split('-')[1]}.html"
        # robot.add_link(url)
        # r=get_request_session(link)
        # soup=BeautifulSoup(r.content)

        # #2021-2022------2019-2020
        # state_link={i.text.replace('\n',' ').strip().lower():i.find_all('a',href=True)[2]['href'] for i in soup.find_all('tr') if ((i.find('a',href=True)!=None) and ('edcil-tsg' not in i.text.replace('\n',' ').strip().lower()) and ('nic' not in i.text.replace('\n',' ').strip().lower()))}
        # state_link={k.split('pdf')[0].split('.')[1]+financial_yr:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        # state_link={'_'.join(k.replace('\n','_').strip().split()).upper().replace('_;',''):'https://pmposhan.education.gov.in/'+v for k,v in state_link.items()}
        
        #2018-2019------2017-2018
        # state_link={i.text.replace('\n',' ').strip().lower():i.find_all('a',href=True)[2]['href'] for i in soup.find_all('tr') if ((i.find('a',href=True)!=None) and ('edcil-tsg' not in i.text.replace('\n',' ').strip().lower()) and ('nic' not in i.text.replace('\n',' ').strip().lower()))}
        # state_link={re.findall('[A-Za-z]+', k.split('pdf')[0].replace('\n',''))[0]:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        # state_link={k+'_'+financial_yr.split('-')[0]+'-'+financial_yr.split('-')[1][-2:]:v for k,v in state_link.items()if(( 'edcil-tsg' not in k) and ('nic' not in k))}
        # state_link={'_'.join(k.replace('\n','_').strip().split()).upper().replace('_;',''):'https://pmposhan.education.gov.in/'+v for k,v in state_link.items()}
        

        #%%
        s3_buccket_download(f'KPMG/PM_POSHAN_FACTS_SHEET/{financial_yr}','.xls',download_path='C:/Users/Administrator/KPMG/PM_POSHAN')
        os.chdir(r"C:\Users\Administrator\KPMG\PM_POSHAN")
        excel_files=glob.glob(os.path.join(os.getcwd(), "*.xls"))
        excel_files=[i.split('\\')[-1] for i in excel_files]
        print(excel_files)
        engine.execute('Delete from KPMG_PM_POSHAN_STUDENT_STATUS_TABLE')
        for xl_lnk in excel_files:
                report_yr=xl_lnk.split('_')[-1].split('.')[0]
                st=' '.join(xl_lnk.split('_')[:-1]).replace('PM POSHAN','').title()
                print(st)
                print(report_yr)
                flag=True
                for st1,dt1 in zip(col_st.State,col_st.PM_POSHAN_Report_Yr):
                    if ((st.title()==st1) and (report_yr==dt1)):
                        print('Data Collected')
                        flag=False
                        continue
                if flag==False:
                    continue
                #%%
                try:
                    df=pd.read_excel(xl_lnk)
                    a1=['Coverage Chidlren vs. Enrolment Primary','Enrolment Vs Coverage Primary','coverage of children against enrolment primary','coverage of children vs enrolment primary','coverage children vs. enrolment primary','enrolment vs children availed mdm primary','no. of children primary',
                         'coverage of children vs enrollment primary','no. of children enrolment primary','coverage of children primary','no. of children primary']
                    b1=['Coverage Chidlren vs. Enrolment Upper Primary','coverage chidlren vs. enrolment up pry','Enrolment Vs Coverage Upper Primary']
                    df1=process_df(df,a1,b1,enrole=True)
                    
                    c1=['coverage chidlren vs. enrolment up. primary','coverage children vs. enrolment upper primary','coverage of children against enrolment upper primary','coverage children vs. enrolment upper primary','Coverage Chidlren vs. Enrolment Upper Primary','coverage chidlren vs. enrolment up pry','Enrolment Vs Coverage Upper Primary','coverage of children vs enrolment upper primary','coverage chidlren vs. enrolment up pry','enrolment vs children availed mdm upper primary','no. of children upper primary',
                        'coverage of children vs enrollment upper primary','no. of children enrolment upper primary','enrolment vs.coverage upper primary','coverage of children upper primary','enrolment vs. coverage of children u.primary']
                    d1=['Coverage Chidlren vs. PAB Approval Primary','no. of children primary']

                    df2=process_df(df,c1,d1,enrole=True)
                    df_enrole=pd.concat([df1,df2])
                    df_enrole['Head']=df_enrole['Head'].replace({'Up Pry':'Upper Primary','Up. Primary':'Upper Primary'})


                    k1=['Coverage Chidlren vs. PAB Approval Primary','coverage chidlren vs. pab approval 2015-16 primary','coverage of children vs pab approval no. of children primary','coverage of children vs pab approval 2016-17 primary','no. of children pab approval primary','coverage of children against approval primary','coverage chidlren vs. pab approval 2016-17 primary','no. of children primary','coverage of children vs pab approval primary','no. of children primary','no.of children primary',
                         'no. of children vs pab approval primary','coverage of children primary']
                    f1=['Coverage Chidlren vs. PAB Approval Upper Primary','no. of children upper primary']

                    
                    df3=process_df(df,k1,f1,enrole=False,col_update1={'children as per PAB Approval':'PAB_Approval_Children_No','PAB Approval 2016-17':'PAB_Approval_Children_No','PAB Approval':'PAB_Approval_Children_No','PAB Approved':'PAB_Approval_Children_No','PAB Approval 2015-16':'PAB_Approval_Children_No'})
                    df3=df3[['Sl_No','State', 'District', 'PAB_Approval_Children_No','Head']]
                    
                    g1=['Coverage Chidlren vs. PAB Approval Upper Primary','coverage chidlren vs. pab approval 2015-16 upper primary','coverage of children vs pab approval 2016-17 upper primary','no. of children pab approval upper primary','coverage of children against approval upper primary','coverage chidlren vs. pab approval 2016-17 upper primary','no. of children upper primary','coverage of children vs pab approval upper primary','no. of children upper primary','no. of children upper primary',
                        'no. of children vs pab approval upper primary','coverage of children upper primary']
                    h1=['Number of meal to be served']

                    df4=process_df(df,g1,h1,col_update1={'children as per PAB Approval':'PAB_Approval_Children_No','PAB Approval 2016-17':'PAB_Approval_Children_No','PAB Approval':'PAB_Approval_Children_No','PAB Approved':'PAB_Approval_Children_No','PAB Approval 2015-16':'PAB_Approval_Children_No'},enrole=False)
                    df4=df4[['Sl_No', 'State','District', 'PAB_Approval_Children_No','Head']]
                    
                    df_number=pd.concat([df3,df4])
                    df_district=pd.merge(df_enrole,df_number,on=['Sl_No', 'District','Head','State'],how='inner')
                    df_district["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    #%%
                    df_district['PAB_Approval_Children_No1']= df_district['PAB_Approval_Children_No'].replace(0,np.nan)
                    df_district['Enrolment_No1']= df_district['Enrolment_No'].replace(0,np.nan)
                    
                    df_district['Avg_Student_Availed_Meals_Vav_PAB_Approval_Pct']=(df_district['Avg_Number_Of_Students_Availed_Meals_No']/df_district['PAB_Approval_Children_No1'])*100
                    df_district['Avg_Student_Availed_Meals_Vav_Enrolment_Pct']=(df_district['Avg_Number_Of_Students_Availed_Meals_No']/df_district['Enrolment_No1'])*100
                    
                    df_district.drop(columns=['PAB_Approval_Children_No1','Enrolment_No1'], inplace=True)

                    df_district['Covered_Under_PM_POSHAN_No']=np.nan
                    df_district['Coverage_Pct']=np.nan
                    df_district['PM_POSHAN_Report_Yr']=report_yr
                    df_district['State']=np.where(df_district['State'].isna(),st,df_district['State'])
                    
                    
                    condition1=(df_district['District'].isin([''])==False)|(~df_district['District'].isna())
                    df_district['District_Clean']=np.where(condition1,df_district['District'].apply(lambda x:district_rewrite.district((x.lower())).split('|')[-1].strip().title()),np.nan)
                    df_district['State_Clean']= df_district['State'].apply(lambda x:state_rewrite.state((x.lower())).split('|')[-1].strip().title())
                      
                 
                    df_district=pd.merge(df_district, df_st_alpha[['State','State_Alpha_Code']],left_on='State_Clean',right_on='State',how='left')
                    print(df_district.columns)
                    df_district=df_district.rename(columns={'State_x':'State'})
                    df_district.drop(columns=['State_y'], inplace=True)
                    
                  
                    df_district=df_district[df_district['State'].isin(['- State has reflected only Adhoc &  First Ins'])==False]

                    #%%
                    df_state = df_district.groupby(['Head','State','Relevant_Date']).agg({'Enrolment_No': 'sum','Avg_Number_Of_Students_Availed_Meals_No': 'sum','PAB_Approval_Children_No':'sum'}).reset_index()
                    sum_df = df_state.groupby(['State','Relevant_Date']).agg({'Enrolment_No': 'sum','Avg_Number_Of_Students_Availed_Meals_No': 'sum','PAB_Approval_Children_No':'sum'}).reset_index()
                    sum_df['Head'] = 'Total'

                   

                    df_bal_vatika=pd.DataFrame(columns=df_state.columns)
                    df_bal_vatika['Head']=['Bal Vatika']
                    df_state=pd.concat([df_bal_vatika,df_state, sum_df], ignore_index=True)
                    df_state[['State','Relevant_Date']]=  df_state[['State','Relevant_Date']].ffill()
                    df_state[['State','Relevant_Date']]=  df_state[['State','Relevant_Date']].bfill()
                   
                    df_state['PAB_Approval_Children_No1']= df_state['PAB_Approval_Children_No'].replace(0,np.nan)
                    df_state['Enrolment_No1']= df_state['Enrolment_No'].replace(0,np.nan)
                   
                    df_state['Avg_Student_Availed_Meals_Vav_PAB_Approval_Pct']=(df_state['Avg_Number_Of_Students_Availed_Meals_No']/df_state['PAB_Approval_Children_No1'])*100
                    df_state['Avg_Student_Availed_Meals_Vav_Enrolment_Pct']=(df_state['Avg_Number_Of_Students_Availed_Meals_No']/df_state['Enrolment_No1'])*100
                   
                    df_state.drop(columns=['PAB_Approval_Children_No1','Enrolment_No1'], inplace=True)


                    df_state['Covered_Under_PM_POSHAN_No']=np.nan
                    df_state['Coverage_Pct']=np.nan
                    df_state['PM_POSHAN_Report_Yr']=report_yr
                    
                    
                    df_state['State_Clean']= df_state['State'].apply(lambda x:state_rewrite.state((str(x).lower())).split('|')[-1].title())
                    df_state=pd.merge(df_state, df_st_alpha[['State','State_Alpha_Code']],left_on='State_Clean',right_on='State',how='left')
                    df_state=df_state.rename(columns={'State_x':'State'})
                    df_state.drop(columns=['State_y'], inplace=True)

                    df_state["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    #%%
                    for j in df_district.State.unique().tolist():
                               Upload_Data('KPMG_PM_POSHAN_STUDENT_COVERAGE_temp_santonu',df_state,j,['MySQL'])
                               Upload_Data('KPMG_PM_POSHAN_DIST_COVERAGE_temp_santonu',df_district,j,['MySQL'])
                    
                  
                except:
                    error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
                    nc_df=pd.DataFrame()
                    nc_df['State']=[st]
                    nc_df['PM_POSHAN_Report_Yr']=[report_yr]
                    nc_df['Error_Msg']=[error_msg]
                    nc_df['Table_Name']=['PM_POSHAN_DIST_COVERAGE']
                    nc_df['Relevant_Date']=today.date()
                    nc_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    nc_df.to_sql('KPMG_PM_POSHAN_STUDENT_STATUS_TABLE', con=engine, if_exists='append', index=False)

        #%%
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')