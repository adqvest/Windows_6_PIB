import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import dateutil.relativedelta
import timeit
import io
import pdfplumber
import numpy as np
from pytz import timezone
import glob
import time
import re
import itertools
import PyPDF2
import requests
import sqlalchemy
from pandas.io import sql
import os
from bs4 import BeautifulSoup
from dateutil import parser
import sys
import ast
import warnings
warnings.filterwarnings('ignore')
import pymysql
from playwright.sync_api import sync_playwright
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import html5lib
import boto3
#%%
# sys.path.insert(0, 'D:\Adqvest')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import JobLogNew as log
import ClickHouse_db
import adqvest_s3
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days


def Upload_Data(table_name, data, db: list):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(data.info())
        
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
    

def S3_upload(filename,bucket_folder):
    
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
         's3',
         aws_access_key_id=ACCESS_KEY_ID,
         aws_secret_access_key=ACCESS_SECRET_KEY,
         config=boto3.session.Config(signature_version='s3v4',region_name = 'ap-south-1'))
    data_s3 =  open(filename, 'rb')
    s3.Bucket(BUCKET_NAME).put_object(Key=f'{bucket_folder}/'+filename, Body=data_s3)
    data_s3.close()
    print("Data uploaded to S3")

def read_link(link,file_name,s3_folder):
    os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
    # os.chdir('C:/Users/Santonu/Desktop/ADQvest/Error files/today error/EDUCATION')
    path=os.getcwd()
    r=requests.get(link,verify=False)
    r.raise_for_status()
    with open(file_name, 'wb') as f:
        f.write(r.content)
    
    files = glob.glob(os.path.join(path, "*.pdf"))
    file=files[0]  
    # split_pdf(file_name, file_name.split('.pdf')[0], 10)
    # S3_upload(file_name,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    # os.remove(file)
    return file

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

def row_filling(df,col_idx,result_list=[],new_col='New_col',fill_type='F'):
    df[new_col]=np.nan
    for i in result_list:
        row_index=df[df.iloc[:, col_idx].str.lower().str.contains(i.lower(),case=False,flags=re.IGNORECASE) ==True].index.to_list()
        if len(row_index)>0:
            df.loc[row_index[0],new_col]=df.iloc[row_index[0],col_idx]
    
    if fill_type=='B':
        df[new_col]=df[new_col].bfill(axis=0)
    else:
        df[new_col]=df[new_col].ffill(axis=0)
    return df

def Column_ffill_2(df,row_index):
    df.fillna('#', inplace=True)
    for c in range(df.shape[1]):
        if re.findall('#', df.iloc[row_index,c]):
                df.iloc[row_index,c]=df.iloc[row_index,c-1]
                print(df.iloc[row_index,c])
    
    # df=df.replace('#',None)
    return df
def row_modificator(df,l1,col_idx,row_del=False,keep_row=False,update_row=False):
  from collections import ChainMap
  keep_inx=[]
  print(type(l1[0]))
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
                   print(r)
                   df.iloc[j,col_idx]=l1[i]
                   df[df.columns[col_idx]]=df[df.columns[col_idx]].str.strip()
            
  if(keep_row==True):
        drop_list=[i for i in df.index if i not in keep_inx]
        df.drop(index=drop_list,inplace=True)
               
  df.reset_index(drop=True,inplace=True)    
  return df  

def process_aishea_df(df):
       df=df.rename(columns={'Ph.D.':'PhD','M.Phil.':'MPhil'})
       df=Column_ffill_2(df,0)
       row_index=0
       for c in range(2,df.shape[1]):
           print(c)
           df.iloc[row_index,c]=df.iloc[row_index,c]+'_'+df.iloc[row_index+1,c]
           print(df.iloc[row_index,c])
           
       df.columns=df.iloc[0,:]
       df=df.iloc[3:,1:]
       df=row_modificator(df,['#'],0,row_del=True)
       df=df.rename(columns={'State/UTs':'State'})
       df['State']=df['State'].apply(lambda x:x.replace('\n',''))
       df['State']=df['State'].replace({'A & N Islands':'Andaman & Nicobar Islands'})
       df['State']=df['State'].replace({'Jammu and Kashmir':'Jammu & Kashmir'})
       df['State']=df['State'].replace({'D & N Haveli and Daman & Diu':'Dadra & Nagar Haveli and Daman & Diu'})
       df['State']=df['State'].replace({'Haveli and Daman and Diu':'Dadra & Nagar Haveli and Daman & Diu'})
       df['State']=df['State'].replace({'and Daman and Diu':'Dadra & Nagar Haveli and Daman & Diu'})
       
       df['State']=df['State'].replace({'TelanganaThe Dadra and Nagar':'Telangana'})
       df['State']=df['State'].replace({'TelanganaThe Dadra and Nagar Haveli':'Telangana'})
      
       return df
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    

    job_start_time = datetime.datetime.now(india_time)
    table_name = "AISHE_STATE_WISE_EST_ENROLEMENT_AT_VARIOUS_LEVEL_YEARLY_DATA"
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
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from AISHE_STATE_WISE_EST_ENROLEMENT_AT_VARIOUS_LEVEL_YEARLY_DATA", con=engine)['Date'][0]

        # headers = {
        #         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        #         'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        #         'Cache-Control': 'no-cache',
        #         'Connection': 'keep-alive',
        #         # 'Cookie': 'JSESSIONID=2924C3AEFF01A7877E464978A0F69E1F.n3; SERVERID=aishe3; _gid=GA1.3.437885514.1715059463; _gat_gtag_UA_250704415_1=1; _ga_BD6Y88TZEC=GS1.1.1715112310.13.0.1715112310.0.0.0; _ga=GA1.1.774415356.1709815094',
        #         'Pragma': 'no-cache',
        #         'Sec-Fetch-Dest': 'document',
        #         'Sec-Fetch-Mode': 'navigate',
        #         'Sec-Fetch-Site': 'none',
        #         'Sec-Fetch-User': '?1',
        #         'Upgrade-Insecure-Requests': '1',
        #         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        #         'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        #         'sec-ch-ua-mobile': '?0',
        #         'sec-ch-ua-platform': '"macOS"',
        #     }
        url='https://aishe.gov.in/aishe-final-report/'   # Link changed | Pushkar | 21 Jun 2024
        # r = requests.get(url, headers=headers)
        pw = sync_playwright().start()
        browser = pw.firefox.launch(headless = False, ignore_default_args=["start-maximized"])
        context = browser.new_context(java_script_enabled = True,bypass_csp=True)
        page = context.new_page()
        page.goto(url, wait_until='networkidle',timeout=120000)
        time.sleep(5)
        robot.add_link(url)
        soup = BeautifulSoup(page.content(),'lxml')
        time.sleep(5)
        pw.stop()
        links=soup.find_all('tr')
        date_link={'20'+i.find_all('td')[0].text.strip().replace(' - English','').split('-')[1]:'https://aishe.gov.in/'+i.find('a',href=True)['href'] for i in links if i.find('a',href=True)!=None}
        date_link={k+'-03-31':v for k,v in date_link.items() if 'hindi' not in k.lower()}
        print(date_link)
        date_link={pd.to_datetime(k, format='%Y-%m-%d').date():v for k,v in date_link.items() if pd.to_datetime(k, format='%Y-%m-%d').date()>max_rel_date}



        
        

       #%%
        print(date_link)
        if len(date_link)>0:
            for current_date,link in date_link.items():
                print(link)
                print(current_date)
                file_name=f"AISHE_REPORTS_{str(current_date)}.pdf"
                s3_folder='EDUCATION/AISHE'
                pdf_file=read_link(link,file_name,s3_folder)
                os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
                # os.chdir('C:/Users/Santonu/Desktop/ADQvest/Error files/today error/EDUCATION')

    #%%
               
                df_list=[]
                page_list=[]
                with pdfplumber.open(pdf_file) as pdf:
                    for i in range(len(pdf.pages)):
                        print(i)
                        page_text=pdf.pages[i].extract_text()
                        lst=[str(page_text).strip().lower().split('\n')][0]
                        lst=[i.replace('\xa0','').strip() for i in lst ]
                        print(lst[0])
                        if "table6. state-wise enrolment at various levels (including estimation)" in lst:
                            break
                    req_pages=[p for p in range(i,i+3)] 
                
                print(req_pages)
                with pdfplumber.open(pdf_file) as pdf:
                    for i in range(len(pdf.pages)):
                        if i in req_pages:
                          try:
                              table = pdf.pages[i].extract_tables()
                              table2=[i for i in table if len(i)>20]
                              df = pd.DataFrame(table2[0],columns=table2[0][0])
                              df=process_aishea_df(df)
                              df['Relevant_Date'] = pd.to_datetime(str(current_date),format='%Y-%m-%d').date()
                              df_list.append(df)
                          except:
                              pass
                
                df_final=pd.DataFrame()
                df_f=pd.merge(df_list[0],df_list[1],on=['State','Relevant_Date'],how='left')
                df_f=pd.merge(df_f,df_list[2],on=['State','Relevant_Date'],how='left')
                df_final=pd.concat([df_final,df_f])

                df_final.columns=[i.replace('\xa0','_').replace(' ','_').replace('Grand Total','Grand_Total')+'_No' for i in df_final.columns]
                df_final=df_final.rename(columns={'State_No':'State','Relevant_Date_No':'Relevant_Date'})
                del_col=[i for i in df_final.columns if (('total' in i.lower()))]
                del_col=[i for i in del_col if (('grand' not in i.lower()))]
                
                df_final= df_final.drop(del_col, axis=1)            
                df_final['State']=df_final['State'].apply(lambda x:x.replace('\n',''))
                
                df_final=df_final.rename(columns={'Ph.D._Male_No':'PhD_Male_No',
                                                  'Ph.D._Female_No':'PhD_Female_No',
                                                  'M.Phil._Male_No':'MPhil_Male_No',
                                                  'M.Phil._Female_No':'MPhil_Female_No'})
                
              
                
                df_final= df_final.drop(['Grand_Total_Total_No'], axis=1)    
                df_final["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                si=row_col_index_locator(df_final,['andaman'])[1]
                df_final=df_final.iloc[si:,:]
                df_final.reset_index(drop=True,inplace=True) 
                df_final=df_final.replace('#',np.nan)
                df_final=df_final.replace('',np.nan)
                df_final=df_final[['State', 'PhD_Male_No', 'PhD_Female_No', 'MPhil_Male_No',
                       'MPhil_Female_No', 'Post_Graduate_Male_No', 'Post_Graduate_Female_No', 'Under_Graduate_Male_No', 'Under_Graduate_Female_No',
                       'PG_Diploma_Male_No', 'PG_Diploma_Female_No', 'Diploma_Male_No',
                       'Diploma_Female_No', 'Certificate_Male_No', 'Certificate_Female_No',
                       'Integrated_Male_No', 'Integrated_Female_No', 'Grand_Total_Male_No',
                       'Grand_Total_Female_No','Relevant_Date', 'Runtime']]
                
                #%%
                Upload_Data('AISHE_STATE_WISE_EST_ENROLEMENT_AT_VARIOUS_LEVEL_YEARLY_DATA',df_final,['MySQL'])

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
