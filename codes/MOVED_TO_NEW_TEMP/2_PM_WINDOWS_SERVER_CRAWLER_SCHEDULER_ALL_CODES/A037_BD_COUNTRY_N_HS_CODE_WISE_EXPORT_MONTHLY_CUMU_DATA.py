import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
warnings.filterwarnings('ignore')
import numpy as np
import json
from selenium import webdriver
import math
import ssl
import glob
from dateutil.relativedelta import relativedelta as rel_del
from requests.exceptions import ConnectionError
ssl._create_default_https_context = ssl._create_unverified_context
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
from fiscalyear import *
import fiscalyear
from calendar import day_name
from calendar import month_name
from urllib.parse import quote

ssl._create_default_https_context = ssl._create_unverified_context
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import adqvest_s3
import boto3
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
# days       = datetime.timedelta(1)
  
#%%
def Upload_Data_to_sql(table_name, data):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    data.to_sql(table_name, con=engine, if_exists='append', index=False)

    
     

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
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = 'AKIAYCVSRU2U3XP2DB75'
    ACCESS_SECRET_KEY ='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'
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
    os.chdir('C:/Users/Administrator/MOM')
    path=os.getcwd()
    r=requests.get(link,verify=False)
    r.raise_for_status()
    # r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.xlsx"))
    # print(files)
    file=files[0]   
    # S3_upload(file_name,s3_folder)
    # print(f'This File loded In S3--->{file_name}')
    # print(file)
    # os.remove(file)
    return file
    

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
                # print(f"Found--{j}")
                # print(f"Column--{i}")
                index2.append(i)
                
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
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

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format="%b '%y",Month_end=True):
    try:
        input_datetime = dt.strptime(str(input_date),input_format)
        output_date = input_datetime.strftime(output_format)
    except:
        input_datetime = dt.strptime(str(input_date),'%b, %Y')
        output_date = input_datetime.strftime(output_format)

    if Month_end==True:
        output_date=pd.to_datetime(str(output_date), format=output_format)+ MonthEnd(1)
        output_date=output_date.date()

    return output_date

def get_date_from_fy(y,m):
    print(y,m)
    try:
        y='20'+y.split('-')[1]
        y=re.findall(r'\d{4}',y)[0]
    except:
        y=int(y)
    
    # print(y)
    fiscalyear.setup_fiscal_calendar(start_month=7)
    m=m.replace('.','').replace('Fab','Feb')
    try:
        month_num=datetime.datetime.strptime(m, '%B').month
    except:
        month_num=datetime.datetime.strptime(m, '%b').month
        
    if month_num>6:
        month_num=month_num-6
    else:
        month_num=month_num+6
        
    fm = FiscalMonth(int(y), month_num)
    fm_start = fm.start.strftime("%Y-%m-%d")
    fm_end = fm.end.strftime("%Y-%m-%d")
    return fm_end
  
def get_sorted_links(link_date,max_rev_date):
    df1=pd.DataFrame()
    new_li=[]
    new_date=[]
    for k,v in link_date.items():
        if v>max_rev_date:
            new_li.append(k)
            new_date.append(v)

    df1['Links']=new_li
    df1['Date']=new_date
    return df1

def get_page_content(url):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=1000,
                                    chromium_sandbox=True)
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content())
        return soup

def get_date_from_string(input_txt):
    # print(input_txt)
    txt=input_txt.strip().title()
    txt=txt.split('The Month Of')[1].strip().replace(' -','-').replace('- ','-')
    try:
        mon=txt.split()[0].split('-')[1].strip()
    except:
        mon=txt.split()[0].strip()
        
    fy_year=txt.split()[1]
    date=get_date_from_fy(fy_year,mon)
    date=pd.to_datetime(str(date), format='%Y-%m-%d').date()
    # print(date)
    return date

def process_bd_export_data(f1,sheet,date):
    # df=pd.read_excel(link,'2 Digit')
    df1=pd.read_excel(f1, sheet_name=sheet)
    # df1=pd.read_excel(excel_file, sheet_name='4 digit')
    df_x=df1.copy()
    if ((len(row_col_index_locator(df_x,['Country-wise']))!=0)):
        if (len(row_col_index_locator(df_x,['HS Code']))!=0):
            df=df1.copy()
            # date=df.iloc[row_col_index_locator(df,['Period'],take_min=True)[1],row_col_index_locator(df,['Period'],take_min=True)[0]]
            # date=date.replace('- ','-')
            # # month=date.split('Period:')[1].strip().split()[0].split('-')[1]
            
            # if len(date.split('Period:')[1].strip().split())==1:
            #     month=date.split('Period:')[1].strip().split()[0].split('-')[0]
            # else:
            #     month=date.split('Period:')[1].strip().split()[0].split('-')[1]
            
                
            
            # if month=='':
            #     month=date.split('Period:')[1].strip().split()[0].split('-')[0]
            #     # month='July'
                
            # if len(date.split('Period:')[1].strip().split())==1:    
            #     year=date.split('Period:')[1].strip().split('-')[1]
            # else:
            #     year=date.split('Period:')[1].strip().split()[1]
                
            # year='2021'
            # month='July'

            td_si=row_col_index_locator(df,['HS Code'],take_min=False)[1]
            
            try:
                td_ei=row_col_index_locator(df,['Total'])[1]
                df=df.iloc[td_si:td_ei,:]
            except:
                df=df.iloc[td_si:,:]
                
            df.columns=df.iloc[row_col_index_locator(df,['HS Code'],take_min=False)[1],:]
            df=row_modificator(df,['Data Source NBR','USD Rate','Country'],0,row_del=True)
            df.reset_index(drop=True,inplace=True)
          
            
            df['Country']=df['Country'].apply(lambda x:x.split(':',1)[1].strip() if((x!='#')) else x).replace({'#':np.nan})
            df['Country']=df['Country'].ffill(axis=0)
            
            df['HS_Code']=df['HS Code'].apply(lambda x:x.split(':',1)[0].strip() if x!='#' else x).replace({'#':np.nan})
            df['Commodity']=df['HS Code'].apply(lambda x:x.split(':',1)[1].strip() if x!='#' else x).replace({'#':np.nan,';':','})
            df['Commodity']=df['Commodity'].apply(lambda x:str(x).replace(';',',').replace('.','').title().strip() if x!=np.nan else x)
            df=df[(df['HS Code']=='#')==False]
            df.drop(columns=['HS Code'], inplace=True)

            
            df['Relevant_Date'] =date
            df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
            
            df=get_renamed_columns(df,{'USD':'Value_USD','Value':'Value_USD'})
            df['Value_USD']=df['Value_USD'].apply(lambda x:float(x))
            df=drop_duplicates(df)
            df=df[['Country','HS_Code','Commodity','Value_USD','Relevant_Date','Runtime']]
        
        else:
            df=pd.DataFrame()

    else:
        df=pd.DataFrame()
    return df

def generate_cum_to_monthly_table(base_table_name,new_table):

    fy_start_dates=pd.read_sql(f"SELECT distinct Relevant_Date FROM {base_table_name} WHERE MONTH(Relevant_Date) = 7;", con=engine)
    fy_start_dates=fy_start_dates.Relevant_Date.to_list()
    fiscalyear.setup_fiscal_calendar(start_month=7)
    
    for dt in fy_start_dates:
        # dt=fy_start_dates[-2]
        fiscalyear.setup_fiscal_calendar(start_month=7)
        if dt.month>=7:
            yr=dt.year+1
        else:
            yr=dt.year
        
        fy = FiscalYear(yr)
        final_df=pd.DataFrame()
        for m in range(0,12):
            if m==0:
                current_month=fy.start.date()+rel_del(months=m, day=31)
                prev_month=fy.start.date()+rel_del(months=m, day=31)
            else:
                current_month=fy.start.date()+rel_del(months=m, day=31)
                prev_month=fy.start.date()+rel_del(months=m-1, day=31)
            

            curr_mon_df=pd.read_sql(f"SELECT * from {base_table_name} where Relevant_Date='{str(current_month)}';", con=engine)    
            prev_mon_df=pd.read_sql(f"SELECT * from {base_table_name} where Relevant_Date='{str(prev_month)}';", con=engine)    
            
            merged_df=pd.merge(curr_mon_df, prev_mon_df[['Country','HS_Code','Value_USD','Relevant_Date']],on=['Country','HS_Code'],how='left')
            # merged_df['Value_USD']=abs(merged_df['Value_USD_x']-merged_df['Value_USD_y'])
            merged_df['Relevant_Date_x']=pd.to_datetime(merged_df['Relevant_Date_x'])
            merged_df['Value_USD']=np.where(merged_df['Relevant_Date_x'].dt.month!=7,(merged_df['Value_USD_x']-merged_df['Value_USD_y']),merged_df['Value_USD_x'])
            merged_df['Value_USD']=np.where(merged_df['Value_USD'].isna(),merged_df['Value_USD_x'],merged_df['Value_USD'])
            final_df=pd.concat([final_df,merged_df])
            
        final_df=final_df[['Country', 'HS_Code', 'Commodity','Value_USD','Relevant_Date_x','Runtime']]
        final_df=final_df.rename(columns={'Relevant_Date_x':'Relevant_Date'})
        final_df['Relevant_Date']=final_df['Relevant_Date'].dt.date
        Upload_Data_to_sql(new_table,final_df)
        # final_df.to_sql(new_table, con=engine, if_exists='append', index=False)

#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BD_COUNTRY_N_HS_CODE_WISE_EXPORT_MONTHLY_CUMU_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        #%%
        os.chdir('C:/Users/Administrator/MOM')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
#%%
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()
        # max_rel_date=pd.to_datetime('2010-01-31', format='%Y-%m-%d').date()
        
        
        delete_pdf=os.listdir(r"C:\Users\Administrator\MOM")
        for file in delete_pdf:
                os.remove(file)
                        
        #%%
        # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error\Bangladesh Exim")
        # f1='C:/Users/Santonu/Desktop/ADQvest/Error files/today error/Bangladesh Exim/Bangladesh_EXIM.xlsx'
        #%%
        url='https://epb.gov.bd/site/view/epb_export_data/'
        robot.add_link(url)
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            # 'Cookie': '_ga=GA1.1.2088649012.1704193935; _ga_XL3REV1DHW=GS1.1.1704355039.8.0.1704355041.0.0.0',
            'If-Modified-Since': 'Thu, 04 Jan 2024 05:20:10 GMT',
            'If-None-Match': '"1704345610-gzip"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 Edg/120.0.0.0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
        }
        
    #%%
        while True:
            r = requests.get(url, headers=headers,verify=False)
            soup = BeautifulSoup(r.content)
            # soup=get_page_content(url)
            export_year=soup.find_all(id="category")[0]
            export_year=[i['value'] for i in export_year.find_all('option') if i['value']!='']
            export_year=sorted(export_year,reverse=True)
            if len(export_year)>0:
                export_year=[export_year[0]]
                print(export_year)
                break
        
            
    #%%
        for exp_yr in export_year:
                time.sleep(30)
                max_rel_date_df = pd.read_sql("select distinct Relevant_Date as Relevant_Date from BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA", con=engine)
                page_url='https://epb.gov.bd/site/view/epb_export_data/'+str(exp_yr)
                
                r1 = requests.get(page_url, headers=headers,verify=False)

                # soup1=get_page_content(page_url)
                soup1 = BeautifulSoup(r1.content)
                links=soup1.find_all('td')
                links=[i.find_all('a') for i in links if i!=None]
                
                all_links=[item for sublist in links for item in sublist]
                all_links={'https:'+i['href']:get_date_from_string(i.text.strip()) for i in all_links if 'country wise export' in i.text.lower().strip()}
                # all_links={'https:'+i['href']:get_date_from_string(i.text.strip()) for i in all_links if (('country wise export' in i.text.lower().strip())|('country wise products wise export' in i.text.lower().strip()))}

                sorted_df=get_sorted_links(all_links,max_rel_date)
                sorted_df=sorted_df.sort_values(by='Date', ascending=False)
                sorted_df=sorted_df[sorted_df['Date'].isin (max_rel_date_df.Relevant_Date.tolist())==False]
                # sorted_df=sorted_df[sorted_df['Date'].isin ([pd.to_datetime('2020-11-30', format='%Y-%m-%d').date()])==False]
                # sorted_df=sorted_df[sorted_df['Date'].isin ([pd.to_datetime('2019-04-30', format='%Y-%m-%d').date()])==False]
                # sorted_df=sorted_df[sorted_df['Date'].isin ([pd.to_datetime('2018-12-31', format='%Y-%m-%d').date()])==False]
                # sorted_df=sorted_df[sorted_df['Date'].isin ([pd.to_datetime('2018-11-30', format='%Y-%m-%d').date()])==False]

                sorted_df.reset_index(drop=True,inplace=True)
                print(sorted_df)
                print(f"Financial_year-->{exp_yr}")
                # export_year.remove(exp_yr)
                
    #%%     
                
                if len(sorted_df)>0:
                    for i in range(sorted_df.shape[0]):
                        print(i)
                        bd_exp_td=pd.DataFrame()
                        bd_exp_fd=pd.DataFrame()
                        bd_exp_sd=pd.DataFrame()
                        bd_exp_ed=pd.DataFrame()
                        
                        rel_date=sorted_df['Date'][i]
                        link=sorted_df['Links'][i]

                        name='BANGLADESH_COUNTRY_WISE_EXPORT'
                        # robot.add_link(link)
                    
                        filename=f"{name}_{rel_date}.xlsx"
                        s3_folder='BANGLADESH_EXIM'
                        excel_file=read_link(link,filename,s3_folder)
                        # print(excel_file)
                       
                        try:
                            td=process_bd_export_data(excel_file,'2 Digit',rel_date)
                        except:
                            td=process_bd_export_data(excel_file,'2 digit',rel_date)

                        
                        try:
                            fd=process_bd_export_data(excel_file,'4 digit',rel_date)
                        except:
                            fd=process_bd_export_data(excel_file,'4 Digit',rel_date)
                        try:
                          sd=process_bd_export_data(excel_file,'6 digit',rel_date)
                        except:
                          sd=process_bd_export_data(excel_file,'6 Digit',rel_date)

                        ed=process_bd_export_data(excel_file,'8 Digit',rel_date)
                        
                        bd_exp_td=pd.concat([bd_exp_td,td])
                        bd_exp_fd=pd.concat([bd_exp_fd,fd])
                        bd_exp_sd=pd.concat([bd_exp_sd,sd])
                        bd_exp_ed=pd.concat([bd_exp_ed,ed])

                        Upload_Data_to_sql('BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA',bd_exp_td)
                        Upload_Data_to_sql('BD_4_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA',bd_exp_fd)
                        Upload_Data_to_sql('BD_6_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA',bd_exp_sd)
                        Upload_Data_to_sql('BD_8_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA',bd_exp_ed)


                        # bd_exp_td.to_sql('BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA', con=engine, if_exists='append', index=False)
                        # bd_exp_fd.to_sql('BD_4_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA', con=engine, if_exists='append', index=False)
                        # bd_exp_sd.to_sql('BD_6_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA', con=engine, if_exists='append', index=False)
                        # bd_exp_ed.to_sql('BD_8_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA', con=engine, if_exists='append', index=False)
                        os.remove(excel_file)

           #%%
                        generate_cum_to_monthly_table('BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA','BD_2_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_DATA')
                        generate_cum_to_monthly_table('BD_4_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA','BD_4_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_DATA')
                        generate_cum_to_monthly_table('BD_6_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA','BD_6_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_DATA')
                        generate_cum_to_monthly_table('BD_8_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_CUMU_DATA','BD_8_DIGIT_EXPORT_COUNTRY_N_HS_CODE_WISE_MONTHLY_DATA')

                           

        #%%
    
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
