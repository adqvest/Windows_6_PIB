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
import pdfplumber
from selenium import webdriver
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
warnings.filterwarnings('ignore')
#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
import MySql_To_Clickhouse as MySql_CH

robot = Robots(__file__)

#%%
engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()




#%%
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

def update_back_data(table_name,org_df):
    org_dates = pd.read_sql(f"select distinct Relevant_Date as Relevant_Date  from {table_name} where Relevant_Date< (select max(Relevant_Date) from {table_name});",con=engine)    
    org_dates['Relevant_Date']=org_dates['Relevant_Date'].apply(lambda x:str(x))
    org_df['Relevant_Date']=org_df['Relevant_Date'].apply(lambda x:str(x))
     
     
    df_dates=org_df.Relevant_Date.to_list()
    org_dates=org_dates.Relevant_Date.to_list()

    common_dates=set(org_dates) & set(df_dates)
    if len(common_dates)>0:
        for i in common_dates:
            df=org_df[(org_df['Relevant_Date']==i)==True]
            datewise_count = df['Relevant_Date'].value_counts().to_list()[0]
            
            q2=f"select Relevant_Date,count(*) as count from {table_name} where Relevant_Date='{i}' group by Relevant_Date order by Relevant_Date desc;"
            cnt= pd.read_sql(q2,con=engine)
            print('--------------------------------------')

            if datewise_count>=cnt['count'][0]:
                print(f"data Deleted for---->{i}")
            
                engine.execute(f"Delete from {table_name} where Relevant_Date='{i}'")
                print(f"data Deleted for---->{i}")
                print(df.info())
                print('----------------------------------------------------------')
                df.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"data Uploded for---->{i}")
                print(df.info())
                
def Upload_Data(table_name, data, db: list):
    query=f"select distinct Relevant_Date as Relevant_Date from {table_name}"
    db_max_date = pd.read_sql(query,engine)
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(data.info())

    if 'Clickhouse' in db:

        click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query2 =f"select * from {table_name} WHERE Relevant_Date > '" + click_max_date +"';" 

        df = pd.read_sql(query2,engine)
        client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())
        print("Data uplodedin Ch")


def get_page_content(url,dt_start,download_path):
    dt_end = datetime.date(dt_start.year, dt_start.month, dt_start.day) + datetime.timedelta(10)
    if dt_end>today.date():
        end_date = datetime.date(yesterday.year, yesterday.month, yesterday.day)
        dt_end = pd.to_datetime(str(end_date), format='%Y-%m-%d').date()


    dt_end=convert_date_format(str(dt_end),output_format='%d/%m/%Y',input_format='%Y-%m-%d',Month_end=False)
    dt_start=convert_date_format(str(dt_start),output_format='%d/%m/%Y',input_format='%Y-%m-%d',Month_end=False)
    

    print(dt_end)
    print(dt_start)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=1000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto(url)
        page.locator('//*[@id="ddlAnnType"]').select_option(label='All Segments')
        page.locator('//*[@id="txtFromDt"]').fill(str(dt_start))
        page.locator('//*[@id="txtToDt"]').fill(str(dt_end))
        page.locator('//*[@id="btnSubmit"][1]').click()
        time.sleep(3)
        
        soup = BeautifulSoup(page.content())
    return soup


def get_date_range(table_name):

    query=f"select max(Relevant_Date) as RD from {table_name}"
    max_date = pd.read_sql(query, con=engine)
    if (max_date['RD'].isnull().all()):
       start_date = pd.to_datetime('2018-01-01', format='%Y-%m-%d').date()
    else:
        start_date = max_date['RD'][0]
       

    start_date = datetime.date(start_date.year, start_date.month, start_date.day) + datetime.timedelta(30)
    end_date = datetime.date(yesterday.year, yesterday.month, yesterday.day)
   
    
    print(start_date)
    if start_date>=end_date:
        time_range=[]
    else:
        time_range = pd.date_range(str(start_date), str(end_date), freq='10D')
    
    return time_range


def row_col_index_locator(df,l1):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j.lower()).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                row_index=df[df.iloc[:, i].str.lower().str.contains(j.lower()) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                break
                
    return index2

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

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format="%b '%y",Month_end=True):

    try:
       input_datetime = dt.strptime(str(input_date),input_format)
       output_date = input_datetime.strftime(output_format)
    except:
        try:
            input_datetime = dt.strptime(str(input_date),"%B '%y")
            output_date = input_datetime.strftime(output_format)
        except:
            try:
                input_datetime = dt.strptime(str(input_date),"%B'%y")
                output_date = input_datetime.strftime(output_format)
            except:
                input_datetime = dt.strptime(str(input_date),"%b'%y")
                output_date = input_datetime.strftime(output_format)
    
    if Month_end==True:
        output_date=pd.to_datetime(str(output_date), format=output_format)+ MonthEnd(1)
        output_date=output_date.date()
        
    return output_date

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    os.chdir('C:/Users/Administrator/BSE/ANGEL_ONE')
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
    os.chdir('C:/Users/Administrator/BSE/ANGEL_ONE')
    path=os.getcwd()
    headers = {
    'authority': 'www.google-analytics.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    # 'content-length': '0',
    'origin': 'https://www.bseindia.com',
    'referer': 'https://www.bseindia.com/',
    'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36 Edg/117.0.2045.41',
    }
    r=requests.get(link,headers =headers,verify=True)
    r.raise_for_status()
    # r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.pdf"))
    print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    print(f'This File loded In S3--->{file_name}')

    return file
    
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df


def get_sorted_links(link_date,col_name=''):
    df1=pd.DataFrame()
    new_li=[]
    new_date=[]
    for k,v in link_date.items():
        new_li.append(k)
        new_date.append(v)

    df1['Particulars']=new_li
    df1[col_name]=new_date
    return df1

def get_specfic_dict_key(di,l1): 
    multi_value_dict={}
    for i in l1:
        for k,v in di.items():
            if re.findall(i, k):
                multi_value_dict[k] =v
            
    return multi_value_dict
def get_desire_table(link,serach_str,monthly=True,qtr=False):
     xls = pd.ExcelFile(link)
     sheet_names = xls.sheet_names
     print(sheet_names)
     for i in range(len(sheet_names)):
         df=pd.DataFrame()
         df = pd.read_excel(link, sheet_name=sheet_names[i])
         df=df.replace(np.nan,'')
         print('Done ',sheet_names[i])
         if monthly==True:
             if df.iloc[:,0].str.lower().str.contains(serach_str.lower()).any()==True:
                print('Sheet Found')
                break
             else:
                if serach_str in df.columns.to_list():
                    print('Sheet Found')
                    break
         elif qtr==True:
             if 'Q' in df.columns.to_list():
                 print('Sheet Found')
                 break
         else:
             df=pd.DataFrame()      
             
     return df
 
def convert_qtr_date(k):
    Q_dict={'Q1':f'y-06-30',
            'Q2':f'y-09-30',
            'Q3':f'y-12-31',
            'Q4':f"y-03-31"}
    
    qtr=k.split('FY')[0].strip()
    st='20'+k.split('FY')[1]

    if qtr=='Q4':
        dt=Q_dict[qtr].replace('y',str(int(st)+1))
    else:
        dt=Q_dict[qtr].replace('y',st) 
        
    return dt    
def row_filling(df,col_idx,result_list=[],new_col='New_col',fill_type='F'):
    df[new_col]=np.nan
    for i in result_list:
        row_index=df[df.iloc[:, col_idx].str.lower().str.contains(i.lower(),case=False,flags=re.IGNORECASE) ==True].index.to_list()
        print(row_index)
        if len(row_index)>0:
            for k in row_index:
                df.loc[k,new_col]=df.iloc[k,col_idx]
                
    if fill_type=='B':
        df[new_col]=df[new_col].bfill(axis=0)
    else:
        df[new_col]=df[new_col].ffill(axis=0)
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

def claen_values(x):
    x=float(str(x).replace(',','').replace('--',''))
    return x
#%%

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')

   
    job_start_time = datetime.datetime.now(india_time)
    table_name = "BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA"
    scheduler = ''
    no_of_ping=0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]

    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
            
        os.chdir('C:/Users/Administrator/BSE/ANGEL_ONE')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()

        url='https://www.bseindia.com/stock-share-price/angel-one-ltd/angelone/543235/corp-announcements/'
        robot.add_link(url)
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()
        print(type(max_rel_date))
        
        os.chdir(r'C:\Users\Administrator\BSE\ANGEL_ONE')
        delete_pdf=os.listdir(r"C:\Users\Administrator\BSE\ANGEL_ONE")
        for file in delete_pdf:
                os.remove(file)
        
        time_range=get_date_range('BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA')
        print(time_range)
        ############### Getting Page Links#################################################################
        if len(time_range)>0:
            for dates1 in time_range:
                dates1=dates1.date()
                print(f'Working on-------------------------------->{dates1}')
                soup=get_page_content(url,dates1,download_path)
                
                page=[i for i in soup.find_all('a',class_="tablebluelink")]
                page2=[i.text for i in soup.find_all('td',class_="tdcolumngrey ng-binding")]
                page3={i.find('a',class_="tablebluelink"):i.find('td',class_="tdcolumngrey ng-binding") for i in soup.find_all('tr') if (i)!=None}
                page4={v.text.split('|')[1].strip():'https://www.bseindia.com'+k['href'] for k,v in page3.items() if (v)!=None}
                print(page4)
                page4=get_specfic_dict_key(page4,['Monthly'])
                
                pg=[i for i in soup.find_all('table',class_="ng-scope")]
                pg2=['' if i==None else  i.find('td',class_="tdcolumngrey ng-binding") for i in pg]
                pg3=['' if i==None else  i.text.split('|')[1].strip() for i in pg2]

                pg5=[i.find_all('tr',class_="ng-scope") for i in pg]
                pg5=['' if len(i)==0 else i[0].find('b',class_="ng-binding").text  for i in pg5]

                dict1=dict(zip(pg3,pg5))
                df1=pd.DataFrame.from_dict([dict1])
                
            
                df1=get_sorted_links(dict1,'Date')
                df2=get_sorted_links(page4,'Links')
                link_df=pd.merge(df1,df2)
                link_df['Date']=link_df['Date'].apply(lambda x:dt.strptime(str(x.strip()),"%d-%m-%Y %H:%M:%S").date())
                print(link_df)
                
        ##################################################################################################
                for i in range(link_df.shape[0]):
                    rel_date=link_df['Date'][i]
                    link=link_df['Links'][i]
                    name='ANGEL_ONE'
                    robot.add_link(link)
    
                    filename=f"{name}_{rel_date}.pdf"
                    s3_folder='BSE/ANGEL_ONE'
                    pdf_file=read_link(link,filename,s3_folder)
                    
                    #%%
                    # pdf_file='C:/Users/Santonu/Desktop/ADQvest/Error files/today error/Angel_One/ANGEL_ONE_2024-05-06.pdf'
                    
                    df_list=[]
                    with pdfplumber.open(pdf_file) as pdf:
                         for i in range(len(pdf.pages)):
                             try:
                                 table2 = pdf.pages[i].extract_tables()
                                 df_x = pd.DataFrame(table2[0],columns=table2[0][0])
                                 df_list.append(df_x)
                             except:
                                 pass
                    df=pd.concat([i for i in df_list[:2]]) 
            
                    
                    #%%
                    if len(df)!=0:
                        df=df.replace('',np.nan)
                        df.dropna(how='all', inplace=True,axis=0)
                        
                        df.dropna(axis=1,how='all',inplace=True)
                        df=row_modificator(df,['Particulars'],0,row_del=True)
                        
                        df['Particulars']=df['Particulars'].apply(lambda x:x.replace("^",''))  
                        df['Unit']=df['Particulars']
                        df['Particulars']=df['Particulars'].apply(lambda x:x.split("(")[0])  
                        df['Particulars']=df['Particulars'].apply(lambda x:x.replace('$','').replace('*','').replace('.','').replace('#','').strip())  
                        df=row_modificator(df,['#'],0,row_del=True)
                        
                        l1=[{'Client Base':'Client Base'},{'Gross Client Acquisition':'Gross Client Acquisition'},
                            {'Avg Client Funding Book ':'Avg Client Funding Book'},
                            {'Avg. Client Funding Book ':'Avg Client Funding Book'},
                            {'Number of Orders':'Number of Orders'},{'Number of Orders':'Number of Orders'},
                            {'Average Daily Orders ':'Average Daily Orders'},{"Angel":"Angel's ADTO"},
                            {"Unique MF SIPs Registered ":'Unique MF SIPs Registered'},
                            {"Retail Turnover Market Share ":"Retail Turnover Market Share"}]
                        df=row_modificator(df,l1,0,update_row=True)
                        
                        
                        df['Unit']=df['Unit'].apply(lambda x:x.split('(')[-1])
                        df['Unit']=df['Unit'].apply(lambda x:x.replace(')',''))
                        
                        unit1=['Rs Bn','Rs Mn','Mn','Bn','Rs Mn.','Mn.','Rs. Bn.','Rs. Bn']
                        unit2=['Rs Bn','Rs Mn','Mn','Bn',"'000",'%','Days','Thousand','Rs Mn.','Mn.','Rs. Bn.','Rs. Bn']

                        df['Unit']=np.where((~df['Unit'].isin(unit1)),df['Unit'].apply(lambda x:x.split()[-1]),df['Unit'])
                        df['Unit']=df['Unit'].apply(lambda x: x.replace("'000",'Thousand'))     
                        df['Unit']=np.where((~df['Unit'].isin(unit2)),np.nan,df['Unit'])
                        df["Unit"]=df["Unit"].replace({'Bn':'Billion','Rs Bn':'INR Billion','Mn':'Million','Rs Mn.':'INR Million','Mn.':'Million','Rs. Bn.':'INR Billion','Rs. Bn':'INR Billion','Rs. Bn.':'INR Billion'})

                        l2=['Particulars','No of Trading Days','Client Base','Gross Client Acquisition','Avg Client Funding Book','Number of Orders',
                            'Average Daily Orders','Unique MF SIPs Registered',"Angel's ADTO",'Retail Turnover Market Share']
                        
                        df=row_filling(df,0,l2,new_col='Metric')
                        df['Unit']=df['Unit'].ffill(axis=0)
                        
                        
                        l4=['Based on Notional Turnover','Based on Option Premium Turnover']
                        df=row_filling(df,0,l4,new_col='Type')
                        df['Type']=np.where(df.Particulars.isin(['Cash ADTO','Commodity ADTO','Cash Turnover Market Share','Commodity Turnover Market Share']),np.nan,df['Type'])
                        df=df[~(df.Particulars==df.Type)]
                        
                        
                        l3=["Angel's ADTO",'Retail Turnover Market Share']
                        df=row_modificator(df,l3,0,row_del=True)
              
                        
                        df = pd.melt(df, id_vars=['Particulars','Metric','Unit','Type'], var_name='Relevant_Date', value_name='Value')
                        df=df[df['Relevant_Date'].isin(['M-o-M Growth (%)','YoY Growth (%)','QoQ Growth (%)','M-o-M\nGrowth (%)'])==False]
                        df=df.replace('#',np.nan)
                        df=df.replace('',np.nan)
                        df['Value']=df['Value'].str.extract('(\d+.\d+|\d+)', expand=False)
                        df['Metric']=df['Metric'].replace({"Angel's ADTO":'ADTO'})

                        df['Relevant_Date']=df['Relevant_Date'].apply(lambda x:convert_date_format(x))
                        df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df['Value']=df['Value'].apply(lambda x:claen_values(x))
                        df = df.dropna(subset=['Particulars'])
    
                        df['Particulars']=df['Particulars'].replace({'Commodity ADTO':'Commodity','Cash ADTO':'Cash','Cash Turnover Market Share':'Cash','Commodity Turnover Market Share':'Commodity'})
                        df['Metric']=np.where((~df['Metric'].isin(['Retail Turnover Market Share','ADTO'])),np.nan,df['Metric'])


                        df=drop_duplicates(df)
                        #%%
                        Upload_Data('BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA',df,['MySQL'])
                        update_back_data('BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA',df)
                        MySql_CH.ch_truncate_and_insert('BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA')
                    ##############################################################################################   
                    os.remove(pdf_file)
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
