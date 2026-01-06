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
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
import numpy as np
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

#%%
engine = adqvest_db.db_conn()
client = ClickHouse_db.db_conn()

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days


#%%
def Upload_Data(table_name, data, db: list):
    query=f"select max(Relevant_Date) as Max from {table_name}"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    
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
    # end_date = pd.to_datetime('2020-05-01', format='%Y-%m-%d').date()
    print(start_date)
    if start_date>=end_date:
        time_range=[]
    else:
        time_range = pd.date_range(str(start_date), str(end_date), freq='10D')
        # time_range = pd.date_range(str(end_date), str(start_date), freq='15D')[::-1]

    return time_range
def pdf_to_excel(file_path,key_word="",OCR_doc=False):
    os.chdir(file_path)
    path=os.getcwd()
    download_path=os.getcwd()
    pdf_list = glob.glob(os.path.join(path, "*.pdf"))
    print(pdf_list)
    matching = [s for s in pdf_list if key_word in s]
    print('Matching')
    print(matching)
                   
    
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
                print(download.path())
                file_name = download.suggested_filename
                # wait for download to complete
                download.save_as(os.path.join(download_path, file_name))
                page.go_back()


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
        if len(row_index)>0:
            df.loc[row_index[0],new_col]=df.iloc[row_index[0],col_idx]
    
    if fill_type=='B':
        df[new_col]=df[new_col].bfill(axis=0)
    else:
        df[new_col]=df[new_col].ffill(axis=0)
    return df 
def process_data(df):
    df['Particulars']=df['Particulars'].apply(lambda x:x.replace("^",''))  
    df['Unit']=df['Particulars']
    
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
    df["Unit"]=df["Unit"].replace({'Bn':'Billion','Mn':'Million','Rs Mn.':'INR Million','Mn.':'Million','Rs. Bn.':'INR Billion','Rs. Bn':'INR Billion','Rs. Bn.':'INR Billion'})

    l2=['Particulars','No. of Trading Days','Client Base','Gross Client Acquisition','Avg Client Funding Book','Number of Orders',
        'Average Daily Orders','Unique MF SIPs Registered',"Angel's ADTO",'Retail Turnover Market Share']
    
    df=row_filling(df,0,l2,new_col='Segment')
    df['Unit']=df['Unit'].ffill(axis=0)
    df = pd.melt(df, id_vars=['Particulars','Segment','Unit'], var_name='Relevant_Date', value_name='Value')
    df=df[df['Relevant_Date'].isin(['M-o-M Growth (%)','YoY Growth (%)','QoQ Growth (%)','M-o-M\nGrowth (%)'])==False]
    df=df.replace('#',np.nan)
    df=df.replace('',np.nan)

    l3=["Angel's ADTO",'Retail Turnover Market Share']
    df=row_modificator(df,l3,0,row_del=True)
    df['Value']=np.where((df["Unit"]=="%"),df['Value']*100,df['Value'])


    return df 

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
                    xl_lnk=read_link(link,filename,s3_folder)

                    pdf_to_excel('C:/Users/Administrator/BSE/ANGEL_ONE')
                    time.sleep(10)


                    os.chdir('C:/Users/Administrator/BSE/ANGEL_ONE')
                    f1 = glob.glob(os.path.join( os.getcwd(), "*.xlsx"))
                    print(f1)
                    
                    ########################## MONTHLY DATA ##############################################################
                    df=get_desire_table(f1[0],'Particulars',monthly=True,qtr=False)
                    if len(df)!=0:
                        df=df.replace('',np.nan)
                        df.dropna(how='all', inplace=True,axis=0)

                        a=['M-o-M Growth (%)','YoY Growth (%)']
                        if any(item in a for item in df.columns.tolist())==False:
                            strart=row_col_index_locator(df,['M-o-M Growth','YoY Growth'])
                            if df.iloc[:,0].str.lower().str.contains('Average Daily Turnover'.lower()).any()==True:
                                end=row_col_index_locator(df,["Average Daily Turnover"])
                                df=df.iloc[strart[1]:end[1],:]
                                df.reset_index(drop=True,inplace=True)
                                
                            else:
                                end=row_col_index_locator(df,["commodity"])
                                df=df.iloc[strart[1]:,:]
                                df.reset_index(drop=True,inplace=True)
                                
                            df.columns=df.iloc[row_col_index_locator(df,['M-o-M Growth','YoY Growth'])[1],:]
                            df = df.drop(row_col_index_locator(df,['M-o-M Growth','YoY Growth'])[1])   
                            try:
                                df.drop(columns=['#'], inplace=True)
                            except:
                                 pass   
                            
                            df.drop(columns=[col for col in df.columns if df[col].nunique() == 1], inplace=True)
                            df.reset_index(drop=True,inplace=True)

                            end_index=row_col_index_locator(df,['Q1','Q2','Q3','Q4'])
                            if len(end_index)!=0:
                                  df=df.iloc[:end_index[1],:]     


                        df=process_data(df)
                        df['Relevant_Date']=df['Relevant_Date'].apply(lambda x:convert_date_format(x))
                        df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

                        df=drop_duplicates(df)
                        Upload_Data('BSE_ANGEL_ONE_BUSINESS_UPDATES_MONTHLY_DATA',df,['MySQL'])
 
                    ############################## QUARTERLY DATA ##############################################

                    # df1=get_desire_table(f1[0],'Particulars',monthly=False,qtr=True)
                    # df1=df1.replace('',np.nan)
                    # df1.dropna(how='all', inplace=True,axis=0)
                    # if len(df1)>5:
                    #     end=row_col_index_locator(df1,["Thanking you"])
                    #     df1=df1.iloc[:end[1],:]
                    #     df1.reset_index(drop=True,inplace=True)  
                    #     df1=df1.replace('#',np.nan)
                    #     df1.dropna(how='all', inplace=True,axis=1)      
                    #     df1.drop(columns=[col for col in df1.columns if df1[col].nunique() == 1], inplace=True)
                    #     df1.reset_index(drop=True,inplace=True)
                    #     df1=process_data(df1)
                    #     df1['Relevant_Date']=df1['Relevant_Date'].apply(lambda x:convert_qtr_date(x)) 
                        
                    #     df1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    #     df1=drop_duplicates(df1)
                    ##############################################################################################   
                
                    os.remove(f1[0])
                    os.remove(xl_lnk)
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
