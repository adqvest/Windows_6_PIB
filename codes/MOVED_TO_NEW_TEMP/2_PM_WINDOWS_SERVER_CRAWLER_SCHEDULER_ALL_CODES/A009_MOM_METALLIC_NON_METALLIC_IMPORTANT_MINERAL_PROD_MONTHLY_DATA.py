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
from dateutil.relativedelta import relativedelta
from requests.exceptions import ConnectionError
ssl._create_default_https_context = ssl._create_unverified_context
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd


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
def update_back_data(table_name,org_df):
    org_dates = pd.read_sql(f"select distinct Relevant_Date as Relevant_Date  from {table_name} where Relevant_Date< (select max(Relevant_Date) from {table_name});",con=engine)
    org_categories = pd.read_sql(f"select distinct Mineral_Type as Mineral_Type  from {table_name};",con=engine)

    org_dates['Relevant_Date']=org_dates['Relevant_Date'].apply(lambda x:str(x))
    org_df['Relevant_Date']=org_df['Relevant_Date'].apply(lambda x:str(x))
     
    df_dates=org_df.Relevant_Date.to_list()
    org_dates=org_dates.Relevant_Date.to_list()
    org_cat=org_categories.Mineral_Type.to_list()
    new_dates=common_dates=set(df_dates) - set(org_dates)
    if len(new_dates)>0:
        df1=org_df[org_df['Relevant_Date'].isin (new_dates)==True]
        df1.to_sql(table_name, con=engine, if_exists='append', index=False)


    common_dates=set(org_dates) & set(df_dates)
    if len(common_dates)>0:
        for mt in org_cat:
            for i in common_dates:
                df=org_df[(org_df['Relevant_Date']==i) & (org_df['Mineral_Type']==mt)]
                if len(df)>0:
                    datewise_count = df['Relevant_Date'].value_counts().to_list()[0]
                    
                    q2=f"select Relevant_Date,count(*) as count from {table_name} where Relevant_Date='{i}' and Mineral_Type='{mt}' group by Relevant_Date,Mineral_Type order by Relevant_Date desc;"
                    cnt= pd.read_sql(q2,con=engine)
                    print('--------------------------------------')

                    if datewise_count==cnt['count'][0]:
                        print(f"data Deleted for---->{i}")
                    
                        engine.execute(f"Delete from {table_name} where Relevant_Date='{i}' and  Mineral_Type='{mt}'")
                        print(f"data Deleted for---->{i}")
                        print(df.info())
                        print('----------------------------------------------------------')
                        df.to_sql(table_name, con=engine, if_exists='append', index=False)
                        print(f"data Uploded for---->{i}")
                        print(df.info())

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
    r=requests.get(link,verify=True)
    r.raise_for_status()
    # r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.pdf"))
    print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    print(f'This File loded In S3--->{file_name}')
    print(file)
    # os.remove(file)
    return file
    

def Upload_Data(table_name, data,mnrl_ty,db: list):
    query=f"select max(Relevant_Date) as Max from {table_name} where Mineral_Type='{mnrl_ty}'"
    data=data[data['Mineral_Type'].isin ([mnrl_ty])==True]
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(data.info())

  
def get_desire_table(link,serach_str,monthly=True,col_search=False):
     xls = pd.ExcelFile(link)
     sheet_names = xls.sheet_names
     print(sheet_names)
     for i in range(len(sheet_names)):
         df=pd.DataFrame()
         df = pd.read_excel(link, sheet_name=sheet_names[i])
         # print(df.columns)
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
         if col_search==True:
              Found=False
              for d in df.columns.to_list():
                 if re.findall(serach_str.lower(), d.lower().replace('\n','')):    
                     print('Sheet Found')
                     # print(df)
                     Found=True
                     break
              if Found==True:
                  break
                
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

def get_page_content(url,download_path):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=1000,
                                    chromium_sandbox=True,
                                    downloads_path=download_path)
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content())
        return soup

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

def process_imported_mineral_df(file,rel_date):
    df=get_desire_table(file,'Provisional Production of Important Minerals',col_search=True,monthly=False)
        
    cur_month=pd.to_datetime(rel_date, format='%Y-%m-%d').date()
    prev_month=cur_month-relativedelta(months=1)
    prev_yr_month=cur_month-relativedelta(months=12)

    cur_month=convert_date_format(str(cur_month),input_format='%Y-%m-%d',output_format='%b-%y',Month_end=False)
    prev_month=convert_date_format(str(prev_month),input_format='%Y-%m-%d',output_format='%b-%y',Month_end=False)
    prev_yr_month=convert_date_format(str(prev_yr_month),input_format='%Y-%m-%d',output_format='%b-%y',Month_end=False)

    si=row_col_index_locator(df,['Mineral'],take_min=True)[1]
    df.drop(df[df[df.columns[1]]==''].index,inplace=True)
    df.columns=df.iloc[si,:]
    df.drop(columns=[col for col in df.columns if df[col].nunique() == 1], inplace=True)
    df.drop(columns=[col for col in df.columns if col not in [cur_month,prev_month,prev_yr_month,'Mineral','Unit']], inplace=True)
    df=df.drop(index=[row_col_index_locator(df,['Mineral'],take_min=True)[1]])
    df.reset_index(drop=True,inplace=True)
    

    df=df.rename(columns={'Mineral':'Minerals'}).replace('**',np.nan)
    df = pd.melt(df, id_vars=['Minerals','Unit'], var_name='Relevant_Date', value_name='Quantity')

    df['Relevant_Date']=df['Relevant_Date'].apply(lambda x: convert_date_format(str(x),output_format='%Y-%m-%d',input_format='%b-%y',Month_end=True))
    df['Quantity']=np.where((df["Unit"].isin(['MMT'])),df['Quantity']*1000000,df['Quantity'])
    df['Quantity']=np.where((df["Unit"].isin(['THT'])),df['Quantity']*1000,df['Quantity'])
    df['Mineral_Type']='Important'
    df['Unit']='MT'
    print(df)
    return df

def process_minerals_df(df,mnrl_ty='Metallic'):
   
    si=row_col_index_locator(df,['Current Month'],take_min=True)[1]
    if mnrl_ty!='Metallic':
        ei=row_col_index_locator(df,['Total_Non_Metallic'],take_min=True)[1]
    else:
        ei=row_col_index_locator(df,['Total Metallic'],take_min=True)[1]
        
    end_col=row_col_index_locator(df,['Cumulative Previous Year'],take_min=True)[0]
    
    unit=df.iloc[row_col_index_locator(df,['Quantity in'],take_min=True)[1],row_col_index_locator(df,['Quantity in'],take_min=True)[0]]


    df=df.iloc[si:ei,:end_col]
    df.drop(columns=[col for col in df.columns if df[col].nunique() == 1], inplace=True)
    cur_date=df.iloc[row_col_index_locator(df,['Current Month'],take_min=True)[1]+1,row_col_index_locator(df,['Current Month'],take_min=True)[0]]
    df.iloc[row_col_index_locator(df,['Quantity'],take_min=True)[1],row_col_index_locator(df,['Quantity'],take_min=True)[0]-1]='Minerals'
    
    df=df.iloc[row_col_index_locator(df,['Quantity'],take_min=True)[1]:,:]
    df.columns=df.iloc[row_col_index_locator(df,['Quantity'],take_min=True)[1],:]
    df=df.drop(index=[row_col_index_locator(df,['Quantity'],take_min=True)[1]])
    df.reset_index(drop=True,inplace=True)
    df['Quantity']= df['Quantity'].apply(lambda x: str(x).split('(')[0])

    
    df['Relevant_Date']=convert_date_format(str(cur_date),output_format='%Y-%m-%d',input_format='%B, %Y',Month_end=True)
    df['Mineral_Type']=mnrl_ty
    
    if (('Quantity in Million Tonne' in unit) and ('Value in Rs. Crore' in unit)):
        
        df=df.rename(columns={'Value':'Value_INR_Cr'}).replace('**',np.nan)
        df['Minerals']=df['Minerals'].apply(lambda x:x.replace('*',''))
        df['Quantity']= df['Quantity'].apply(lambda x: float(x))
        df['Unit']='MT'
        df['Quantity']=np.where((~df["Minerals"].isin(['Diamond'])),df['Quantity']*1000000,df['Quantity'])
        df['Unit']=np.where((df["Minerals"].isin(['Diamond'])),'Carat',df['Unit'])
    
    print(cur_date)
    print(df)
    return df,unit,cur_date
#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA'
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
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()
        # max_rel_date=pd.to_datetime('2021-12-31', format='%Y-%m-%d').date()
        
        
        delete_pdf=os.listdir(r"C:\Users\Administrator\MOM")
        for file in delete_pdf:
                os.remove(file)
                        
        #%%
        url='https://mines.gov.in/webportal/content/monthly-summary-on-minerals-and-non-ferrous-metal'
        robot.add_link(url)
        soup=get_page_content(url,download_path)
        page=soup.find_all("tr")
        date_links={i.find('a')['href']:convert_date_format(str(i.text.split('\xa0')[0].split('(')[0].split('\n')[-1].strip()),output_format='%Y-%m-%d',input_format='%B-%Y',Month_end=True) for i in page if i.find('a')!=None}
        sorted_df=get_sorted_links(date_links,max_rel_date)
        sorted_df=sorted_df.sort_values(by='Date', ascending=False)
        sorted_df.reset_index(drop=True,inplace=True)
        mineral_df=pd.DataFrame()
        if len(sorted_df)>0:
            for i in range(sorted_df.shape[0]):
                
                rel_date=sorted_df['Date'][i]
                link=sorted_df['Links'][i]
                name='Prod_metallic_non_metallic_imported_minerals'
                robot.add_link(link)
            
                filename=f"{name}_{rel_date}.pdf"
                s3_folder='MINISTRY_OF_MINES'
                pdf_file=read_link(link,filename,s3_folder)
                
            
                pdf_to_excel('C:/Users/Administrator/MOM')
                time.sleep(10)

            
                os.chdir('C:/Users/Administrator/MOM')
                f1 = glob.glob(os.path.join( os.getcwd(), "*.xlsx"))[0]
                print(f1)
        
                #Important Mineral production
                imported_mi=process_imported_mineral_df(f1,rel_date)
              
                #Non-Metallic Mineral production
                nm_m_prod=get_desire_table(f1,'Production of Minerals: Non-Metallic Minerals')
                nm_m_prod,unit_nm,Curr_date=process_minerals_df(nm_m_prod,mnrl_ty='Non-Metallic')

                #Metallic Mineral production
                mm_prod=get_desire_table(f1,'Production of Minerals: Metallic Minerals')
                mm_prod,unit_mm,curr_dt_mm=process_minerals_df(mm_prod,mnrl_ty='Metallic')
                
            
                final_df=pd.concat([nm_m_prod,mm_prod,imported_mi])
                final_df['Minerals']=final_df['Minerals'].apply(lambda x:x.replace('.','').strip())
                final_df["Minerals"]=final_df["Minerals"].apply(lambda x:x.replace('\n',' ').strip().title())
                final_df["Minerals"]=final_df["Minerals"].replace({'Other met Minerals':'Other Metallic Minerals','Other non-metallic':'Other Non Metallic Minerals'})

                final_df['Quantity']=final_df['Quantity'].apply(lambda x:float(x))
                final_df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                mineral_df=pd.concat([mineral_df,final_df])

                os.remove(f1)
                os.remove(pdf_file)
                #%%
                
                
            mineral_df=drop_duplicates(mineral_df)
            Upload_Data('MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA',mineral_df,'Important',['MySQL'])
            Upload_Data('MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA',mineral_df,'Metallic',['MySQL'])
            Upload_Data('MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA',mineral_df,'Non-Metallic',['MySQL'])

            update_back_data('MOM_METALLIC_NON_METALLIC_IMPORTANT_MINERAL_PROD_MONTHLY_DATA',mineral_df)
        else:
            print('Data is upto date')    
            
        #%%
    
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
