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
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
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
#%%
def Upload_Data(table_name, data, db: list):
   
    query1=f"select max(Relevant_Date) as Max from {table_name} where Estimates='Final'"
    db_max_date1 = pd.read_sql(query1,engine)["Max"][0]
    df1=data[data['Estimates'].isin (['Final'])==True]
    df1=df1.loc[df1['Relevant_Date'] > db_max_date1]

    query=f"select max(Estimated_On) as Max from {table_name} where Estimates!='Final'"
    db_max_date = pd.read_sql(query,engine)["Max"][0]
    df2=data[data['Estimates'].isin (['Final'])==False]
    
    if db_max_date==None:
        db_max_date=pd.to_datetime('2022-09-30', format='%Y-%m-%d').date()
        df2=df2.loc[df2['Estimated_On'] >= db_max_date]
    else:
        df2=df2.loc[df2['Estimated_On'] > db_max_date]
    
    print(df1.info())
    print(df2.info())

    if 'MySQL' in db:
        df1.to_sql(table_name, con=engine, if_exists='append', index=False)
        df2.to_sql(table_name, con=engine, if_exists='append', index=False)
       

    if 'Clickhouse' in db:

        click_max_date = client.execute(f"select max(Relevant_Date) from {table_name}")
        click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
        query2 =f"select * from {table_name} WHERE Relevant_Date > '" + click_max_date +"';" 

        df = pd.read_sql(query2,engine)
        client.execute(f"INSERT INTO {table_name} VALUES",df.values.tolist())
        print("Data uplodedin Ch")


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

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format='%Y-%m-%d',Month_end=True):

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

def get_links(sect_id):
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://desagri.gov.in',
            'Referer': 'https://desagri.gov.in/statistics-type/advance-estimates/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36 Edg/117.0.2045.60',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
        }

        data = {
            'action': 'stats_SessionFilter',
            'stat_session': sect_id,
            'cat_id': '123',
            'tax_id': 'statistics-type',
        }

        r = requests.post('https://desagri.gov.in/wp-admin/admin-ajax.php', headers=headers, data=data)
        soup1=BeautifulSoup(r.content)
        soup1=BeautifulSoup(r.content)
        file=[i for i in soup1.find_all(class_="mtli_attachment mtli_pdf") if 'english' in i.text.lower()]
        file=[i['href'] for i in file]
        return file 
        

def read_link(link,file_name,s3_folder):
    os.chdir('C:/Users/Administrator/AGRI/DOES')
    path=os.getcwd()
    headers = {
     'Accept': '*/*',
     'Accept-Language': 'en-US,en;q=0.9',
     'Connection': 'keep-alive',
     'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
     'Origin': 'https://desagri.gov.in',
     'Referer': 'https://desagri.gov.in/statistics-type/advance-estimates/',
     'Sec-Fetch-Dest': 'empty',
     'Sec-Fetch-Mode': 'cors',
     'Sec-Fetch-Site': 'same-origin',
     'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36 Edg/117.0.2045.60',
     'X-Requested-With': 'XMLHttpRequest',
     'sec-ch-ua': '"Microsoft Edge";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
     'sec-ch-ua-mobile': '?1',
     'sec-ch-ua-platform': '"Android"',}
    
    
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

def driver_parameter():
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
        }

    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs) 
    options.add_experimental_option('excludeSwitches', ['enable-automation']) 

    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument('--incognito')
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--use-fake-ui-for-media-stream")
    options.add_argument("--use-fake-device-for-media-stream")
    options.add_experimental_option("prefs", prefs)
    
def get_page_content(url,driver_path):
    options=driver_parameter()
    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
    driver.get(url)
    time.sleep(10)
    driver.implicitly_wait(10)
    # ele1=driver.find_element(By.XPATH, '//*[@id="details-button"]')
    # time.sleep(2)
    # ele1.click()
    time.sleep(2)

    soup=BeautifulSoup(driver.page_source)
    df=pd.read_html(driver.page_source)
    return soup

estimate_dict={
                '2nd':'Second',
                '3':'Third',
                '3rd':'Third',
                '4':'Fourth',
                'ofFirst':'First'
                }   
#%%

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')

   
    job_start_time = datetime.datetime.now(india_time)
    table_name = "DOES_FOODGRAIN_ADVANCE_ESTIMATES_RANDAOM_DATA"
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
            
        os.chdir('C:/Users/Administrator/AGRI/DOES')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
 
        url='https://desagri.gov.in/statistics-type/advance-estimates/'
        robot.add_link(url)
        max_rel_date = pd.read_sql("select max(Estimated_On) as Date from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA where Estimates!='Final'", con=engine)['Date'][0]
        if max_rel_date==None:
           max_rel_date=pd.to_datetime('2022-09-30', format='%Y-%m-%d').date()
        else:
           max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()

        max_rel_date1 = pd.read_sql("select max(Estimated_On) as Date from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA where Estimates='Final'", con=engine)['Date'][0]
        max_rel_date1=pd.to_datetime(str(max_rel_date1), format='%Y-%m-%d').date()
        max_rel_date=min(max_rel_date,max_rel_date1)


        print(type(max_rel_date))
        
        os.chdir(r'C:\Users\Administrator\AGRI\DOES')
        delete_pdf=os.listdir(r"C:\Users\Administrator\AGRI\DOES")
        for file in delete_pdf:
                os.remove(file)
        

        #%%
        soup=get_page_content(url,driver_path)
        page=soup.find_all(class_="mb-4",id="stat_sess_filter")[0]
        sector_id={i['value']:i.text for i in page.find_all('option') if i['value']!=''}
        #%%

        ############### Getting Page Links#################################################################
        sector_id={k:v for k,v in sector_id.items() if (int(v.split('-')[0])>=2022)}
        
        for k,v in sector_id.items():
            links=get_links(k)
            substring_to_remove = 'http://desagri.gov.in/wp-content/uploads/2024/06/English.pdf'
            links = [link for link in links if substring_to_remove not in link]
            print(links)
            if len(links)>0:
                 for lnk in links:
                    print(lnk)
                    date=str(lnk).split('http://desagri.gov.in/wp-content/uploads')[-1].split('Time')[0].split('-Series-')[0].strip()
                    estimate=str(lnk).split('http://desagri.gov.in/wp-content/uploads')[-1].split('Time')[-1].split('-Series-')[-1].split('-AE')[0]
                    crop_yr=str(lnk).split('http://desagri.gov.in/wp-content/uploads')[-1].split('Time')[-1].split('-Series-')[-1].split('-AE')[1].split('-English')[0]
                    print(date)
                    date=convert_date_format(date,input_format='/%Y/%m/',Month_end=True)
                    filename=f"Foodgrain_{date}_{estimate}_AE_CY{crop_yr}.pdf"
                   
                    
                    if date>max_rel_date:  
                        print(date)
                        curr_estimates=estimate_dict[str(lnk).split('Time-Series-Production')[1].split('AE')[0].replace('-','')]
 
                #%%

                        s3_folder='AGRI/ADVANCE_ESTIMATES'
                        xl_lnk=read_link(lnk,filename,s3_folder)
    
                        pdf_to_excel('C:/Users/Administrator/AGRI/DOES')
                        time.sleep(10)
    
    
                        os.chdir('C:/Users/Administrator/AGRI/DOES')
                        f1 = glob.glob(os.path.join(os.getcwd(), "*.xlsx"))[0]
                        print(f1)
                    #%%
                        
                        df=get_desire_table(f1,'Advance Estimates of Production of',monthly=True,qtr=False)
                        cur_cy=row_col_index_locator(df,['Advance Estimates of Production of'])

                        cur_cy=df.iloc[cur_cy[1],cur_cy[0]]
                        cur_cy=re.findall('\d+-\d+', cur_cy)
                        
                        unit=row_col_index_locator(df,['tonnes'])
                        unit=df.iloc[unit[1],unit[0]].replace('(','').replace(')','')
                        
                        
                        strart=row_col_index_locator(df,['Crop'])
                        end=row_col_index_locator(df,["included in Other Rabi Pulses"])
                        df.iloc[strart[1],-1]=cur_cy[0]
                        
                        
                        if len(end)==0:
                            df=df.iloc[strart[1]:,strart[0]:]
    
                        else:
                            df=df.iloc[strart[1]:end[1],strart[0]:]
                    
                        try:
                            del_col=row_col_index_locator(df,['Target'],take_min=False)[0]
                            df.drop(df.columns[[del_col]], axis=1, inplace=True)
                        except:
                            pass
                        
                        try:
                            end_row=row_col_index_locator(df,['Data for the year'],take_min=False)[1]
                            if end_row>10:
                                df=df.iloc[:end_row,:]
                        except:
                            pass

                        df.reset_index(drop=True,inplace=True)
                        try:
                            start_col=row_col_index_locator(df,['first','second','third','fourth',"fourth"],take_min=True)[0]
                        except:
                            start_col=-1

                        # start_col=row_col_index_locator(df,['first','second','third','fourth',"fourth"],take_min=True)[0]
                        # l1=[-i for i in range(len(df.columns.to_list())+1-start_col)]
                        # l1.insert(1, 1)  
                        def process_final_estimates(df):
                            df=df.iloc[:,:start_col]
                            df.reset_index(drop=True,inplace=True)
                            df=df.replace('',np.nan)
                            for i in range(0,len(df.columns)):
                                df[df.columns[i]]=df[df.columns[i]].ffill(axis=0)
                                
                
                            df.reset_index(drop=True,inplace=True)
                            
                            df.columns=df.iloc[1,:]
                            df=df.iloc[1:,:]
                            estimates='Final'
                            strart_2=row_col_index_locator(df,['Rice'])
                            df=df.iloc[strart_2[1]:,:]
                            df['Estimates']=estimates
                            df['Estimates']= df['Estimates'].apply(lambda x:x.strip())
                            df=df.replace('',np.nan)
                            df['Crop']=df['Crop'].ffill(axis=0)
                            l3=['Total']
                            df=row_modificator(df,l3,1,row_del=True)
                            df = pd.melt(df, id_vars=['Crop','Season','Estimates'], var_name='Crop_year', value_name='Value_Lakh_MT')
                            return df

                        def process_other_estimates(df,l1,estimates):
                            l1=[0,1,-1]
                            df=df.iloc[:,l1]
                            df.reset_index(drop=True,inplace=True)
                            df.columns=df.iloc[0,:]
                            df=df.iloc[1:,:]
                            
                            
                            df.reset_index(drop=True,inplace=True)
                            df=df.replace('',np.nan)
                            df[df.columns[0]]=df[df.columns[0]].ffill(axis=0)
                            df[df.columns[1]]=df[df.columns[1]].ffill(axis=0)
                            df.reset_index(drop=True,inplace=True)
                            
                            strart_2=row_col_index_locator(df,['Rice'])
                            df=df.iloc[strart_2[1]:,:]
                            
                            
                            # estimates1=row_col_index_locator(df,['Advance Estimate'])
                            
                            # estimates=df.iloc[estimates1[1],estimates1[0]]
                            # print(estimates.split()[0])
                            # estimates=estimates.split()[0]
                            # df=df.iloc[estimates1[1]+1:,:]
    

                            df=df.replace('',np.nan)
                            df['Crop']=df['Crop'].ffill(axis=0)
                            l3=['Total']
                            df=row_modificator(df,l3,1,row_del=True)
                            df['Estimates']=estimates
                            df['Estimates']= df['Estimates'].apply(lambda x:x.strip())
                            df = pd.melt(df, id_vars=['Crop','Season','Estimates'], var_name='Crop_year', value_name='Value_Lakh_MT')
                            return df
                        df2=process_final_estimates(df)
                        # df1=process_other_estimates(df,l1)
                        df1=process_other_estimates(df,[0,1,-1],curr_estimates)

                        df2['Crop']=df2['Crop'].apply(lambda x:' '.join(re.findall(r'[a-zA-Z]+', x)))
                        df1['Crop']=df1['Crop'].apply(lambda x:' '.join(re.findall(r'[a-zA-Z]+', x)))

                        df=pd.concat([df1,df2])
                        df['Estimated_On']=df['Crop_year']
                        df['Estimated_On']=np.where((df["Estimates"]=="First"),df['Estimated_On'].apply(lambda x:str(x).split('-')[0]+'-09-30'),df['Estimated_On'])
                        df['Estimated_On']=np.where((df["Estimates"]=="Second"),df['Estimated_On'].apply(lambda x:'20'+str(x).split('-')[1]+'-02-28'),df['Estimated_On'])
                        df['Estimated_On']=np.where(df["Estimates"]=="Third",df['Estimated_On'].apply(lambda x:'20'+str(x).split('-')[1]+'-05-31'),df['Estimated_On'])
                        df['Estimated_On']=np.where((df["Estimates"]=="Fourth"),df['Estimated_On'].apply(lambda x:'20'+str(x).split('-')[1]+'-08-31'),df['Estimated_On'])
                        df['Estimated_On']=np.where((df["Estimates"]=="Final"),df['Estimated_On'].apply(lambda x:str(int('20'+str(x).split('-')[1])+1)+'-01-31'),df['Estimated_On'])
                        
                        df['Value_Lakh_MT']=df['Value_Lakh_MT'].apply(lambda x:str(x).replace('--','').replace('@',''))
                        df["Value_Lakh_MT"] =pd.to_numeric(df["Value_Lakh_MT"],errors='coerce',downcast='float')

                        
                        df['Estimated_On']=df['Estimated_On'].apply(lambda x:convert_date_format(x,Month_end=False))
                        df['Estimated_On'] = pd.to_datetime(df['Estimated_On'], format='%Y-%m-%d')
                        df['Estimated_On']=df['Estimated_On'].dt.date
                        df['Crop']= df['Crop'].apply(lambda x:x.strip())
                        
                        df=df.replace('--',np.nan)
                        if unit not in ['Lakh Tonnes',"Million Tonnes",'Production in Lakh Tonnes']:
                            raise Exception("Please Check Unit")
                        
                        def get_date(x):
                            x=x.split('-')[1]
                            dt='20'+x+'-06'+'-30'
                            dt=convert_date_format(dt,Month_end=False)
                            return dt
                            
                        df['Relevant_Date']=df['Crop_year'] 
                        df['Relevant_Date']=df['Relevant_Date'].apply(lambda x:get_date(x))
                        df['Relevant_Date'] = pd.to_datetime(df['Relevant_Date'], format='%Y-%m-%d')
                        df['Relevant_Date']=df['Relevant_Date'].dt.date
                        
                        df['Unit']=unit
                        df['Value_Lakh_MT']=np.where((df["Unit"]=="Million Tonnes"),df['Value_Lakh_MT']*10,df['Value_Lakh_MT'])
                        # df['Unit']='Lakh Tonnes'
    
                        df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        df=drop_duplicates(df)
                        df.drop(['Unit'], axis=1,inplace=True)


                    #%%
                        Upload_Data('DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA',df,['MySQL'])
                        os.remove(f1)
                        os.remove(xl_lnk)
                        #%%
                        engine.execute('Delete from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_NO_PIT_RANDAOM_DATA')


                        q1='''select Crop,Season,Estimates,Crop_year,Value_Lakh_MT,Relevant_Date,Runtime  
                        from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA where Estimates!='Final' and Estimated_On=(select max(Estimated_On) from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA where Estimates!='Final');'''
                        df1 = pd.read_sql(q1,engine)
                        
                        q2='''select Crop,Season,Estimates,Crop_year,Value_Lakh_MT, Relevant_Date,Runtime  from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA where Estimates='Final' and 
                           Relevant_Date<=(select max(Relevant_Date) from DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_RANDAOM_DATA 
                           where Estimates='Final');'''
                        df2 = pd.read_sql(q2,engine)
                        final_df=pd.concat([df1,df2])
                        final_df.to_sql('DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_NO_PIT_RANDAOM_DATA', con=engine, if_exists='append', index=False)
                        MySql_CH.ch_truncate_and_insert('DOES_FOODGRAIN_PROD_ADVANCE_ESTIMATES_NO_PIT_RANDAOM_DATA')
                    #%%
                    else:
                        print('Data already Collected')
                   


                                    
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
