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
def City_state_mapping(df,column,new_column):
    raw=df[str(column)]
    dic ={}
    state = []
    for i in raw:
        print(f'-------------->{i}')
        if ((i !=None) and (i !='-') and (i!='Others')):
            try:
                states =  state_rewrite.state((i.lower()))
                print(states)
                dic[i.lower()] = states.split('|')[-1].upper()
                print(states.split('|')[-1].upper())
            except:
                dic[i.lower()] = None
        else:
            dic[i] = None

    clean = []
    for i in df[str(column)]:
        try:
           clean.append(dic[i.lower()])
        except:
           clean.append(dic[i])
    
    df[str(new_column)] = clean
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
    dropdown_element = driver.find_element(By.ID, "txtSelectCategory") 
    dropdown = Select(dropdown_element)
    dropdown.select_by_visible_text('Food Grain Bulletin')
    # dropdown.select_by_value("Food Grain Bulletin")
    time.sleep(3)
    
    soup=BeautifulSoup(driver.page_source)
    return soup

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

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format='%B %Y'):
    try:
        input_datetime = dt.strptime(str(input_date),input_format)
        output_date = input_datetime.strftime(output_format)
    except:
        try:
            input_datetime = dt.strptime(str(input_date),'%b%Y')
            output_date = input_datetime.strftime(output_format)
        except:
            input_datetime = dt.strptime(str(input_date),'%B%Y')
            output_date = input_datetime.strftime(output_format)

   
    output_date=pd.to_datetime(str(output_date), format='%Y-%m-%d')+ MonthEnd(1)
    output_date=output_date.date()
    return output_date
        

def S3_upload(filename,bucket_folder):
    ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
    ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
    BUCKET_NAME = 'adqvests3bucket'
    os.chdir('C:/Users/Administrator/FCI')
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
    os.chdir('C:/Users/Administrator/FCI')
    path=os.getcwd()
    r = get_request_session(link)
    with open(file_name, 'wb') as f:
        f.write(r.content)
    files = glob.glob(os.path.join(path, "*.pdf"))
    print(files)
    file=files[0]   
    S3_upload(file_name,s3_folder)
    return file
    
def extract_table_using_plumber(pdf_file,search_str):
    df_list=[]
    page_list=[]
    with pdfplumber.open(pdf_file) as pdf:
         for i in range(len(pdf.pages)):
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
               
               
def convert_date_format_1(input_date,output_format='%Y-%m-%d',input_format='%d.%m.%Y'):
    input_datetime = dt.strptime(str(input_date), input_format)
    output_date = input_datetime.strftime(output_format)
    lnk_date=pd.to_datetime(str(output_date), format='%Y-%m-%d').date()
    return output_date

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

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
def Column_ffill(df,row_index):
    df.fillna('#', inplace=True)
    for c in range(df.shape[1]):
        if df.iloc[row_index,c]=='#':
            df.iloc[row_index,c]=df.iloc[row_index,c-1]
            print(df.iloc[row_index,c])
    
    df=df.replace('#',None)
    return df

def process_fpd_df(df1,fy_yr):
    df1=Column_ffill(df1,0)
    df1[df1.columns[0]]=df1[df1.columns[0]].ffill(axis=0)    
    df1.columns=df1.iloc[0,:]   

    row_start=row_col_index_locator(df1,['Andhra'])[1]
    end_row=row_col_index_locator(df1,['Total'])[1]
    df1.drop(columns=[i for i in df1 if ((i !=fy_yr) and (i!=df1.columns[0]))],inplace=True)


    df1=df1.iloc[row_start-1:end_row,:]
    df1.columns=df1.iloc[0,:]
    df1=df1.iloc[1:,:]
    df1.reset_index(drop=True,inplace=True)
    df1.columns=[i.replace('\n',' ') for i in df1.columns]
    
    df1=get_renamed_columns(df1,{'Sates/Uts':'State','Wheat':'Wheat','Coarse':'Coarse grain','Rice':'Rice'})
    df1['State']=df1['State'].apply(lambda x:x.replace('\n',''))
    df1['State']=df1['State'].apply(lambda x:re.findall('[A-Za-z]+', x.replace('\n','')))
    df1['State']=df1['State'].apply(lambda x:' '.join(x).upper())
    df1['State']=df1['State'].replace({'H P':'HIMACHAL PRADESH','Uttarpradesh':'UTTAR PRADESH','J K':'JAMMU AND KASHMIR'})
    
    return df1
def get_renamed_columns(df,col_dict):
    df.fillna('#', inplace=True)
    for k,v in col_dict.items():
        print(k)
        try:
            ele=[item for item in df.columns if k.lower() in item.lower()]
            k1=df.columns.to_list().index(ele[0])
            df=df.rename(columns={f"{df.columns[k1]}":f"{col_dict[k]}"})
        except:
            pass
    
    df.reset_index(drop=True,inplace=True)
    df=df.replace('#',np.nan)
    return df
#%%

#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = "FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA"
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
        os.chdir('C:/Users/Administrator/FCI')
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path=os.getcwd()
        url='https://dfpd.gov.in/Home/DocumentReport?language=1'
        robot.add_link(url)
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Date from FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA", con=engine)['Date'][0]
        max_rel_date=pd.to_datetime(str(max_rel_date), format='%Y-%m-%d').date()
        print(type(max_rel_date))
        next_mon=max_rel_date+MonthEnd(1)
        next_mon=next_mon.date()
        fy_yr=get_financial_year(next_mon)

        delete_pdf=os.listdir(r"C:\Users\Administrator\FCI")
        

        ############### Getting Page Links#################################################################
        soup=get_page_content(url,driver_path)
        page=soup.find_all('tr')
        date_link=[i.find_all('td')[1:3] for i in page]
        date_link={i[0].text:i[1].find('a', href=True,target="_blank") for i in date_link if len(i)!=0}
        date_link={k:'https://dfpd.gov.in'+v['href'] for k,v in date_link.items() if re.findall('WriteReadData',v['href'])}
        print(date_link)
        date_link={convert_date_format(k.replace(',','')):v for k,v in  date_link.items()}
        date_link={k:v for k,v in date_link.items() if k>max_rel_date}
        ##################################################################################################
        #%%

        if len(date_link)==0:
            print("No new data available")
        else:
            for rel_date,link in date_link.items():
                print(rel_date,link)
                robot.add_link(link)
                filename=f"FPD_FOOD_GRAIN_PROCUREMENT_{rel_date}.pdf"
                s3_folder='AGRI/PROCUREMENT'
                pdf_file=read_link(link,filename,s3_folder)
                time.sleep(10)

                os.chdir('C:/Users/Administrator/FCI')
                #%%
                search_str='Procurement of Rice, Wheat and Coarse grain'
                df1=extract_table_using_plumber(pdf_file,search_str)
                df1=process_fpd_df(df1,fy_yr)
                df_final = pd.melt(df1, id_vars=['State'], var_name='Commodity', value_name='Procurement_Lakh_MT')
                
                df_final['Relevant_Date']=rel_date
                df_final["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                df_final=df_final.replace("#",np.nan)
                df_final['Commodity']=df_final['Commodity'].replace({'Coarse grain ???? ????':'Coarse grain'})
                df_final['Commodity']=df_final['Commodity'].apply(lambda x:x.replace('???? ????',''))

                #################################################################    
                df_final = df_final.reset_index(drop=True)
                df_final=df_final[df_final.iloc[:,0].str.contains("SatesUts")==False]
                df_final=df_final[df_final.iloc[:,0].str.contains("In Lakh Tons")==False]
                df_final['Procurement_Lakh_MT']=np.where((df_final["Procurement_Lakh_MT"]==''),np.nan,df_final["Procurement_Lakh_MT"])
                df_final=df_final.replace("Neg",np.nan)
                df_final['Production_Lakh_MT']=np.nan
                
                df_final['State']=df_final['State'].apply(lambda x:x.replace('NET (Tirpura)','TRIPURA').strip())
                df_map=City_state_mapping(df_final,'State','State_Clean')
                output = pd.merge(df_final, df_map[['State','State_Clean']], on='State', how = 'left')

                output=output[['State','State_Clean_y','Production_Lakh_MT','Procurement_Lakh_MT','Commodity','Relevant_Date','Runtime']]
                output=output.rename(columns={'State_Clean_y':'State_Clean'})
                output['Relevant_Date'] = pd.to_datetime(output['Relevant_Date'],format='%Y-%m-%d')
                output['Relevant_Date']=output['Relevant_Date'].dt.date
                output=drop_duplicates(output)
                
                Upload_Data('FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA',output,['MySQL'])
                Upload_Data('FPD_FOODGRAIN_PROCUREMENT_MONTHLY_CUMU_DATA',output,['Clickhouse'])

                os.remove(pdf_file)
                
         
        log.job_end_log(table_name,job_start_time, no_of_ping)


    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
