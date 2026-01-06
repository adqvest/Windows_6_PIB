import sqlalchemy
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
import re
import io
import glob
import csv
import calendar
warnings.filterwarnings('ignore')
import numpy as np
from selenium import webdriver
import requests
from time import sleep
import re
import datetime as datetime
import numpy as np
import sys
import time
from PyPDF2 import PdfFileReader, PdfWriter
from playwright.sync_api import sync_playwright

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta


#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
import ClickHouse_db
import boto3
import dbfunctions

#%%
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()
#****   Date Time ****
import datetime
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
 
#%%
def send_files_s3(key):
    folder_path = 'C:\\Users\\Administrator\\RBI_Wages/'

    filenames = os.listdir(folder_path)

    for filename in filenames:
        dbfunctions.to_s3bucket(folder_path+filename,key)
        
def Upload_Data(table_name, data, db: list):
    query=f"select distinct Relevant_Date as Relevant_Date from {table_name}"
    db_max_date = pd.read_sql(query,engine)
    data=data[data['Relevant_Date'].isin(db_max_date.Relevant_Date.tolist())==False]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        
def row_col_index_locator(df,l1):
    df.reset_index(drop=True,inplace=True)
    df.fillna('#', inplace=True)
    
    index2=[]
    dict1={}
    for j in l1:
        for i in range(df.shape[1]):
            if df.iloc[:,i].str.lower().str.contains(j).any()==True:
                print(f"Found--{j}")
                print(f"Column--{i}")
                index2.append(i)
                row_index=df[df.iloc[:, i].str.lower().str.contains(j) == True].index[0]
                print(f"Row--{row_index}")
                index2.append(row_index)
                break
                
    return index2

#%%

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA'
    if(py_file_name is None):
       py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0
    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            
            
        # os.chdir(r'C:\Users\Administrator\RBI_Wages')
        os.chdir(r'C:\Users\Administrator\RBI_Wages')
        delete_pdf=os.listdir(r"C:\Users\Administrator\RBI_Wages")
        for file in delete_pdf:
            os.remove(file)

        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_directory =r"C:\Users\Administrator\RBI_Wages"
        #%%
        max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA",engine)["Max"][0]
        print(max_rel_date)
        #%%
        # download_directory = r'C:\Users\Administrator\RBI_Wages'
        # driver_path="C:/Users/Santonu/Desktop/ADQvest/Chrome Driver/chromedriver.exe"
        # os.chdir(r"C:\Users\Santonu\Desktop\ADQvest\Error files\today error")
        download_file_path=os.getcwd()
        
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")

        # options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-gpu")
        # options.add_argument("--headless")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
             # "safebrowsing.enabled": True
            }

        options.add_experimental_option("prefs", prefs)
   
        driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
        url = 'https://data.rbi.org.in/DBIE/#/dbie/reports/Statistics/Real%20Sector/Prices%20&%20Wages'
        driver.get(url)
        time.sleep(60)
        # robot.add_link(url)
        df1=pd.read_html(driver.page_source)
        df1=df1[0]
        df1=df1[df1[df1.columns[0]].isin(['Average Daily Wage Rates (in Rs.) in Rural India for Men'])]
        df1.reset_index(drop=True,inplace=True)
        pg_max_dt=pd.to_datetime(df1['To'][0],format='%d-%b-%Y').date()
        if pg_max_dt>max_rel_date:
   
            for row in  driver.find_elements(By.XPATH,"//tr")[1:]:
                try:
                    second_td = row.find_elements(By.XPATH,"./td")[0]
                    if "average daily wage rates" in second_td.text.lower():
                        print(second_td.text)
                        second_td.find_element(By.XPATH,"./a").click()
                        time.sleep(20)
                        break
                except:
                    continue
       
            # time.sleep(20)
            new_window = driver.window_handles[-1]
            driver.switch_to.window(new_window)
            iframe = driver.find_element(By.XPATH,"//iframe[@id='openDocChildFrame']")
            driver.switch_to.frame(iframe)
            time.sleep(20)
            
            
            buttons = driver.find_elements(By.XPATH,"//button")
            for button in buttons:
                    print(button.get_attribute('title').lower())
                    if 'export (ctrl+e)' in button.get_attribute('title').lower():
                        time.sleep(1)
                        button.click()
                        break
                   
            export_buttons = driver.find_elements(By.XPATH,"//button")
            for exp_btn in export_buttons:
                  print(exp_btn.text)
                  if 'export' in exp_btn.text.lower():
                      exp_btn.click()
                      time.sleep(15) 
                      break
            driver.quit()
        #%%
            send_files_s3('RBI/RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA/')
            files = glob.glob(os.path.join(download_directory, "*.xlsx"))
            print(files)
            excel_file=files[0]
            #%%
            # df=pd.read_excel('C:/Users/Santonu/Desktop/ADQvest/Error files/today error/Average Daily Wage Rates (in Rs.) in Rural India for Men.xlsx')
            df=pd.read_excel(excel_file)

            start_index=row_col_index_locator(df,['month']) 
            end_index=row_col_index_locator(df,['note']) 
            df.columns=df.iloc[start_index[1],:]
            df=df.iloc[start_index[1]+1:end_index[1],:]
            df.drop(columns=['#'],inplace=True)
            df.reset_index(drop=True,inplace=True)
            
            df = pd.melt(df, id_vars=['Month','State'], var_name='Type', value_name='Wage_INR')
            df=df[~(df.Month=='#')]
            df['Relevant_Date'] = pd.to_datetime(df['Month'], format='%Y-%m-%d')
            df['Relevant_Date']=df['Relevant_Date'].dt.date
            df['Relevant_Date']=df['Relevant_Date'].apply(lambda x: x  + relativedelta(day=31))
            df=df[~(df.State.isin(['#']))]
            df[['State']]=df[['State']].applymap(lambda x:x.title())
            
            df=df.replace('-',np.nan)
            df[['Wage_INR']]=df[['Wage_INR']].applymap( lambda x: float(x) if pd.notnull(x) else x)
            
            df['Type']=df['Type'].replace("Sweeper","Sweeping/ Cleaning Workers")
            df['Type']=df['Type'].replace("Sowing","Sowing (including Planting/ Transplanting/Weeding workers)")
            df['Type']=df['Type'].replace("Ploughing","Ploughing/Tilling Workers")
            df['Type']=df['Type'].replace("Picking","Picking Workers (includingTea, Cotton, Tobacco &other commercialcrops")
            df['Type']=df['Type'].replace("Harvesting","Harvesting/ Winnowing/ Threshing workers")
            df['Type']=df['Type'].replace("Blacksmit","Blacksmith")
            
            df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            df=df[['State','Type','Wage_INR','Relevant_Date','Runtime']]
            Upload_Data('RBI_ALL_INDIA_RURAL_WAGES_MEN_MONTHLY_DATA',df,['MySQL'])

        #################################################################
        else:
            print('Data upto date')
            driver.quit()
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)

        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
