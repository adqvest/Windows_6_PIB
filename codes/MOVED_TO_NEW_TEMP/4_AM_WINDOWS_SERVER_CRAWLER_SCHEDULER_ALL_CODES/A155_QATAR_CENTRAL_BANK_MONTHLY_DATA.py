import pandas as pd
from dateutil import parser
import datetime as datetime
import numpy as np
from pytz import timezone
import time
import re
import requests
import glob
import os
from bs4 import BeautifulSoup
from dateutil import parser
import sys
import warnings
warnings.filterwarnings('ignore')
from playwright.sync_api import sync_playwright
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

#%%
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import boto3
import adqvest_s3
from botocore.config import Config
import MySql_To_Clickhouse as MySql_CH
import ClickHouse_db
#%%
engine = adqvest_db.db_conn()
connection = engine.connect()
client = ClickHouse_db.db_conn()

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days


#%%
def clean_values(x):
   try:
      x=float(str(x).replace(',', '').replace('*',''))
   except:
      x=np.nan
    
   return x

def last_day_of_month(rdate):
   next_month = rdate.replace(day=28) + datetime.timedelta(days=4)
   return next_month - datetime.timedelta(days=next_month.day)

def convert_date_format(input_date,output_format='%Y-%m-%d',input_format="%b %Y",Month_end=True):
    from datetime import datetime as dt
    from pandas.tseries.offsets import MonthEnd
    try:
       input_datetime = dt.strptime(str(input_date),input_format)
       output_date = input_datetime.strftime(output_format)
    except:
        try:
            input_datetime = dt.strptime(str(input_date),"%B %Y")
            output_date = input_datetime.strftime(output_format)
        except:
            try:
                input_datetime = dt.strptime(str(input_date),"%B'%y")
                output_date = input_datetime.strftime(output_format)
            except:
                input_datetime = dt.strptime(str(input_date),"%b'%y")
                output_date = input_datetime.strftime(output_format)
    
    
    output_date=pd.to_datetime(str(output_date), format='%Y-%m-%d')+ MonthEnd(1)
    output_date=output_date.date()
    return output_date
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

def driver_parameter2(download_directory):
    prefs = {
        "download.default_directory": download_directory,  # Set the download directory
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    }

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--allow-running-insecure-content")  # Allow insecure content
    chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=https://www.qcb.gov.qa")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument("--use-fake-ui-for-media-stream")
    chrome_options.add_argument("--use-fake-device-for-media-stream")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-blink-features")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    return chrome_options


def download_file_via_selenium(url, download_directory,driver_path):
    chrome_options = driver_parameter2(download_directory)
    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=chrome_options)
    driver.get(url)

    # Wait for the page to load
    time.sleep(5)
    driver.implicitly_wait(10)

    # Find and click the 'Monthly Monetary Bulletin' link
    ele1 = driver.find_element(By.XPATH, "//*[contains(text(), 'Monthly Monetary Bulletin')]")
    time.sleep(2)
    ele1.click()
    time.sleep(2)

    # Find and click the download link
    download_link = driver.find_element(By.XPATH, '//*[@id="v-pills-home1"]/div/div[2]/ul/li/a')
    download_link = download_link.get_attribute('href')
    driver.quit()
    return download_link
    
def get_page_content(url,driver_path):
    
    options=driver_parameter()
    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
    driver.get(url)
    driver.implicitly_wait(10)
    ele1=driver.find_element(By.XPATH, "//*[contains(text(), 'Monthly Monetary Bulletin')]")
    time.sleep(2)
    ele1.click()
    time.sleep(2)

    soup=BeautifulSoup(driver.page_source)
    return soup

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
    print("Data uploaded to S3")

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def Upload_Data(table_name,data,table_no,db: list):
    query=f"select max(Relevant_Date) as Max from AdqvestDB.{table_name} where Table_No ='{table_no}'"
    db_max_date = pd.read_sql(query,engine)
    if (db_max_date['Max'].isnull().all()):
       pass
    else:
        db_max_date = db_max_date["Max"][0]
        data=data[data['Table_No'].isin ([table_no])==True]
        data=drop_duplicates(data)
        data=data.loc[data['Relevant_Date'] > db_max_date]
    
    if 'MySQL' in db:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        print("Data uploded in MySQL")
        print(data.info())

    if 'Click_house' in db:
        q1=f"select max(Relevant_Date) from AdqvestDB.{table_name} where Table_No ='{table_no}'"
        ch_max_date = client.execute(q1)
        ch_max_date = str([a_tuple[0] for a_tuple in ch_max_date][0])
        query=f"select * from AdqvestDB.{table_name} where Table_No ='{table_no}'"
        df = pd.read_sql(query,engine)
        client.execute("INSERT INTO AdqvestDB."+table_name+" VALUES",df.values.tolist())
        print("Data uploded in Click_house")

def delete_re_upload(df):
    table_no=df['Table_No'].unique()[0]
    table_name=df['Table_Name'].unique()[0]
    try:
       RD = pd.read_sql(f"Select min(Relevant_Date) as RD from AdqvestDB.QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING where Table_Name ='{table_name}'",con=engine)['RD'].iloc[0]
       query1 = "Delete from AdqvestDB.QATAR_CENTRAL_BANK_MONTHLY_DATA where Table_Name ='{table_name}' and Relevant_Date >= '"+str(RD)+"';"
       engine.execute(query1)
       Upload_Data('QATAR_CENTRAL_BANK_MONTHLY_DATA',df,table_no,'MySQL')
    except:
       Upload_Data('QATAR_CENTRAL_BANK_MONTHLY_DATA',df,table_no,'MySQL')
        
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
   os.chdir('C:/Users/Administrator/AdQvestDir/')
   
   job_start_time = datetime.datetime.now(india_time)
   table_name = "QATAR_CENTRAL_BANK_MONTHLY_DATA"
   scheduler = ''
   no_of_ping = 0

   if(py_file_name is None):
      py_file_name = sys.argv[0].split('.')[0]

   try :
      if(run_by == 'Adqvest_Bot'):
         log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
      else:
         log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


      driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
      download_path=os.getcwd()

      last_date = pd.read_sql("select max(Relevant_Date) as RD from QATAR_CENTRAL_BANK_MONTHLY_DATA;",con=engine)
      last_date = last_date['RD'][0]
      
      url = 'https://www.qcb.gov.qa/EN/Pages/publication.aspx?IndexSelect=1'
      #%%
      soup=get_page_content(url,driver_path)
   
      #%%
      table = soup.find_all('ul',attrs={'class':'list-uploaded'})
      links_date={convert_date_format(i.text.split('-',1)[-1].replace('Download','').strip().replace('-',' ').title(),input_format='%b %Y'):"http://www.qcb.gov.qa" + i.find('a').get('href') for i in table}
      links_date={k:v for k,v in links_date.items() if k>last_date}
      links_date= dict(sorted(links_date.items(), key=lambda item: item[1])) 
      # links_date=sorted(links_date.items())
      if len(links_date)>0:
         for  date,link in links_date.items():
            print(link)
            os.chdir('C:/Users/Administrator/CIBIL_EQUIFAX_CRIF')
            # os.chdir('C:/Users/Santonu/Desktop/ADQvest/Error files/today error/QCB')
            path=os.getcwd()
            file="QATAR_CENTRAL_BANK_"+str(date)+".xls"
            s3_folder='QATAR_CENTRAL_BANK'
             
            headers = {
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                }
            # link='https://www.qcb.gov.qa/PublicationFiles/%D8%A7%D9%84%D9%86%D8%B4%D8%B1%D8%A9%20%D8%A7%D9%84%D8%B4%D9%87%D8%B1%D9%8A%D8%A9%20%D9%84%D8%B4%D9%87%D8%B1%20%D9%85%D8%A7%D8%B1%D8%B3%202024.xls'
            try:
               r = requests.get(link, verify=True,headers=headers, timeout = 60)
               with open(file, 'wb') as f:
                  f.write(r.content)
               
               files = glob.glob(os.path.join(path, "*.xls"))
               S3_upload(file,s3_folder)

               xls = pd.ExcelFile(file)
               sheets = xls.sheet_names
               latest_date=date
            except:
               os.remove(file)
               download_directory = 'C:/Users/Administrator/CIBIL_EQUIFAX_CRIF'
               url = 'https://www.qcb.gov.qa/EN/Pages/publication.aspx?IndexSelect=1'
               link = download_file_via_selenium(url,download_directory,driver_path)
               print(link)
               r = requests.get(link, verify=True,headers=headers, timeout = 60)
               with open(file, 'wb') as f:
                  f.write(r.content)
               
               files = glob.glob(os.path.join(path, "*.xls"))
               S3_upload(file,s3_folder)

               xls = pd.ExcelFile(file)
               sheets = xls.sheet_names
               latest_date=date
                

      
            xls_dict = pd.read_excel(file,sheet_name = None,header=None)

            def search(heading):
                  df = pd.DataFrame()
                  cond = False
                  for sheet in sheets:
                     data = pd.read_excel(file,sheet_name = sheet,header=None)
                     text = data.fillna('').to_string()
                     text = re.sub(r'  +',' ',text).strip()
                     if(re.search(heading, text)):
                        data_rows = data.shape[0]
                        data.reset_index(inplace=True,drop=True)
                        data = data.replace('',np.nan)
                        df = pd.concat([df,data])
                        df = df.replace('.',np.nan)
                        cond = True
                        break
                  return df

            def arabic_removal(df):
                  df = df.fillna("")
                  for i,row in df.iterrows():
                     for col in df.columns:
                        if ((type(row[col])!=float) and (type(row[col])!=int)):
                              row[col] = row[col].encode('ascii', 'ignore').decode('ascii')
                  drop_total = df.iloc[:,0][df.iloc[:,0].str.lower().str.contains("table|end|period|source|average|pre data|revised data",na = False)].index
                  df = df.drop(drop_total)
                  df.reset_index(inplace = True,drop = True)
                  df = df.replace(r'',np.nan)
                  df = df.dropna(axis = 1,how = 'all')
                  df.iloc[:,1] = df.iloc[:,1].ffill()
                  drop_rows = df.iloc[:,1][df.iloc[:,1].isnull()].index
                  df = df.drop(drop_rows)
                  df.reset_index(inplace = True,drop = True)
                  df = df.dropna(axis = 1,how = 'all')
                  df.columns = [x for x in range(df.shape[1])]
                  df = df.dropna(subset=[df.columns[0]])
                  df.reset_index(inplace = True,drop = True)
                  return df

            def calculate_date(table):
                  table.reset_index(inplace = True,drop = True)
                  table=table.drop(index=table[table.iloc[:,0].str.lower().str.contains("primary data")==True].index)

                  table.reset_index(inplace = True,drop = True)

                  table['Unit'] = 'Millions of QR'
                  table['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                  table['Relevant_Date'] = table['a'] +"-"+ table['b'].astype(str)
                  table['Relevant_Date'] = table['Relevant_Date'].str.replace(r'*','')
                  table['Relevant_Date'] = table['Relevant_Date'].str.replace(r'.0$','')
                  del table['a']
                  del table['b']
                  dates = table['Relevant_Date'].tolist()
                  new_dates = []
                  for i in dates:
                     try:
                        rdate = pd.to_datetime(i,format = "%b-%Y").date()
                     except:
                        rdate = pd.to_datetime(i,format = "%B-%Y").date()
                     alldate = last_day_of_month(rdate)
                     new_dates.append(alldate)
                  table['Relevant_Date'] = new_dates
                  table.reset_index(inplace = True,drop = True)
                  drop_total = table["Variable_Name"][table["Variable_Name"].str.lower().str.contains('total',na = False)].index
                  table = table.drop(drop_total)
                  table = table[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
                  return table

            list1 = ['VariableName','x','y','Change Value','Change Pct']
            header1 = pd.DataFrame(np.array(list1).reshape(-1,len(list1)))
            df1 = search('Main Indicators of the Banking Sector')
            col_dictionary = {'Money Supply ( M1 ) The Banking System':'Money Supply M1 The Banking System','Quasi Money ( M2 - M1 ) The Banking System':'Quasi Money M2 - M1 The Banking System','Money Supply ( M2 ) The Banking System':'Money Supply M2 The Banking System',
                              'Money Supply ( M3 ) The Banking System':'Money Supply M3 The Banking System','Foreign Assets ( Net ) The Banking System':'Foreign Assets Net The Banking System','Domestic Credit ( Net ) The Banking System':'Domestic Credit Net The Banking System',
                              'Domestic Assets ( Net ) The Banking System':'Domestic Assets Net The Banking System','Foreign Assets ( Net ) Commercial Banks':'Foreign Assets Net Commercial Banks','Money Supply ( M1 ) The Banking System Change Value':'Money Supply M1 The Banking System Change Value',
                              'Quasi Money ( M2 - M1 ) The Banking System Change Value':'Quasi Money M2 - M1 The Banking System Change Value','Money Supply ( M2 ) The Banking System Change Value':'Money Supply M2 The Banking System Change Value','Money Supply ( M3 ) The Banking System Change Value':'Money Supply M3 The Banking System Change Value',
                              'Foreign Assets ( Net ) The Banking System Change Value':'Foreign Assets Net The Banking System Change Value','Domestic Credit ( Net ) The Banking System Change Value':'Domestic Credit Net The Banking System Change Value','Domestic Assets ( Net ) The Banking System Change Value':'Domestic Assets Net The Banking System Change Value',
                              'Foreign Assets ( Net ) Commercial Banks Change Value':'Foreign Assets Net Commercial Banks Change Value','Money Supply ( M1 ) The Banking System Change Pct':'Money Supply M1 The Banking System Change Pct','Quasi Money ( M2 - M1 ) The Banking System Change Pct':'Quasi Money M2 - M1 The Banking System Change Pct',
                              'Money Supply ( M2 ) The Banking System Change Pct':'Money Supply M2 The Banking System Change Pct','Money Supply ( M3 ) The Banking System Change Pct':'Money Supply M3 The Banking System Change Pct','Foreign Assets ( Net ) The Banking System Change Pct':'Foreign Assets Net The Banking System Change Pct',
                              'Domestic Credit ( Net ) The Banking System Change Pct':'Domestic Credit Net The Banking System Change Pct','Domestic Assets ( Net ) The Banking System Change Pct':'Domestic Assets Net The Banking System Change Pct','Foreign Assets ( Net ) Commercial Banks Change Pct':'Foreign Assets Net Commercial Banks Change Pct'}

            def clean(df):
                  df = df.replace('*',np.nan)
                  df = df.replace('**',np.nan)
                  df = df.dropna(axis = 1,how = 'all')
                  row = df.iloc[:,0][df.iloc[:,0].str.lower().str.contains("the banking system",na = False)].index[0]
                  df = df[row:]
                  df = df.iloc[:,:-1]
                  df = df.dropna(how='all')
                  df.reset_index(inplace = True,drop = True)
                  return df

            clean_df1 = clean(df1)
            header1 = header1.append(clean_df1)
            header1.reset_index(inplace = True,drop = True)
            header1 = header1.drop([14,18])
            header1.reset_index(inplace = True,drop = True)

            def slicing1(df):
               table = pd.DataFrame()
               for i in range(2,df.shape[1]):
                     slice_df = df[[0,i]]
                     slice_df.columns = [x for x in range(slice_df.shape[1])]
                     table = pd.concat([table,slice_df])
               table.columns = ['Variable_Name',"Value"]
               table.reset_index(inplace = True,drop = True)
               return table

            table1 = slicing1(header1)
            table1['Value'] = table1['Value'].replace('y',np.nan)
            table1['merge'] = np.nan
            table1['merge'] = np.where((table1["Variable_Name"].str.lower().str.contains('variablename',na = False)),table1["Value"],table1["merge"])
            table1['merge'] = table1['merge'].ffill()
            drop_row = table1["Variable_Name"][table1["Variable_Name"].str.lower().str.contains('variablename',na = False)].index
            table1 = table1.drop(drop_row)
            table1.reset_index(inplace = True,drop = True)
            table1['Variable_Name'] = [re.sub(r':','',str(x)) for x in table1['Variable_Name']]
            table1["merge1"] = np.nan
            table1["merge1"] = np.where((table1["Value"].isnull()),table1["Variable_Name"],table1["merge1"])
            table1['merge1'] = table1['merge1'].ffill()
            drop_row1 = table1["Value"][table1["Value"].isnull()].index
            table_1 = table1.drop(drop_row1)
            table_1.reset_index(inplace = True,drop = True)
            table_1['merge'] = table_1['merge'].replace(np.nan,'')
            table_1['Variable_Name'] = table_1['Variable_Name']+' '+table_1['merge1']+' '+table_1['merge']
            table_1['Variable_Name'] = table_1['Variable_Name'].str.rstrip()
            table_1['Variable_Name'].replace(col_dictionary,inplace = True)
            del table_1['merge1']
            del table_1['merge']
            table_1['Table_No'] = 'Table 1'
            table_1['Table_Name'] = "Main Indicators of the Banking Sector"
            table_1['Relevant_Date'] = date
            table_1['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            table_1['Unit'] = 'Millions of QR'
            table_1['Value'] = table_1['Value'].apply(lambda x: clean_values(x))
            table_1 = table_1[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
            
            table_1.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_1)

           
            list2 = ['Date1','Date2','Currency Notes_500_QAR','Currency Notes_200_QAR','Currency Notes_100_QAR','Currency Notes_50_QAR','Currency Notes_10_QAR','Currency Notes_5_QAR','Currency Notes_1_QAR','Total_Notes',
                     'Currency Coins_50_AED','Currency Coins_25_AED','Currency Coins_10_AED','Currency Coins_5_AED','Currency Coins_1_AED','Total_Coins','Grand_Total']

            header2 = pd.DataFrame(np.array(list2).reshape(-1,len(list2)))
            df2 = search("Notes")
            clean_df2 = arabic_removal(df2)
            header_2 = clean_df2[-2:]
            header2 = header2.append(header_2)
            header2.reset_index(inplace = True,drop = True)

            print('header2 done')

            def slicing(df):
               table = pd.DataFrame()
               for i in range(2,df.shape[1]):
                     slice_df = df[[0,1,i]]
                     slice_df.columns = [x for x in range(slice_df.shape[1])]
                     table = pd.concat([table,slice_df])
               table.columns = ["a","b","Value"]
               table['Variable_Name'] = np.nan
               table['Variable_Name'] = np.where((table["b"].str.lower().str.contains('date2',na = False)),table["Value"],table["Variable_Name"])
               table['Variable_Name'] = table['Variable_Name'].ffill()
               drop_row = table["b"][table["b"].str.lower().str.contains('date2',na = False)].index
               table = table.drop(drop_row)
               return table

            table2 = slicing(header2)
            table2['Table_No'] = 'Table 2'
            table2['Table_Name'] = 'Currency Issued'
            table_2 = calculate_date(table2)
            table_2['Value'] = table_2['Value'].apply(lambda x: clean_values(x))

            table_2.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_2)
            
            

            list3 = ['Date1','Date2','Net Foreign Assets QCB Assets','Net Foreign Assets QCB Liabilities','Net Foreign Assets QCB Net',
                     'Net Foreign Assets Commercial Banks Assets','Net Foreign Assets Commercial Banks Liabilities','Net Foreign Assets Commercial Banks Net','Net_Foreign_Assets_Total',
                     'Net Domestic Assets Claims on Govt Claims','Net Domestic Assets Claims on Govt. Desposits','Net Domestic Assets Claims on Govt. Net',
                     'Net Domestic Assets Domestic Claims Credit','Net Domestic Assets Domestic Claims Financial Securities','Net Domestic Assets Domestic Claims Total',
                     'Net Domestic Assets Other Items','Net Domestic Assets Total','Total']

            header3 = pd.DataFrame(np.array(list3).reshape(-1,len(list3)))
            df3 = search("Monetary Survey of the Banking System")
            clean_df3 = arabic_removal(df3)
            header_3 = clean_df3[-2:]
            header3 = header3.append(header_3)
            header3.reset_index(inplace = True,drop = True)

            table3 = slicing(header3)
            table3['Table_No'] = 'Table 3'
            table3['Table_Name'] = 'Monetary Survey of the Banking System'
            table_3 = calculate_date(table3)
            table_3['Value'] = table_3['Value'].apply(lambda x: clean_values(x))
            
            table_3.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_3)
            
            list4 = ['Date1','Date2','Govt. Credit','Govt. Financial Securities in QAR','Govt. Financial Securities in Foreign Currencies','Govt. Total Claims','Govt. Deposits','Govt. Net Claims',
                     'Private Sector & Govt. Inst Credit Govt. inst','Private Sector & Govt. Inst Credit SemiGovt. inst','Private Sector & Govt. Inst Credit Private Sector','Total Credits',
                     'Private Sector & Govt. Inst Financial Securities','Total Claims']

            header4 = pd.DataFrame(np.array(list4).reshape(-1,len(list4)))
            df4 = search("Commercial Banks' Claims on Government & Private Sector")
            clean_df4 = arabic_removal(df4)
            drop_total = clean_df4.iloc[:,0][clean_df4.iloc[:,0].str.lower().str.contains("does not",na = False)].index
            clean_df4 = clean_df4.drop(drop_total)
            header_4 = clean_df4[-2:]
            header4 = header4.append(header_4)
            header4.reset_index(inplace = True,drop = True)

            table4 = slicing(header4)
            table4['Table_No'] = 'Table 4'
            table4['Table_Name'] = "Commercial Banks Claims on Govt. & Private Sector"
            table_4 = calculate_date(table4)
            table_4['Value'] = table_4['Value'].apply(lambda x: clean_values(x))
            
            table_4.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_4)
            
            list5 = ['Date1','Date2','Currency in Circulation','Demand Deposits','Money Supply M1','Time Deposits','Deposits in Foreign Currencies','Quasi Money','Money Supply M2']

            header5 = pd.DataFrame(np.array(list5).reshape(-1,len(list5)))
            df5 = search("Circulation")
            clean_df5 = arabic_removal(df5)
            header_5 = clean_df5[-2:]
            header5 = header5.append(header_5)
            header5.reset_index(inplace = True,drop = True)

            table5 = slicing(header5)
            table5['Table_No'] = 'Table 5'
            table5['Table_Name'] = "Money Supply"
            table_5 = calculate_date(table5)
            table_5['Value'] = table_5['Value'].apply(lambda x: clean_values(x))
            
            table_5.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_5)
            
            list6 = ['Date1','Date2','Currency in Circulation','Demand Deposits','Money Supply M1','Time Deposits','Deposits in Foreign Currencies','Quasi Money','Money Supply M2']

            header6 = pd.DataFrame(np.array(list6).reshape(-1,len(list6)))
            df6 = search("Changes in Money Supply")
            clean_df6 = arabic_removal(df6)
            header_6 = clean_df6[-2:]
            header6 = header6.append(header_6)
            header6.reset_index(inplace = True,drop = True)

            table6 = slicing(header6)
            table6['Table_No'] = 'Table 6'
            table6['Table_Name'] = "Changes in Money Supply"
            table_6 = calculate_date(table6)
            table_6['Value'] = table_6['Value'].apply(lambda x: clean_values(x))
            
            table_6.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_6)
            
            list7 = ['Date1','Date2','Foreign Assets','Claims on Govt.','Domestic Claims','Other Items','Total']

            header7 = pd.DataFrame(np.array(list7).reshape(-1,len(list7)))
            df7 = search("Changes in Factors Affecting Money Supply")
            clean_df7 = arabic_removal(df7)
            header_7 = clean_df7[-2:]
            header7 = header7.append(header_7)
            header7.reset_index(inplace = True,drop = True)

            table7 = slicing(header7)
            table7['Table_No'] = 'Table 7'
            table7['Table_Name'] = "Changes in Factors Affecting Money Supply"
            table_7 = calculate_date(table7)
            table_7['Value'] = table_7['Value'].apply(lambda x: clean_values(x))
            
            table_7.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_7)
            
            list8 = ['Date1','Date2','Gold','Balances with Foreign Banks','Foreign_Securities','IMF Reserve Position','SDR Holding','AMF Share','Balances with Local Banks','Other Assets','Total Assets']

            header8 = pd.DataFrame(np.array(list8).reshape(-1,len(list8)))
            df8 = search("Gold")
            clean_df8 = arabic_removal(df8)
            drop_total = clean_df8.iloc[:,0][clean_df8.iloc[:,0].str.lower().str.contains("the share that qcb owns",na = False)].index
            clean_df8 = clean_df8.drop(drop_total)
            header_8 = clean_df8[-2:]
            header8 = header8.append(header_8)
            header8.reset_index(inplace = True,drop = True)

            table8 = slicing(header8)
            table8['Table_No'] = 'Table 8'
            table8['Table_Name'] = "Financial Statement of QCB Assets"
            table_8 = calculate_date(table8)
            table_8['Value'] = table_8['Value'].apply(lambda x: clean_values(x))
            
            table_8.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_8)
            
            list9 = ['Date1','Date2','Currency Issued','Due to Govt.','Capital and Reserves','Revaluation Account','Required Reserve','Deposits of Local Banks','Other Liabilities','Total Liabilities']

            header9 = pd.DataFrame(np.array(list9).reshape(-1,len(list9)))
            df9 = search("Revaluation")
            clean_df9 = arabic_removal(df9)
            header_9 = clean_df9[-2:]
            header9 = header9.append(header_9)
            header9.reset_index(inplace = True,drop = True)

            table9 = slicing(header9)
            table9['Table_No'] = 'Table 9'
            table9['Table_Name'] = "Financial Statement of QCB Liabilities"
            table_9 = calculate_date(table9)
            table_9['Value'] = table_9['Value'].apply(lambda x: clean_values(x))
            
            table_9.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_9)
            
            list10 = ['Date1','Date2','Reserve Money M0 Currency Issued','Reserve Money M0 Banks Balances with QCB Required Reserve','Reserve Money M0 Banks Balances with QCB Excess Reserve','Reserve Money M0 Banks Balances with QCB Others',
                        'Reserve Money M0 = Counterpart Assets','Counterpart Assets Net Foreign Assets-Foreign Assets','Counterpart Assets Net Foreign Assets-Foreign Liabilities','Counterpart Assets Net Foreign Assets-Net',
                        'Counterpart Assets Net Domestic Assets-Claims on Govt.Net','Counterpart Assets Net Domestic Assets-Claims on Local Banks','Counterpart Assets Net Domestic Assets-Other Items Net','Counterpart Assets Net Domestic Assets-Net']

            header10 = pd.DataFrame(np.array(list10).reshape(-1,len(list10)))
            df10 = search("Counterpart Assets")
            clean_df10 = arabic_removal(df10)
            header_10 = clean_df10.dropna(subset=[0], axis=0)
            header_10 = header_10[-2:]
            header10 = header10.append(header_10)
            header10.reset_index(inplace = True,drop = True)

            table10 = slicing(header10)
            table10['Table_No'] = 'Table 10'
            table10['Table_Name'] = "Reserve Money M0 and The Counterpart Assets"
            table_10 = calculate_date(table10)
            table_10['Value'] = table_10['Value'].apply(lambda x: clean_values(x))
            
            table_10.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_10)
            
            list11 = ['Date1','Date2','Gold','Balances with Foreign Banks','Foreign Securities','SDR Holding and IMF Reserve Position',
                        'Total Official Reserve','Other Liquid Assets in Foreign Currencies Deposits','Total']

            header11 = pd.DataFrame(np.array(list11).reshape(-1,len(list11)))
            df11 = search("QCB's International Reserves and Foreign Currency Liquidity")
            clean_df11 = arabic_removal(df11)
            drop_total = clean_df11.iloc[:,0][clean_df11.iloc[:,0].str.lower().str.contains("the share that qcb owns",na = False)].index
            clean_df11 = clean_df11.drop(drop_total)
            header_11 = clean_df11[-2:]
            header11 = header11.append(header_11)
            header11.reset_index(inplace = True,drop = True)

            table11 = slicing(header11)
            table11['Table_No'] = 'Table 11'
            table11['Table_Name'] = "QCBs International Reserves and Foreign Currency Liquidity"
            table_11 = calculate_date(table11)
            table_11['Value'] = table_11['Value'].apply(lambda x: clean_values(x))
            
            table_11.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_11)
            
            list12 = ['Date1','Date2','Reserves Cash in QR','Reserves Due from QCB','Foreign Assets Cash','Foreign Assets Due from Banks Abroad',
                        'Foreign Assets Credit Outside Qatar','Foreign Assets Investments Abroad','Other Foreign Assets',
                        'Domestic Assets Due from Banks in Qatar','Domestic Assets Domestic Credit','Domestic Assets Domestic Investments',
                        'Fixed Domestic Assets','Other Domestic Assets','Total Assets']

            header12 = pd.DataFrame(np.array(list12).reshape(-1,len(list12)))
            df12 = search("Commercial Banks' Assets")
            clean_df12 = arabic_removal(df12)
            header_12 = clean_df12.dropna(subset=[0], axis=0)
            header_12 = header_12[-2:]
            header12 = header12.append(header_12)
            header12.reset_index(inplace = True,drop = True)

            table12 = slicing(header12)
            table12['Table_No'] = 'Table 12'
            table12['Table_Name'] = "Commercial Banks Assets"
            table_12 = calculate_date(table12)
            table_12['Value'] = table_12['Value'].apply(lambda x: clean_values(x))
            
            table_12.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_12)
            
            list13 = ['Date1','Date2','Foreign Liabilities Non Resident Deposits','Foreign Liabilities Due to Banks abroad-Banks','Foreign Liabilities Due to Banks abroad-H.O & Branches',
                        'Foreign Liabilities Debt Securities','Other Foreign Liabilities','Domestic Liabilities Resident Deposits','Domestic Liabilities Due to Banks in Qatar','Domestic Liabilities Due to QCB',
                        'Domestic Liabilities Debt Securities','Domestic Liabilities Margins','Domestic Liabilities Capital Accounts','Domestic Liabilities Provisions','Other Domestic Liabilities',
                        'Total Liabilities','Contra Accounts']

            header13 = pd.DataFrame(np.array(list13).reshape(-1,len(list13)))
            df13 = search("Commercial Banks' Liabilities")
            clean_df13 = arabic_removal(df13)
            header_13 = clean_df13[-2:]
            header13 = header13.append(header_13)
            header13.reset_index(inplace = True,drop = True)

            table13 = slicing(header13)
            table13['Table_No'] = 'Table 13'
            table13['Table_Name'] = "Commercial Banks Liabilities"
            table_13 = calculate_date(table13)
            table_13['Value'] = table_13['Value'].apply(lambda x: clean_values(x))
            
            table_13.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_13)
           

            list14 = ['Date1','Date2','Private Sector Deposits Demand Deposits in QAR','Private Sector Deposits Timing & Saving in QAR','Private Sector Deposits Demand Deposits in Foreign Currencies','Private Sector Deposits Timing & Saving in Foreign Currencies','Private Sector Deposits Total',
                        'Public Sector Deposits Demand Deposits in QAR','Public Sector Deposits Timing & Saving in QAR','Public Sector Deposits Demand Deposits in Foreign Currencies','Public Sector Deposits Timing & Saving in Foreign Currencies','Public Sector Deposits Total','Non Resident Deposits','Grand Total']

            header14 = pd.DataFrame(np.array(list14).reshape(-1,len(list14)))
            df14 = search("Commercial Banks Deposits")
            clean_df14 = arabic_removal(df14)
            header_14 = clean_df14[-2:]
            header14 = header14.append(header_14)
            header14.reset_index(inplace = True,drop = True)

            table14 = slicing(header14)
            table14['Table_No'] = 'Table 14'
            table14['Table_Name'] = "Commercial Banks Deposits"
            table_14 = calculate_date(table14)
            table_14['Value'] = table_14['Value'].apply(lambda x: clean_values(x))
            
            table_14.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_14)
            
            list15 = ['Date1','Date2','Govt. Demand Deposits in QAR','Govt. Timing & Saving in QAR','Govt. in Foreign Currencies','Govt. Total',
                        'Govt. Inst Demand Deposits in QAR','Govt. Inst Timing & Saving in QAR','Govt. Inst in Foreign Currencies','Govt. Inst Total',
                        'Semi Govt. Inst Demand Deposits in QAR','Semi Govt. Inst Timing & Saving in QAR','Semi Govt. Inst in Foreign Currencies','Semi Govt. Inst Total',
                        'Public Sector Demand Deposits in QAR','Public Sector Timing & Saving in QAR','Public Sector Total in QAR','Public Sector in Foreign Currencies','Public Sector Total']

            header15 = pd.DataFrame(np.array(list15).reshape(-1,len(list15)))
            df15 = search("Commercial Banks Deposits- Public Sector")
            clean_df15 = arabic_removal(df15)
            header_15 = clean_df15.dropna(subset=[0], axis=0)
            header_15 = header_15[-2:]
            header15 = header15.append(header_15)
            header15.reset_index(inplace = True,drop = True)

            table15 = slicing(header15)
            table15['Table_No'] = 'Table 15'
            table15['Table_Name'] = "Commercial Banks Deposits-Public Sector"
            table_15 = calculate_date(table15)
            table_15['Value'] = table_15['Value'].apply(lambda x: clean_values(x))
            
            table_15.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_15)
            
            list16 = ['Date1','Date2','Personal Demand Deposits','Personal Time and Saving','Personal In Foreign Currencies','Personal Total',
                                 'Companies & Institutions Demand Deposits','Companies & Institutions Time and Saving','Companies & Institutions In Foreign Currencies','Companies & Institutions Total',
                                 'Private Sector Demand Deposits','Private Sector Time and Saving','Private Sector Total','Private Sector In Foreign Currencies','Private Sector Total']
            header16 = pd.DataFrame(np.array(list16).reshape(-1,len(list16)))
            df16 = search("Commercial Banks Deposits- Private Sector")
            clean_df16 = arabic_removal(df16)
            header_16 = clean_df16[-2:]
            header16 = header16.append(header_16 )
            header16.reset_index(inplace = True,drop = True)
            table16 = slicing(header16)
            table16['Table_No'] = 'Table 16'
            table16['Table_Name'] = 'Commercial Banks Deposits-Private sector'
            table_16 = calculate_date(table16)
            table_16['Value'] = table_16['Value'].apply(lambda x: clean_values(x))
            table_16.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_16)
           
            list17 = ['Date1','Date2','Public Sector','General Trade','Industry','Contractors','Real Estate','Consumption','Services','Others','Total Domestic Credits','Outside Qatar','Total']
            header17 = pd.DataFrame(np.array(list17).reshape(-1,len(list17)))
            df17 = search("Distribution of Credit Facilities")
            clean_df17 = arabic_removal(df17)
            header_17 = clean_df17[-2:]
            header17 = header17.append(header_17)
            header17.reset_index(inplace = True,drop = True)
            table17 = slicing(header17)
            table17['Table_No'] = 'Table 17'
            table17['Table_Name'] = 'Distribution of Credit Facilities'
            table_17 = calculate_date(table17)
            table_17['Value'] = table_17['Value'].apply(lambda x: clean_values(x))
            
            table_17.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_17)
            
            list18 = ['Date1','Date2','Govt. Over Draft','Govt. Loans & Others','Govt. Total',
                                 'Govt. Institutions Over Draft','Govt. Institutions Loans & Others','Govt. Institutions Total',
                                 'Semi Govt. Institutions Over Draft','Semi Govt. Institutions Loans and Others','Semi Govt. Institutions Total',
                                 'Public Sector Over Draft','Public Sector Loans and Others','Public Sector Total']
            header18 = pd.DataFrame(np.array(list18).reshape(-1,len(list18)))
            df18 = search("Commercial Banks Credit Facilities- Public Sector")
            clean_df18 = arabic_removal(df18)
            header_18 = clean_df18[-2:]
            header18 = header18.append(header_18)
            header18.reset_index(inplace = True,drop = True)
            table18 = slicing(header18)
            table18['Table_No'] = 'Table 18'
            table18['Table_Name'] = 'Commercial Banks Credit Facilities-Public sector'
            table_18 = calculate_date(table18)
            table_18['Value'] = table_18['Value'].apply(lambda x: clean_values(x))
            
            table_18.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_18)
            
            list19 = ['Date1','Date2','General Trade','Contractors and Real Estate','Consumption','Other Sectors','Private Sector Over Draft','Private Sector Loans and Others','Private Sector Total']
            header19 = pd.DataFrame(np.array(list19).reshape(-1,len(list19)))
            df19 = search("Commercial Banks Credit Facilities- Private Sector")
            clean_df19 = arabic_removal(df19)
            header_19 = clean_df19[-2:]
            header19 = header19.append(header_19)
            header19.reset_index(inplace = True,drop = True)
            table19 = slicing(header19)
            table19['Table_No'] = 'Table 19'
            table19['Table_Name'] = 'Commercial Banks Credit Facilities-Private Sector'
            table_19 = calculate_date(table19)
            table_19['Value'] = table_19['Value'].apply(lambda x: clean_values(x))
            
            table_19.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_19)
            
            list20 = ['Date1','Date2','No. of Cheques Cleared','Complete Value of Cheques Cleared','Daily Avg No. of cheques','Daily Avg Value']

            header20 = pd.DataFrame(np.array(list20).reshape(-1,len(list20)))
            df20 = search("Banks' Clearings")
            clean_df20 = arabic_removal(df20)
            header_20 = clean_df20[-2:]
            header20 = header20.append(header_20)
            header20.reset_index(inplace = True,drop = True)

            table20 = slicing(header20)
            table20['Table_No'] = 'Table 20'
            table20['Table_Name'] = "Banks Clearings"
            table_20 = calculate_date(table20)
            table_20['Value'] = table_20['Value'].apply(lambda x: clean_values(x))
            
            table_20.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_20)
            
            bank_dictionary = {'Traditional':'Qatari Banks Traditional','Islamic':'Qatari Banks Islamic','Specialized':'Qatari Banks Specialized',
                                 'Arab':'Foreign banks Arab','Non- Arab':'Foreign Banks Non-Arab'}
            def clean(df):
                  df = df.replace('*',np.nan)
                  df = df.dropna(axis = 1,how = 'all')
                  row = df.iloc[:,0][df.iloc[:,0].str.lower().str.contains("all banks",na = False)].index[0]
                  df = df[row:]
                  df = df.iloc[:,:-1]
                  df = df.dropna()
                  drop_unwanted = df.iloc[:,0][df.iloc[:,0].str.lower().str.contains('qatari|foreign',na = False)].index
                  df = df.drop(drop_unwanted)
                  df.reset_index(inplace = True,drop = True)
                  return df

            def clean1(df):
                  full_df = pd.DataFrame()
                  for i in range(1,df.shape[1]):
                     slice_df = df[[0,i]]
                     slice_df.columns = [x for x in range(slice_df.shape[1])]
                     full_df = pd.concat([full_df,slice_df])
                  full_df.columns = ["Group_Name","Value"]
                  full_df["Name"] = np.nan
                  full_df["Name"] = np.where((full_df["Group_Name"].str.lower().str.contains('group')),full_df["Value"],full_df["Name"])
                  full_df["Name"] = full_df["Name"].ffill()
                  drop_row = full_df["Group_Name"][full_df["Group_Name"].str.lower().str.contains('group',na = False)].index
                  table = full_df.drop(drop_row)
                  table.reset_index(inplace = True,drop = True)
                  drop_total = table["Name"][table["Name"].str.lower().str.contains('total',na = False)].index
                  table = table.drop(drop_total)
                  table['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                  table['Unit'] = 'Thousands of QR'
                  return table

            list21 = ['Group','Reserve Cash in QR','Reserve Due from QR','Foreign Assets Cash','Foreign Assets Due from Banks Abroad','Foreign Assets Credit Outside Qatar','Foreign Assets Investment Abroad',
                        'Other Foreign Assets','Domestic Assets Due from Banks in Qatar','Domestic Assets Domestic Credit','Domestic Assets Domestic Investments','Domestic Assets Fixed Assets','Other Domestic Assets',
                        'Total Assets']
            header21 = pd.DataFrame(np.array(list21).reshape(-1,len(list21)))
            df21 = search("Group")
            clean_df21 = clean(df21)
            clean_df21.iloc[:,0].replace(bank_dictionary,inplace = True)
            header21 = header21.append(clean_df21)
            header21.reset_index(inplace = True,drop = True)
            table_21 = clean1(header21)
            table_21['Table_No'] = 'Table 21'
            table_21['Table_Name'] = "Banks Groups Assets"
            table_21['Relevant_Date'] = latest_date
            table_21['Variable_Name'] = table_21['Name']+'-'+table_21['Group_Name']
            table_21['Value'] = table_21['Value'].apply(lambda x: clean_values(x))
            table_21 = table_21[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
            
            table_21.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_21)
            

            list22 = ['Group','Foreign Liabilities Non Resident Deposits','Foreign Liabilities Due to Banks Abroad','Foreign Liabilities Debt Securtries','Other Foreign Liabilities',
                        'Domestic Liabilities Resident Deposits','Domestic Liabilities Due to Banks in Qatar','Domestic Liabilities Due to QCB','Domestic Liabilities Debt Securities',
                        'Domestic Liabilities Margins','Domestic Liabilities Capital Accounts','Domestic Liabilities Provisions','Other Domestic Liabilities','Total Liabilities','Contra Accounts']

            header22 = pd.DataFrame(np.array(list22).reshape(-1,len(list22)))
            df22 = xls_dict['Table 22']
            clean_df22 = clean(df22)
            clean_df22.iloc[:,0].replace(bank_dictionary,inplace = True)
            header22 = header22.append(clean_df22)
            header22.reset_index(inplace = True,drop = True)
            table_22 = clean1(header22)
            table_22['Table_No'] = 'Table 22'
            table_22['Table_Name'] = "Banks Groups Liabilities"
            table_22['Relevant_Date'] = latest_date
            table_22['Variable_Name'] = table_22['Name']+'-'+table_22['Group_Name']
            table_22['Value'] = table_22['Value'].apply(lambda x: clean_values(x))
            table_22 = table_22[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
            
            table_22.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_22)



            list23 = ['Group','Private Sector Deposits Demand Deposits in QR','Private Sector Deposits Time & Saving in QR','Private Sector Deposits Demand Deposits in Foreign Currencies','Private Sector Deposits Time & Saving in Foreign Currencies','Private Sector Deposits Total',
                        'Public Sector Deposits Demand Deposits in QR','Public Sector Deposits Time & Saving in QR','Public Sector Deposits Demand Deposits in Foreign Currencies','Public Sector Deposits Time & Saving in Foreign Currencies','Public Sector Deposits Total',
                        'Non Resident Deposits','Grand Total']

            header23 = pd.DataFrame(np.array(list23).reshape(-1,len(list23)))
            df23 = xls_dict['Table 23']
            clean_df23 = clean(df23)
            clean_df23.iloc[:,0].replace(bank_dictionary,inplace = True)
            header23 = header23.append(clean_df23)
            header23.reset_index(inplace = True,drop = True)
            table_23 = clean1(header23)
            table_23['Table_No'] = 'Table 23'
            table_23['Table_Name'] = "Banks Groups Deposits"
            table_23['Relevant_Date'] = latest_date
            table_23['Variable_Name'] = table_23['Name']+'-'+table_23['Group_Name']
            table_23['Value'] = table_23['Value'].apply(lambda x: clean_values(x))
            table_23 = table_23[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
            
            table_23.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_23)
            

            list24 = ['Group','Public Sector','General Trade','Industry','Contractors','Real Estate','Consumption','Services','Others','Total Domestic Credits','Outside Qatar','Total']

            header24 = pd.DataFrame(np.array(list24).reshape(-1,len(list24)))
            df24 = xls_dict['Table 24']
            clean_df24 = clean(df24)
            clean_df24.iloc[:,0].replace(bank_dictionary,inplace = True)
            header24 = header24.append(clean_df24)
            header24.reset_index(inplace = True,drop = True)
            table_24 = clean1(header24)
            table_24['Table_No'] = 'Table 24'
            table_24['Table_Name'] = "Banks Groups Credit Facilities"
            table_24['Relevant_Date'] = latest_date
            table_24['Variable_Name'] = table_24['Name']+'-'+table_24['Group_Name']
            table_24['Value'] = table_24['Value'].apply(lambda x: clean_values(x))
            table_24 = table_24[['Table_No','Table_Name','Variable_Name','Value','Unit','Relevant_Date','Runtime']]
            
            table_24.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_24)
            

            list25 = ['Date1','Date2','Overnight','1-Week','1-Month','3-Months','6-Months','1-Year']

            header25 = pd.DataFrame(np.array(list25).reshape(-1,len(list25)))
            df25 = search("Overnight")
            clean_df25 = arabic_removal(df25)
            header_25 = clean_df25[-2:]
            header25 = header25.append(header_25)
            header25.reset_index(inplace = True,drop = True)

            table25 = slicing(header25)
            table25['Table_No'] = 'Table 25'
            table25['Table_Name'] = "Interbanks Interest Rates Weighted Average"
            table_25 = calculate_date(table25)
            table_25['Unit'] = 'Percent'
            table_25['Value'] = table_25['Value'].apply(lambda x: clean_values(x))
            
            table_25.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_25)
            
           
            list26 = ['Date1','Date2','Demand Deposits','Saving Deposits','Time Deposits 1-month','Time Deposits 3-months','Time Deposits 6-months','Time Deposits 1-year','Time Deposits More than 1-year']

            header26 = pd.DataFrame(np.array(list26).reshape(-1,len(list26)))
            df26 = search("Interest Rates on Customers' Deposits in Qatari Riyal")
            clean_df26 = arabic_removal(df26)
            header_26 = clean_df26[-2:]
            header26 = header26.append(header_26)
            header26.reset_index(inplace = True,drop = True)

            table26 = slicing(header26)
            table26['Table_No'] = 'Table 26'
            table26['Table_Name'] = "Interest Rates on Customers Deposits in QAR Weighted Average"
            table_26 = calculate_date(table26)
            table_26['Unit'] = 'Percent'
            table_26['Value'] = table_26['Value'].apply(lambda x: clean_values(x))
            
            table_26.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_26)

           
            list27 = ['Date1','Date2','Overdraft','Bill Discounted','Loans Less than 1-Year','Loans from 1-Year to 3-Years','Loans 3-Years and Up','Car Loans','Cedit Card Loans']

            header27 = pd.DataFrame(np.array(list27).reshape(-1,len(list27)))
            df27 = search("Interest Rates on Credit Facilities in Qatari Riyal")
            clean_df27 = arabic_removal(df27)
            header_27 = clean_df27[-2:]
            header27 = header27.append(header_27)
            header27.reset_index(inplace = True,drop = True)

            table27 = slicing(header27)
            table27['Table_No'] = 'Table 27'
            table27['Table_Name'] = "Interest Rates on Credit Facilities in QAR Weighted Average"
            table_27 = calculate_date(table27)
            table_27['Unit'] = 'Percent'
            table_27['Value'] = table_27['Value'].apply(lambda x: clean_values(x))
            
            table_27.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_27)


            list28 = ['Date1','Date2','No. of Tranactions Repo','No. of Tranactions QMR Lending','No. of Tranactions QMR Deposit',
                        'Value of Tranactions Repo','Value of Tranactions QMR Lending','Value of Tranactions QMR Deposit',
                        'Interest Rate Repo Pct','Interest Rate QMR Lending Pct','Interest Rate QMR Deposit Pct']

            header28 = pd.DataFrame(np.array(list28).reshape(-1,len(list28)))
            df28 = search("QMR & Repo Operations")
            clean_df28 = arabic_removal(df28)
            header_28 = clean_df28[-2:]
            header28 = header28.append(header_28)
            header28.reset_index(inplace = True,drop = True)

            table28 = slicing(header28)
            table28['Table_No'] = 'Table 28'
            table28['Table_Name'] = "QMR & Repo Operations"
            table_28 = calculate_date(table28)
            table_28['Value'] = table_28['Value'].apply(lambda x: clean_values(x))
            
            table_28.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_28)


           
            list29 = ['Date1','Date2','drop_col','Saudi Riyal','UAE Dirham','Bahraini Dinar','Omani Riyal','Kuwaiti Dinar','Euro','Japanese Yen Per Hundred','Sterling Pound','Swiss Franc','Chinese Yuan']

            header29 = pd.DataFrame(np.array(list29).reshape(-1,len(list29)))
            df29 = search("Exchange Rate of GCC and Major Currencies")
            df29 = df29.dropna(thresh=5,axis=0)
            clean_df29 = arabic_removal(df29)
            clean_df29  = clean_df29 [clean_df29.iloc[:,0].notnull()]
            clean_df29.reset_index(drop = True,inplace = True)
            header_29 = clean_df29[-2:]
            header29 = header29.append(header_29)
            header29.reset_index(inplace = True,drop = True)

            table29 = slicing(header29)
            table29['Table_No'] = 'Table 29'
            table29['Table_Name'] = "Exchange Rate of GCC and Major Currencies against QR"
            table_29 = calculate_date(table29)
            drop_total = table_29['Variable_Name'][table_29['Variable_Name'].str.lower().str.contains("drop_col",na = False)].index
            table_29 = table_29.drop(drop_total)
            table_29['Unit'] = 'QR'
            table_29['Value'] = table_29['Value'].apply(lambda x: clean_values(x))
            
            table_29.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_29)


            list30 = ['Date1','Date2','No. of Listed Companies','General Index','Market Capitalization','Banking & Financial Sector No. of Shares','Banking & Financial Sector Value of Shares',
                        'Consumer Goods & Services Sector No. of Shares','Consumer Goods & Services Sector Value of Shares','Industrial Sector No. of Shares','Industrial Sector Value of Shares',
                        'Insurance No. of Shares','Insurance Value of Shares',
                        'Real Estate Sector No. of Shares','Real Estate Sector Value of Shares','Telecome Sector No. of Shares','Telecome Sector Value of Shares','Tranpotation No. of Shares',
                        'Transportation Value of Shares','Total No. of Shares','Total Value of Shares']

            header30 = pd.DataFrame(np.array(list30).reshape(-1,len(list30)))
            df30 = search("Qatar Stock Exchange")
            df30 = df30.dropna(thresh=5,axis=0)
            clean_df30 = arabic_removal(df30)
            drop_total = clean_df30.iloc[:,0][clean_df30.iloc[:,0].str.lower().str.contains("source",na = False)].index
            clean_df30 = clean_df30.drop(drop_total)
            header_30 = clean_df30[-2:]
            header30 = header30.append(header_30)
            header30.reset_index(inplace = True,drop = True)

            table30 = slicing(header30)
            table30['Table_No'] = 'Table 30'
            table30['Table_Name'] = "Qatar Stock Exchange"
            table_30 = calculate_date(table30)
            table_30['Value'] = table_30['Value'].apply(lambda x: clean_values(x))
            table_30.to_sql(name = "QATAR_CENTRAL_BANK_MONTHLY_DATA_STAGING",con = engine,if_exists = 'append',index = False)
            delete_re_upload(table_30)
            

            # os.remove(file)
         MySql_CH.ch_truncate_and_insert('QATAR_CENTRAL_BANK_MONTHLY_DATA')
      else:
            print('No New Data Found')

      
         


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
