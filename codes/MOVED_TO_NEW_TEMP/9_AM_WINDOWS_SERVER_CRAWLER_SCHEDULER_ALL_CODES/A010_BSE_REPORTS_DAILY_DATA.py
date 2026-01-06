import datetime as datetime
import os
import re
import sys
import time
import warnings
import pandas as pd
import requests
from pytz import timezone

import requests
from bs4 import BeautifulSoup
from dateutil import parser
warnings.filterwarnings('ignore')
import numpy as np
import camelot
from dateutil.relativedelta import *
import boto3
from botocore.config import Config
from calendar import monthrange
from requests_html import HTMLSession

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions
import time
from dateutil.relativedelta import relativedelta

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

from adqvest_robotstxt import Robots
robot = Robots(__file__)

import adqvest_db
import JobLogNew as log
import adqvest_s3
import ClickHouse_db

def drop_duplicates(df):
    columns = df.columns.to_list()
    try:
        columns.remove('Runtime')
    except:
        pass    
    unique = df.drop_duplicates(subset=columns, keep='last')
    print(unique.shape)
    return unique

def get_data(single_date):
    year_from = str(single_date.strftime('%Y'))
    month_from = str(single_date.strftime('%b'))
    day_from = str(single_date.strftime('%d')).lstrip('0')

    year_to = str(single_date.strftime('%Y'))
    month_to = str(single_date.strftime('%b'))
    day_to = str(single_date.strftime('%d')).lstrip('0')
 
    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

    download_file_path = r"C:\Users\Administrator\AdQvestDir\BSE REPORTS"

    prefs = {
        "download.default_directory": download_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
        }
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--no-sandbox')
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

    driver.maximize_window()

    driver.get("https://www.bseindia.com/corporates/ann.html")
    robot.add_link("https://www.bseindia.com/corporates/ann.html")
    driver.implicitly_wait(10)

    driver.execute_script("window.scrollTo(0, window.scrollY + 200)")
    #----inputing from_date and to_date
    date=driver.find_element(By.XPATH,"//*[@id='txtFromDt']")
    time.sleep(5)
    date.click()

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_from)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_from)
    time.sleep(2)


    from_day = driver.find_element("xpath","//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_from+"']")
    from_day.click()
    time.sleep(5)

    #---to date--

    date=driver.find_element(By.XPATH,"//*[@id='txtToDt']")
    time.sleep(5)
    date.click()


    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_to)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_to)
    time.sleep(2)


    to_day = driver.find_element("xpath","//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_to+"']")
    to_day.click()
    time.sleep(5)



    elem = driver.find_element("xpath",'//*[@class="btn btn-default"]')
    elem.click()
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html')


    return driver

documents={"_Investor_Presentation_":'Investor Presentation',"_Earnings_Call_Transcript_":'Earnings Call Transcript',"_Annual_Report_":'Annual Report'}
def get_doc_type_info(soup1,doc_type,date):
        # soup1=BeautifulSoup(soup1)
        print(documents[doc_type].strip().lower())

      # 
        doc_dict={i.find('td',class_="tdcolumngrey").text.replace('\n',''):i.find('a',class_="tablebluelink",href=True).get('href') for i in soup1.find_all('tr') if ((i.find('td',class_="tdcolumngrey")!=None) and (i.find('a',class_="tablebluelink",href=True)!=None))}
        doc_dict={k:'https://www.bseindia.com/'+v for k,v in doc_dict.items() if documents[doc_type].strip().lower() in k.lower()}
        print(doc_dict)
        df_t=pd.DataFrame()
        if len(doc_dict)>0:
            # dates=soup1.find_all('b',{'class':'ng-binding'})[2:-3]
            # date=dates[0].get_text().strip()
            date=str(parser.parse(date).date())
            print(date)

            for k,v in doc_dict.items():
                df=pd.DataFrame()
                try:
                    company=k.split("-")[0]
                except:
                    company=k.split("-")

                
                print(company)
                df['Links']=[v]
                df['Relevant_Date']=[date]
                df['File_Name']=[company + doc_type + date.replace('-', '_') + ".pdf"]
                df['Source']=[documents[doc_type]]
                df['Company']=[company]
                df['Runtime']=[datetime.datetime.now()]
                print(df)
                df_t=pd.concat([df_t,df])


        if len(df_t)==0:
           df_t=pd.DataFrame()
           return df_t
        else:
            return df_t 

def process_data(soup,date):

    soup1=BeautifulSoup(soup)
    df2=pd.DataFrame()
    for doc in documents.keys():
        df1=get_doc_type_info(soup1,doc,date)
        df2=pd.concat([df2,df1])

    return df2


    

def pushToS3(df,engine):
    for i,row in df.iterrows():
        try:

            ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
            ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
            BUCKET_NAME = 'adqvests3bucket'
            s3 = boto3.resource(
                's3',
                aws_access_key_id=ACCESS_KEY_ID,
                aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4',region_name = 'ap-south-1')
                )
            links=row['Links']
            file_name=row['File_Name']

            r =  requests.get(links,verify = False,headers={"User-Agent": "XY"}, timeout = 120)

            robot.add_link(links)

            time.sleep(3)
            with open(file_name,'wb') as f:
                f.write(r.content)
                f.close()

            time.sleep(3)
            data =  open(file_name, 'rb')

            s3.Bucket(BUCKET_NAME).put_object(Key='Annual_Reports/'+file_name, Body=data)
            data.close()

            # Deleting the file from the local machine
            if os.path.exists(file_name):
                os.remove(file_name)

            connection=engine.connect()
            connection.execute('update BSE_REPORTS_DAILY_DATA set Download_Status = "Yes" where Links = "'+links+'"')
        except:
            connection=engine.connect()
            connection.execute('update BSE_REPORTS_DAILY_DATA set Download_Status = "No" where Links = "'+links+'"')


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BSE_REPORTS_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date=pd.read_sql("SELECT max(Relevant_Date) as Max FROM AdqvestDB.BSE_REPORTS_DAILY_DATA",engine)['Max'][0]
        single_date=max_rel_date+days
        print(single_date)
        final_df=pd.DataFrame()
        max_date=today.date()
        while single_date<max_date:

            driver=get_data(single_date)
            time.sleep(5)
            df=process_data(driver.page_source,str(single_date))
            final_df=pd.concat([final_df,df])

            while True:
                next_elem=driver.find_elements("xpath","//*[@id='idnext']")
                if len(next_elem)==0:
                    break
                else:
                    time.sleep(5)
                    next_elem[0].click()
                    time.sleep(4)
                    
                    df=process_data(driver.page_source,str(single_date))
                    final_df=pd.concat([final_df,df])
                   
                    
            single_date=single_date+days
        try:
            driver.close()
        except:
            pass

        '''  data upload  '''

        engine = adqvest_db.db_conn()

        final_df=drop_duplicates(final_df)
        # print(final_df.shape)
        final_df['Company'] = final_df['Company'].str.replace(r'\(\d+\)', '')
        final_df['Company'] = final_df['Company'].str.replace('.', '')
        final_df['Company'] = final_df['Company'].str.replace('$', '')
        final_df['Company'] = final_df['Company'].str.replace('-', '')

        final_df['Source']=np.where(final_df['Source']=='Annual Reports','Annual Report',final_df['Source'])
        
        final_df.to_sql("BSE_REPORTS_DAILY_DATA", index=False, if_exists='append', con=engine)

        pushToS3(final_df,engine)

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
