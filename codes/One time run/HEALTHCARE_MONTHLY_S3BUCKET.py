import requests
import sys
import warnings
from selenium import webdriver
import os
import time
from bs4 import BeautifulSoup
import numpy as np
import re
from pytz import timezone
from pandas.io import sql
from calendar import monthrange
import datetime as datetime
import pandas as pd
import sqlalchemy
import boto3
from botocore.config import Config
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HEALTH_CARE'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        chrome_driver_path = r'C:/Users/Administrator/AdQvestDir/chromedriver.exe'
        download_file_path = 'C:\\Users\\Administrator\\AdQvestDir\\Junk One time folder\\'
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
        }
        options.add_experimental_option('prefs', prefs)
        options.add_argument('--ignore-certificate-errors')
        links = pd.read_sql('select File_Name,File_Name_Ref,Link,Relevant_Date from AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS where Download_Status is NULL and Comments is Null',engine)
        links['File_Name_Ref'] = links['File_Name_Ref'].apply(lambda x: x + ".xls")
        os.chdir('C:\\Users\\Administrator\\AdQvestDir\\Junk One time folder')
        for a, values in links.iterrows():
            file = values['File_Name']
            file_ref = values['File_Name_Ref']
            link = values['Link']
            date = values['Relevant_Date']
            access_credentials = pd.read_sql("select * from AWS_S3_ACCESS_CREDENTIALS", engine)
            ACCESS_KEY_ID = access_credentials["Acess_Key_ID"][0]
            ACCESS_SECRET_KEY = access_credentials["Access_Secret_Key"][0]
            BUCKET_NAME = 'adqvests3bucket'
            driver = webdriver.Chrome(executable_path=chrome_driver_path, chrome_options=options)
            driver.get(url=link)
            driver.find_element_by_xpath("//input[@name = 'lbFile']").click()
            no_of_ping+=1
            time.sleep(5)
            driver.close()
            file_name = file
            os.rename(file, file_ref)
            file_name_new = file_ref
            data = open(file_ref, 'rb')
            s3 = boto3.resource(
                's3',
                aws_access_key_id=ACCESS_KEY_ID,
                aws_secret_access_key=ACCESS_SECRET_KEY,
                config=Config(signature_version='s3v4', region_name='ap-south-1')
            )
            # Uploading the file to S3 bucket
            s3.Bucket(BUCKET_NAME).put_object(Key='HEALTHCARE_DATA/' + file_name_new, Body=data)
            data.close()
            if os.path.exists(file_ref):
                os.remove(file_ref)
            connection = engine.connect()
            connection.execute("UPDATE AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS set Download_Status= 'Yes' where Link=%s",link)
            connection.execute("commit")
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        a=input()
        print(a)
        connection = engine.connect()
        connection.execute("UPDATE AdqvestDB.HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS set Download_Status= 'No' where Link=%s",link)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if __name__ == '__main__':
    run_program(run_by='manual')
