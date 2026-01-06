#!/usr/bin/env python
# coding: utf-8
import sys
import os
import requests

from fiscalyear import *
import fiscalyear
from datetime import datetime as dt
from pandas.tseries.offsets import MonthEnd
from selenium import webdriver
import time
from bs4 import BeautifulSoup

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import pandas as pd
import adqvest_s3
import boto3
import ntpath
from botocore.config import Config

os.chdir('C:/Users/Administrator/AdQvestDir/')

def url_to_s3(url, key,filename):
    if(key[-1] != '/'):
        key = key + '/'
    response = requests.get(url)
    if response.status_code == 200:
        file_content =  response.content
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.resource(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
    try:
        s3.Bucket(BUCKET_NAME).put_object(Key=key+filename, Body=file_content)
        print(f"File uploaded to s3://{BUCKET_NAME}/{key}")
    except NoCredentialsError:
        print("Credentials not available")

def Upload_Data(table_name, data, db: list):
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
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

def to_s3bucket(filepath,key):
    BUCKET_NAME = 'adqvests3bucket'

    filename = ntpath.basename(filepath)

    data = open(filepath, 'rb')
    s3 = boto3.resource(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
    # Uploading the file to S3 bucket
    s3.Bucket(BUCKET_NAME).put_object(Key=key + filename, Body=data)
    print("File Uploaded to S3 Bucket!")

def from_s3bucket(key,download_path,filename):
    BUCKET_NAME = 'adqvests3bucket'
    s3 = boto3.client(
        's3',
        config=Config(signature_version='s3v4', region_name='ap-south-1')
        )
    # Downloading the file from S3 bucket
    s3.download_file(f'{BUCKET_NAME}',f'{key +"/"+filename}',f'{download_path + filename}')
    print("File downloaded from S3 Bucket!")

def to_sqldb(df,table_name):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    df.to_sql(name=table_name,con=engine,if_exists='append',index=False)
    connection.close()
    print("Data Uploaded to SQL!")

def to_clickhouse(table_name):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    click_max_date = client.execute(f"select max(Relevant_Date) from {table_name} ")
    click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
    query = f'select * from AdqvestDB.{table_name}  where Relevant_Date > "' + click_max_date +'"'
    df_ch = pd.read_sql(query, engine)
    client.execute(f"INSERT INTO {table_name} VALUES", df_ch.values.tolist())
    print("Data Uploaded to Clickhouse!")
    connection.close()

def read_sql(query):
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    read_sql_df = pd.read_sql(query,con = connection)
    connection.close()
    return read_sql_df


#--------------------------Added by Santonu 2025,Oct 10---------------------------------------------------
def convert_date_format(input_date, output_format='%Y-%m-%d', input_format="%B, %Y", Month_end=True):
    # from datetime import datetime as dt
    # from pandas.tseries.offsets import MonthEnd
    
    date_formats = [input_format,"%B '%y","%B, %y","%m, %Y","%b, %Y","%b, %y"]
    
    input_str = str(input_date)
    parsed_date = None

    for fmt in date_formats:
        try:
            parsed_date = dt.strptime(input_str, fmt)
            break
        except ValueError:
            continue
    
    if parsed_date is None:
        raise ValueError(f"Date '{input_date}' does not match known formats.")
    
    output_date = parsed_date.strftime(output_format)
    output_date = pd.to_datetime(output_date, format=output_format)
    
    if Month_end:
        output_date += MonthEnd(1)
    
    return output_date.date()


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
    # fy_year=str(fy.start.year)+'-'+str(fy.end.year)
    return str(fy.end.year)[-2:]

def get_fiscal_quarter(date, fiscal_start_month=4):
        month = date.month
        fiscal_quarter = ((month - fiscal_start_month) % 12) // 3 + 1
        return fiscal_quarter

def get_quarter_fy_from_date(date):
    qtr="Q"+str(get_fiscal_quarter(date))
    fy='FY'+get_financial_year(date)
    return qtr+'_'+fy


def get_page_content(url):


    # options=driver_parameter(
    prefs = {
        "download.prompt_for_download": True,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
        }

    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', prefs)
    # options.add_experimental_option('excludeSwitches', ['enable-automation'])

    options.add_argument("--disable-infobars")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument('--incognito')
    options.add_argument("--disable-notifications")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument("--use-fake-ui-for-media-stream")
    options.add_argument("--use-fake-device-for-media-stream")
    options.add_experimental_option("prefs", prefs)
    # options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    # options.set_capability("goog:loggingPrefs", {"browser": "ALL"})

    # driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
    # driver = uc.Chrome(options=options)
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    driver.maximize_window()
    time.sleep(10)
    driver.implicitly_wait(10)
    soup=BeautifulSoup(driver.page_source)
    time.sleep(3)
    driver.quit()
    return soup
#--------------------------------------------------------------------------------------------------
