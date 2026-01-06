import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
import zipfile
import re
import ast
import datetime as datetime
from pytz import timezone
import requests

import numpy as np
import time

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import json
import requests
from json import JSONDecoder

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from pyxlsb import open_workbook as open_xlsb
import ssl

from selenium.webdriver.common.keys import Keys

from dateutil.relativedelta import relativedelta
from dateutil import parser

import boto3

from botocore.config import Config



import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import adqvest_s3


#functions
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
no_of_ping = 0

def find_month(x):
    if("jan" in x.lower()):
        month = "January"
    elif("feb" in x.lower()):
        month = "February"
    elif("mar" in x.lower()):
        month = "March"
    elif("apr" in x.lower()):
        month = "April"
    elif("may" in x.lower()):
        month = "May"
    elif("jun" in x.lower()):
        month = "Jun"
    elif("jul" in x.lower()):
        month = "July"
    elif("aug" in x.lower()):
        month = "August"
    elif("sep" in x.lower()):
        month = "September"
    elif("oct" in x.lower()):
        month = "October"
    elif("nov" in x.lower()):
        month = "November"
    elif("dec" in x.lower()):
        month = "December"



    return month

def date_value(x):
    x = x.strip()
    try:
        x = datetime.datetime.strptime(x, '%B %Y').date()
    except:
        x = datetime.datetime.strptime(x, '%b %Y').date()
    return datetime.date(x.year, x.month, calendar.monthrange(x.year, x.month)[1])

def clean_col(x):
    x = re.sub(r'  +',' ',x)
    x = x.strip()
    x = x.replace(' ','_')
    return x

def find_date(x):
    try:
        x = x.replace('"','')
        x = x.replace('–','')
        x = x.replace('-','')
        x = re.sub(r'  +',' ',x)
        x = x.replace('\u200b','')
        x = x.strip()
        try:
            date = datetime.datetime.strptime(x,'%B %Y').date()
        except:
            try:
                date = datetime.datetime.strptime(x,'%b %Y').date()
            except:
                try:
                    date = datetime.datetime.strptime(x,'%B%Y').date()
                except:
                    date = datetime.datetime.strptime(x,'%b%Y').date()

        y = int(date.strftime('%Y'))
        m = int(date.strftime('%m'))
        d = calendar.monthrange(y, m)[1]

        return datetime.date(y,m,d)
    except:
        raise Exception("Error in find_date")

url = 'https://utimf.com/about/statutory-disclosures/disclosure-of-aum/'
chrome_driver =r"C:\Users\Administrator\chromedriver.exe"
download_file_path = r"C:\Users\Administrator\AMC"
#     os.chdir('C:/Users/Administrator/AMC')
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

driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

driver.get(url)
#driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
time.sleep(1)

soup = BeautifulSoup(driver.page_source)

driver.quit()


links = soup.findAll("a",href = True)
title = [x.text for x in links]
links_df = pd.DataFrame({"Links":links,"Title":title})
links_df = links_df[(links_df["Title"].str.lower().str.contains("scheme")) & (links_df["Title"].str.lower().str.contains("aaum"))]
links_df["Links"] = links_df["Links"].apply(lambda x: x.get("href"))
links_df.reset_index(drop = True,inplace = True)

links_df = links_df[:links_df[links_df["Title"].str.lower().str.contains("march 2019")].index[0] + 1]

links_df["Title"] = links_df["Title"].apply(lambda x: x.split("Schemewise")[0].strip())

links_df["Year"] = links_df["Title"].apply(lambda x: re.findall(r'[0-9]{4}',x)[0])
links_df["Year"] = links_df["Year"].apply(lambda x: str(x))

links_df["Month"] = links_df["Title"].apply(lambda x: re.findall(r'[A-Za-z]{3}',x)[0])
links_df["Relevant_Date"] = links_df["Month"] + " " + links_df["Year"]
links_df["Relevant_Date"] = links_df["Relevant_Date"].apply(lambda x: date_value(x))

links_df["Links"] = links_df["Links"].apply(lambda x: x.replace(" ","%20"))

Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'UTI MF'",engine)
Latest_Date = Latest_Date["Max"][0]
print(links_df)
links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
links_df.reset_index(drop = True,inplace = True)
print(links_df)
