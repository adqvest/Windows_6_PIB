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
import io

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
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains

from dateutil.relativedelta import relativedelta
from dateutil import parser
from selenium.webdriver.common.by import By
import xlwings
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
#functions
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
connection = engine.connect()


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
        try:
            x = datetime.datetime.strptime(x, '%b %Y').date()
        except:
            try:
                x = datetime.datetime.strptime(x, '%B %y').date()
            except:
                x = datetime.datetime.strptime(x, '%b %y').date()
        
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
        
# def TATA(url):
#     try:

#         chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
#         download_file_path = r"C:\Users\Administrator\AMC"
#         os.chdir('C:/Users/Administrator/AMC')
#         prefs = {
#             "download.default_directory": download_file_path,
#             "download.prompt_for_download": False,
#             "download.directory_upgrade": True
#             }
#         options = webdriver.ChromeOptions()
#         options.add_argument("--disable-infobars")
#         options.add_argument("start-maximized")
#         options.add_argument("--disable-extensions")
#         options.add_argument("--disable-notifications")
#         options.add_argument('--ignore-certificate-errors')
#         options.add_argument('--no-sandbox')

#         options.add_experimental_option('prefs', prefs)

#         driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

#         driver.get(url)

#         time.sleep(20)

#         #element = driver.find_element_by_xpath('//*[@id="main-wrap"]/div[2]/section/div/div/div/div[7]')
#         element = driver.find_element(By.XPATH, "//*[contains(text(),'Disclosure for Monthly AAUM ')]")


#         desired_y = (element.size['height'] / 2) + element.location['y']
#         current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
#         scroll_y_by = desired_y - current_y
#         driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
#         time.sleep(10)
#         element.click()
#         time.sleep(10)
#         soup = BeautifulSoup(driver.page_source)
#         time.sleep(2)
#         all_elements = soup.findAll('div', class_ = 'fadv-accord-Content')
#         links = all_elements[3].findAll('li')

#         dates_list = []
#         for i in links:
#             a = i.find('a').text
#             a = parser.parse(a, fuzzy = True).strftime('%B %Y')
#             a = date_value(a)
#             a = a.strftime('%Y-%m-%d')
#             dates_list.append(a)

#         links_list = []
#         for i in links:
#             a = i.find('a')['href']
#             if 'http' not in a:
#                 a = 'https://www.tatamutualfund.com/' + a
#             else:
#                 pass
#             links_list.append(a)

#         driver.close()


#         links_df=pd.DataFrame()
#         links_df['Links']=links_list
#         links_df['Relevant_Date']=dates_list
#         links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
#         links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())
#         print(links_df)
#         Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'TATA MF'",engine)
#         Latest_Date = Latest_Date["Max"][0]

#         links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

#         return links_df
#     except:
#         error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
#         error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
#         error_type = error_type + " (Error in TATA)"
#         error_msg = error_msg + " (Error in TATA)"
#         print(error_type)
#         print(error_msg)

# input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM' and AMC_Name IN ('TATA MF')",engine)

# amc_df = TATA(input_file_df.iloc[0,1])


# url = amc_df["Links"].iloc[0]
# Relevant_Date = amc_df['Relevant_Date'].iloc[0]
# print(url)

# try:
#     os.remove("monthly_mf.xls")
# except:
#     pass
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
# #no_of_ping += 1
# r = requests.get(url,headers = headers)
# with open('monthly_mf.xls', 'wb') as f:
#         f.write(r.content)
#         f.close()

file_name = r'C:\Users\Administrator\AMC\monthly_mf.xls'

df = pd.read_excel(file_name)
print(df.head())
print('end')