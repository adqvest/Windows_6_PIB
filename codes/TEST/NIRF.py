
import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar
import os
import requests
import json
from bs4 import BeautifulSoup
import random
import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io
import csv
import numpy as np
from time import sleep
import time
import sys
import PyPDF2
from PyPDF2 import PdfFileReader,PdfFileWriter
from dateutil.relativedelta import relativedelta

from playwright.sync_api import sync_playwright
import urllib
from urllib.request import Request, urlopen
from pytz import timezone
from requests_html import HTMLSession
import glob
import camelot
# import boto3
# from botocore.config import Config
import warnings
from urllib.parse import urlparse
warnings.filterwarnings('ignore')
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
# os.chdir(r'C:\Users\dell\Downloads')
import adqvest_db
from tabula.io import read_pdf
from dateutil import parser
import datetime as datetime
from pandas.tseries.offsets import MonthEnd

from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from selenium.webdriver.common.by import By
import camelot
import json
from json import JSONDecoder

pd.options.display.max_columns = None
pd.options.display.max_rows = None

driver_path = r'C:\Users\Administrator\AdQvestDir\chromedriver'
download_file_path = r'C:\Users\Administrator\Junk\NIRF'
#mysql

engine = adqvest_db.db_conn()
connection = engine.connect()

#clickhouse
from clickhouse_driver import Client
client = Client('ec2-3-109-104-45.ap-south-1.compute.amazonaws.com',
                user='default',
                password='Cli@dbaDb378',
                database='AdqvestDB',
               port=9000)

# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days

download_file_path = r'C:\Users\Administrator\Junk\NIRF'
chrome_driver_path = r'C:\Users\Administrator\AdQvestDir\chromedriver'
# driver = webdriver.Chrome(executable_path=chrome_driver_path)


headers = {'authority': 'www.nirfindia.org',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

r = requests.get('https://www.nirfindia.org/2023/UniversityRanking.html',headers = headers,timeout=60)
soup = BeautifulSoup(r.content)
a_tag = [i for i in soup.find_all('a',href=True) if '.pdf' in i['href']]
links = [i['href'] for i in a_tag]

# print(len(links))
os.chdir(r'C:\Users\Administrator\Junk\NIRF')
wd = r'C:\Users\Administrator\Junk\NIRF'
# for lin in links:
#     file = os.path.basename(lin)
#     r1 = requests.get(lin,headers,timeout=60)
#     with open(file,'wb') as f:
#         f.write(r1.content)
#         f.close()
#     #break
os.chdir(r'C:\Users\Administrator\Junk\NIRF') 
excel = os.listdir()
pdf = os.listdir()

pdf = [i.replace('.pdf','') for i in pdf if '.pdf' in i]
excel = [i.replace('.xlsx','') for i in excel if '.xlsx' in i]
remaining_files = list(set(pdf)-set(excel))
remaining_files = [i+'.pdf' for i in remaining_files]

file_names = os.listdir()
os.chdir(r'C:\Users\Administrator\Junk\NIRF')
# for file_name in file_names[95:]:
i = 0 
for file_name in remaining_files :
    prefs = {'download.default_directory': download_file_path}
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=chrome_driver_path,options=chrome_options)
    
    driver.get("https://www.ilovepdf.com/")
    driver.find_element("xpath", "//*[contains(text(),'Login')]").click()
    email = driver.find_element("xpath", "//*[@id='loginEmail']")
    email.send_keys("kartmrinal101@outlook.com")
    password = driver.find_element("xpath", "//*[@id='inputPasswordAuth']")
    password.send_keys("zugsik-zuqzuH-jyvno4")
    time.sleep(3)
    driver.find_element("xpath", "//*[@id='loginBtn']").click()
    time.sleep(3)
    driver.find_element("xpath", "//*[contains(text(),'PDF to Excel')]").click()
    time.sleep(3)
    input_element = driver.find_element("xpath", "//*[@type='file']")
    time.sleep(10)
    input_element.send_keys(wd + '\\' + file_name)
    time.sleep(9)
    driver.find_element("xpath", "//*[@id='processTask']").click()
    time.sleep(15)
    driver.close()
    print('Done for :',file_name)
    time.sleep(30)
    i += 1
    if i % 5 == 0:
        time.sleep(30)
    else:
        pass
    os.remove(wd + '\\' + file_name)

    #break



# file_names[39:]

# ###################
# '''Cleaning part'''






# # SPONSORED TESTING
# os.chdir(r'C:\Users\Administrator\Junk\NIRF') 
# remaining_files = os.listdir()
# excluded = []
# for file_name in remaining_files:

#     file = pd.ExcelFile(file_name)
#     sheets = file.sheet_names
#     tbls = pd.DataFrame()
#     for i in sheets:
#         temp = pd.read_excel(file_name, sheet_name=i, header=None)
#         temp = temp.dropna(how='all',axis=1)
#         tbls = pd.concat([tbls, temp], ignore_index=True)
#     tbls = tbls.reset_index(drop=True)
    
#     institute = tbls.iloc[:, 0][tbls.iloc[:, 0].str.lower().str.contains('institute name', na=False)].values[0]
#     institute_name = institute.split(':')[-1].split('[')[0].strip().replace('  ', ' ')

#     # Sponsored Research Details
#     st_idx = tbls[tbls.iloc[:, 0].str.lower().str.contains('sponsored', na=False)].index[0]#
#     end_idx = tbls[tbls.iloc[:, 0].str.lower().str.contains('consultancy', na=False)].index[0]
#     df4 = tbls.iloc[st_idx:end_idx, :].reset_index(drop=True)
#     df4 = df4.dropna(how='all', axis=1)
    
#     for i in range(3):
#         for index, row in df4.iterrows():
#             nan_indices = np.where(row.isna())[0]
#             for nan_index in nan_indices:
#                 #print(nan_index)
#                 if nan_index != len(row) - 1:  # Check if NaN is not at the end
#                     df4.iloc[index, nan_index] = df4.iloc[index, nan_index + 1]
#                     df4.iloc[index, nan_index + 1] = np.nan
#     df4 = df4.dropna(how='all', axis=1)
    
#     df4.columns = df4[df4.iloc[:, 0].str.lower().str.contains('year', na=False)].iloc[0, :].values
#     df4 = df4[~df4.iloc[:, 0].str.lower().str.contains('year|details', na=False)]
    
#     #df4 = df4.dropna(how='any', axis=0)
#     df4 = df4.reset_index(drop=True)
#     df4 = df4.T.reset_index()
#     df4.columns = df4[df4.iloc[:, 0].str.lower().str.contains('year', na=False)].iloc[0, :].values
#     df4 = df4[~df4.iloc[:, 0].str.lower().str.contains('year', na=False)]
#     df4 = df4.reset_index(drop=True)
#     df4.columns = [i.replace('(', '').replace(')', '').replace('.', '').title().replace(' ', '_') for i in df4.columns]
#     df4 = df4.rename(columns={'Total_Amount_Received_Amount_In_Rupees':'Amount_INR'})
#     df4['Relevant_Date'] = pd.to_datetime(df4.Financial_Year.apply(lambda x: '31-3-' + str(x.split('-')[-1]))).dt.date
#     df4 = df4.drop('Amount_Received_In_Words',axis=1)
#     df4['Institute_Name'] = institute_name
#     df4.columns
#     df4 = df4[['Institute_Name', 'Total_No_Of_Sponsored_Projects', 'Total_No_Of_Funding_Agencies', 'Amount_INR', 'Financial_Year', 'Relevant_Date']]
#     df4 = df4.rename(columns = {'Total_No_Of_Sponsored_Projects':'Sponsored_Projects_Nos', 'Total_No_Of_Funding_Agencies':'Funding_Agencies_Nos'})
#     df4['Runtime'] = datetime.datetime.now()
    
#     #df4.to_sql('NIRF_INSTITUTE_WISE_SPONSERED_RESEARCH_DETAILS_YEARLY_DATA', if_exists='append', index=False, con=engine)

#     # else:
#     #     continue
    
#     print('Done for ',institute_name)





        
        
        