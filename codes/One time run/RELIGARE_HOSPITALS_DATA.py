import sqlalchemy
import pandas as pd
from pandas.io import sql
import calendar

import os
import requests
import json
from bs4 import BeautifulSoup


from time import sleep
import random

import re
import ast
import datetime as datetime
from pytz import timezone
import requests, zipfile, io

import csv
import numpy as np
import zipfile
import sys
import time

#import camelot
from lxml import etree

import warnings
warnings.filterwarnings('ignore')
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db

os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()

india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days




def lambda_pincode(x):
    try:
        pin = re.findall(r'[0-9]{6}',x)[0]
        return pin
    except:
        return np.nan



driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
driver.get('https://www.religarehealthinsurance.com/health-plan-network-hospitals.html')
time.sleep(1)



religare_df = pd.DataFrame()
state_list = soup.findAll("select",attrs = {"id":"searchState"})[0].findAll("option")



for state in state_list:
    driver.find_element_by_xpath("//*[@id='searchState']/option[text()='"+state.text+"' ]").click()
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'lxml')

    city_list = soup.findAll("select",attrs = {"id":"city"})[0].findAll("option")


    for city in city_list:
        print("state:",state)
        print("city:",city)
        try:
            driver.find_element_by_xpath("//*[@id='city']/option[text()='"+city.text+"' ]").click()
        except:
            continue
        time.sleep(1)
        driver.find_element_by_xpath('//*[@id="button"]').click()
        time.sleep(1)

        limit = 0
        while True:
            try:
                name_list = []
                address_list = []
                soup = BeautifulSoup(driver.page_source, 'lxml')
                time.sleep(0.5)
                address_list.append(soup.findAll(class_ = "locationContainer"))
                name_list.append(soup.findAll("code"))
                address_list = [x.get_text() for x in address_list[0]]
                name_list = [x.get_text() for x in name_list[0]]

                df = pd.DataFrame({"Address":address_list,"Hospital_Name":name_list})
                df["State"] = state.text
                df["City"] = city.text
                religare_df = pd.concat([religare_df,df])
                driver.find_element_by_xpath("//*[contains(@rel,'next')]").click()
                time.sleep(1)
            except:
                if(limit == 0):
                    limit += 1
                else:
                    print("here")

                    break





religare_df["Address"] = religare_df["Address"].apply(lambda x: x.replace("\n",""))
religare_df["Pincode"] = religare_df["Address"].apply(lambda x: lambda_pincode(x))
religare_df["Relevant_Date"] = today.date()
religare_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
religare_df.to_sql(name = "GENERAL_INSURANCE_HOSPITALS",if_exists = "append",index = False,con = engine)
