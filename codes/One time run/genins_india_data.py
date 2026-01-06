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






os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run")


driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
url = "http://www.geninsindia.com/providr.aspx"


driver.get(url)
driver.maximize_window()
time.sleep(2)


genins_df = pd.DataFrame()

soup = BeautifulSoup(driver.page_source)
pattern = "(.*)drpuwd"
"ctl00$ContentPlaceHolder1$drpuwd"

insurer_list = soup.findAll(attrs = {'name':re.compile(pattern)})
insurer_list = insurer_list[0].findAll("option")[1:]

for insurer in insurer_list:

    driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$drpuwd"]/option[contains(text(),"'+insurer.text+'")]').click()
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source)
    pattern = "(.*)drpState"

    state_list = soup.findAll(attrs = {'name':re.compile(pattern)})
    state_list = state_list[0].findAll("option")[1:]
    #state_list = [x for x in state_list if (x.text == "JHARKHAND")|(x.text == "KARNATAKA")]
    #state_list = [x for x in state_list if x.text == "Karnataka"]



    for state in state_list:

        driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$drpState"]/option[contains(text(),"'+state.text+'")]').click()
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source)
        pattern = "(.*)TxtCitys"

        city_list = soup.findAll(attrs = {'name':re.compile(pattern)})
        city_list = city_list[0].findAll("option")[1:]

        for city in city_list:
            time.sleep(3)
            try:
                driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$TxtCitys"]/option[contains(text(),"'+city.text+'")]').click()
                time.sleep(3)
            except:
                driver.refresh()
                driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$drpState"]/option[contains(text(),"'+state.text+'")]').click()
                time.sleep(3)
                driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$TxtCitys"]/option[contains(text(),"'+city.text+'")]').click()
            time.sleep(3)    
            driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_CmdGo"]').click()
            time.sleep(3)

            print(insurer.text)
            print(state.text)
            print(city.text)

            soup = BeautifulSoup(driver.page_source)
            with open ("genins_tpa.html","w",encoding = 'utf-8') as f:
                f.write(str(soup))
                f.close()

            df = pd.read_html('genins_tpa.html')[0]
            try:
                df.columns = df.loc[df[df[0] == "Hospital"].index[0]]
            except:
                driver.back()
                continue

            df = df[df[df["Hospital"] == "Hospital"].index[0] + 1:]
            df.dropna(inplace = True)
            df["Insurance_Provider"] = insurer.text
            print(df)

            genins_df = pd.concat([genins_df,df])
            driver.back()

genins_df = genins_df[["Hospital","Address","City","State","PinCode","Insurance_Provider"]]
genins_df.rename(columns = {"Hospital":"Hospital_Name"},inplace = True)
genins_df["TPA_Name"] = "Genins India TPA Ltd"




genins_df["Relevant_Date"] = today.date()
genins_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
genins_df.to_excel("genins_df.xlsx",index = False)
genins_df.to_sql(name = "GENERAL_INSURANCE_HOSPITALS",if_exists = "append",index = False,con = engine)
