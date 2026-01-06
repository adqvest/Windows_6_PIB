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
url = "https://www.healthindiatpa.com/Hospital_Search.aspx"

driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
driver.get(url)
driver.maximize_window()
time.sleep(2)

driver.find_element_by_xpath("/html/body/form/div[3]/div[5]/div/div/div[1]/button").click()
time.sleep(1)
driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$HospitalControl$ddlInsCompany"]/option[contains(text(),"HealthIndia Insurance TPA Services Pvt.Ltd.")]').click()
time.sleep(1)

hi_df = pd.DataFrame()

soup = BeautifulSoup(driver.page_source)
pattern = "(.*)ddlState"

state_list = soup.findAll(attrs = {'name':re.compile(pattern)})
state_list = state_list[0].findAll("option")[1:]
for state in state_list:
    print("State ",state.text)
    driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$HospitalControl$ddlState"]/option[contains(text(),"'+state.text+'")]').click()

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source)

    pattern = "(.*)ddlCity"
    city_list = soup.findAll(attrs = {'name':re.compile(pattern)})
    city_list = city_list[0].findAll("option")[1:]

    for city in city_list:
        print("City ",city.text)


        driver.find_element_by_xpath('//*[@name="ctl00$ContentPlaceHolder1$HospitalControl$ddlCity"]/option[contains(text(),"'+city.text+'")]').click()
        time.sleep(1)
        driver.find_element_by_xpath('//*[@id="ctl00_ContentPlaceHolder1_HospitalControl_btnSearch"]').click()
        time.sleep(1)


        try:
            alert = driver.switch_to_alert()
            alert.accept()
            cond = True
            print("Cond",cond)
        except:
            cond = False


        if(cond == True):
            continue

        else:
            print("here")
            i = 1
            while True:
                try:
                    try:
                        df_copy = df.copy()

                    except:
                        pass

                    #try:
                    soup = BeautifulSoup(driver.page_source)
                    with open ("hi_tpa.html","w",encoding = 'utf-8') as f:
                        f.write(str(soup))
                        f.close()

                    df = pd.read_html('hi_tpa.html')[3]
                    print(df)

                    try:
                        if(df.equals(df_copy)):
                            break
                    except:
                        pass

                    hi_df = pd.concat([hi_df,df])

                    driver.execute_script("javascript:__doPostBack('ctl00$ContentPlaceHolder1$HospitalControl$gvHospitals','Page$"+str(i)+"')")
                    i += 1
                except:
                    time.sleep(1)
                    driver.back()
                    try:
                        driver.find_element_by_xpath("/html/body/form/div[3]/div[5]/div/div/div[1]/button").click()
                        time.sleep(1)
                        break
                    except:
                        pass

            time.sleep(2)
                #raise Exception

hi_df = hi_df[["Hospital Name","City","State","Address"]]
hi_df.rename(columns = {"Hospital Name":"Hospital_Name"},inplace = True)

hi_df["Relevant_Date"] = today.date()
hi_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
hi_df["TPA_Name"] = "Health India Insurance TPA Services Pvt. Ltd"
hi_df.to_excel("hi_df.xlsx",index = False)
hi_df.to_sql(name = "GENERAL_INSURANCE_HOSPITALS",if_exists = "append",index = False,con = engine)
