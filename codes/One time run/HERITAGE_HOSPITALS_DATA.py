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





start_url = "http://223.31.103.204/HeritageWeb/AREA_STATE_CITY"
driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
driver.get(start_url)
driver.maximize_window()
time.sleep(2)

heritage_df = pd.DataFrame()

os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run")
soup = BeautifulSoup(driver.page_source)
pattern = "StateProvince-DropDownCls(.*)"

state_list = soup.findAll(class_ = re.compile(pattern))
state_list = state_list[0].findAll("option")[1:]
for state in state_list:
    driver.find_element_by_xpath('//*[@id="StateProvince-DropDownID"]/option[contains(text(),"'+state.text+'")]').click()

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source)

    pattern = "City-DropdownCls(.*)"
    city_list = soup.findAll(class_ = re.compile(pattern))
    city_list = city_list[0].findAll("option")[1:]

    for city in city_list:

        driver.find_element_by_xpath('//*[@id="City-DropdownID"]/option[contains(text(),"'+city.text+'")]').click()

        time.sleep(2)

        soup = BeautifulSoup(driver.page_source)


        insur_list = soup.findAll(attrs = {'id':"ddlInsuranceCompany"})
        insur_list = insur_list[0].findAll("option")[1:]

        for insur in insur_list:

            driver.find_element_by_xpath('//*[@id="ddlInsuranceCompany"]/option[contains(text(),"'+insur.text+'")]').click()

            time.sleep(2)

            driver.find_element_by_xpath("//*[@id='btnSave']").click()
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source)

            temp_df = pd.DataFrame()

            limit = 0
            while True:
                try:

                    with open ("heritage.html","w",encoding = 'utf-8') as f:
                        f.write(str(soup))
                        f.close()
                    df = pd.read_html('heritage.html')[0]

                    address_list = soup.findAll("label",attrs = {"for":"hospitalist_ADDRESS"})
                    city_list = soup.findAll("label",attrs = {"for":"hospitalist_CITY"})
                    state_list = soup.findAll("label",attrs = {"for":"hospitalist_STATE"})
                    pin_list = soup.findAll("label",attrs = {"for":"hospitalist_PIN"})
                    df = df[["Hospital Name"]]
                    df = df[df["Hospital Name"] != "Hospital Name"]
                    df["Address"] = address_list
                    df["City"] = city_list
                    df["State"] = state_list
                    df["Pincode"] = pin_list
                    df["Insurance_Provider"] = insur.text

                    if(temp_df.empty):
                        pass

                    elif(temp_df.equals(df)):
                        raise Exception

                    heritage_df = pd.concat([heritage_df,df])

                    driver.find_element_by_xpath("//*[@id='NetWorkHospitalGrid_next']").click()
                    os.remove("heritage.html")
                    temp_df = df.copy()
                    time.sleep(2)
                except:
                    print("stopped")

                    break
            time.sleep(2)
            driver.get(start_url)
            driver.find_element_by_xpath('//*[@id="StateProvince-DropDownID"]/option[contains(text(),"'+state.text+'")]').click()
            time.sleep(3)
            driver.find_element_by_xpath('//*[@id="City-DropdownID"]/option[contains(text(),"'+city.text+'")]').click()

heritage_df["Address"] = heritage_df["Address"].apply(lambda x: x.next_sibling)
heritage_df["State"] = heritage_df["State"].apply(lambda x: x.next_sibling)
heritage_df["City"] = heritage_df["City"].apply(lambda x: x.next_sibling)
heritage_df["Pincode"] = heritage_df["Pincode"].apply(lambda x: x.next_sibling)
heritage_df["Relevant_Date"] = today.date()
heritage_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
heritage_df["TPA_Name"] = "Heritage Health TPA Pvt Ltd"
heritage_df["Hospital_Type"] = "TPA"
heritage_df.rename(columns = {"Hospital Name":"Hospital_Name"},inplace = True)
heritage_df.to_excel("heritage.xlsx",index = False)
heritage_df.to_sql(name = "GENERAL_INSURANCE_HOSPITALS",if_exists = "append",index = False,con = engine)
