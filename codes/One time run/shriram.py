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

driver.get("https://www.stfc.in/branch-locator/")

driver.maximize_window()
time.sleep(5)

soup = BeautifulSoup(driver.page_source)

sriram_df = pd.DataFrame()


state_list = soup.findAll(attrs = {"name":"statename"})[0].findAll("option")[1:]
for state in state_list:
    driver.find_element_by_xpath("//*[@name='statename']//*[contains(text(),'"+state.text+"')]").click()
    time.sleep(1.5)
    soup = BeautifulSoup(driver.page_source)
    city_list = soup.findAll(attrs = {"name":"cityname"})[0].findAll("option")[1:]
    for city in city_list:
        try:
            driver.find_element_by_xpath("//*[@name='cityname']//*[contains(text(),'"+city.text+"')]").click()
        except:
            continue
        time.sleep(1.5)
        soup = BeautifulSoup(driver.page_source)
        branch_list = soup.findAll(attrs = {"name":"branchname"})[0].findAll("option")[1:]

        for branch in branch_list:
            try:
                driver.find_element_by_xpath("//*[@name='branchname']//*[contains(text(),'"+branch.text+"')]").click()
            except:
                continue
            try:
                driver.get("https://www.stfc.in/branch-locator/")
                time.sleep(1)
                driver.find_element_by_xpath("//*[@name='statename']//*[contains(text(),'"+state.text+"')]").click()
                time.sleep(1.5)
                driver.find_element_by_xpath("//*[@name='cityname']//*[contains(text(),'"+city.text+"')]").click()
                time.sleep(1.5)
                driver.find_element_by_xpath("//*[@name='branchname']//*[contains(text(),'"+branch.text+"')]").click()
                time.sleep(1.5)
                driver.find_element_by_xpath("//*[@type = 'submit']").click()
                time.sleep(0.5)


            except :
                try:
                    print("Exception")
                    driver.get("https://www.stfc.in/branch-locator/")
                    time.sleep(1)
                    driver.find_element_by_xpath("//*[@name='statename']//*[contains(text(),'"+state.text+"')]").click()
                    time.sleep(1.5)
                    driver.find_element_by_xpath("//*[@name='cityname']//*[contains(text(),'"+city.text+"')]").click()
                    time.sleep(1.5)
                    driver.find_element_by_xpath("//*[@name='branchname']//*[contains(text(),'"+branch.text+"')]").click()
                    time.sleep(1.5)
                    driver.find_element_by_xpath("//*[@type = 'submit']").click()
                    time.sleep(0.5)
                except:
                    continue    



            soup = BeautifulSoup(driver.page_source)

            address_list = []
            address_list.extend(soup.findAll(class_ = "border"))

            df = pd.DataFrame({"Address":address_list,"City":city.text,"State":state.text})
            print(df)
            sriram_df = pd.concat([sriram_df,df])

sriram_df["Address"]  = sriram_df["Address"].apply(lambda x: x.get_text().replace("\t","").replace("\n",""))
sriram_df["Relevant_Date"] = today.date()
sriram_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
sriram_df["Pincode"] = sriram_df["Address"].apply(lambda x:lambda_pincode(x))
sriram_df["Brand"] = "Shriram Transport Finance Co. Ltd"

driver.quit()
sriram_df.to_excel("Shriram.xlsx",index = False)

sriram_df.to_sql(name = "STORE_LOCATOR_DATA",if_exists = "append",index = False,con = engine)
