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

start_url = "https://pizzaonline.dominos.co.in/menu"
driver.get(start_url)

driver.maximize_window()
driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[3]/div[1]/div/div[2]/label").click()
time.sleep(2)

driver.find_elements_by_xpath("//*[@class = 'lbl--wrpr']")[0].click()
time.sleep(1)
soup = BeautifulSoup(driver.page_source,'lxml')
state_list = soup.findAll(class_ ='lbl--wrpr')[0]
state_list = state_list.findNext('ul').findAll("li")
state_list_str = [x.text for x in state_list]

address_list = []
city_l = []
loc_l = []
# state_list = [x for x in state_list if ((x.text == 'NEW DELHI')|(x.text == 'OOTY')|(x.text == 'NOIDA'))]
for state in state_list:
    driver.get(start_url)
    driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[3]/div[1]/div/div[2]/label").click()
    time.sleep(2)

    driver.find_elements_by_xpath("//*[@class = 'lbl--wrpr']")[0].click()
    time.sleep(1.5)
    state_search_box = driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[2]/div/div[3]/div/div[1]/div[2]/div[1]/div/div/div/input")
    state_search_box.send_keys(state.text)
    time.sleep(1)
    driver.find_element_by_xpath("//*[@data-label='"+state['data-label']+"']").click()
    time.sleep(1.5)

    driver.find_elements_by_xpath("//*[@class = 'lbl--wrpr']")[1].click()
    time.sleep(1.5)

    soup = BeautifulSoup(driver.page_source,'lxml')
    city_list = soup.findAll(class_ ='lbl--wrpr')[1]
    city_list = city_list.findNext('ul').findAll("li")

    for city in city_list:
        driver.find_elements_by_xpath("//*[@class = 'lbl--wrpr']")[1].click()
        city_search_box = driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[2]/div/div[3]/div/div[1]/div[2]/div[2]/div/div/div/input")
        city_search_box.send_keys(city.text)
        time.sleep(1)
        driver.find_element_by_xpath("//*[contains(text(),'"+city.text+"')]").click()
        time.sleep(1.5)
        soup = BeautifulSoup(driver.page_source,'lxml')
        address_list.extend(soup.findAll(class_ = "pickupStoreCont"))

        driver.get(start_url)
        driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[3]/div[1]/div/div[2]/label").click()
        time.sleep(1.5)
        driver.find_elements_by_xpath("//*[@class = 'lbl--wrpr']")[0].click()
        time.sleep(1.5)
        state_search_box = driver.find_element_by_xpath("//*[@id='__next']/div/div/div[1]/div[1]/div/div[2]/div/div[3]/div/div[1]/div[2]/div[1]/div/div/div/input")
        state_search_box.send_keys(state.text)
        time.sleep(1)
        driver.find_element_by_xpath("//*[@data-label='"+state['data-label']+"']").click()
        time.sleep(1.5)

        city_l.append(city.text)

        loc_l.append(state.text)



df = pd.DataFrame({"City":loc_l,"Address":address_list})
df["Address"] = df["Address"].apply(lambda x: x.get_text())
df["Pincode"] = df["Address"].apply(lambda x: lambda_pincode(x))
df["Relevant_Date"] = today.date()
df["Brand"] = "Domino's Pizza"
df["Company"] = "Jubilant FoodWorks Limited"
df["Runtime"] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
driver.quit()
df.to_excel("dominos.xlsx",index = False)

df.to_sql(name = "STORE_LOCATOR_DATA",if_exists = "append",index = False,con = engine)
