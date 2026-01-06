# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 15:09:34 2021

@author: Abhishek Shankar
"""


import scrapy
import re
import datetime as datetime
from selenium.webdriver.chrome.options import Options
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import os
import calendar
#os.chdir(r"C:\Adqvest\Mercadolibre")
import sqlalchemy
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from dateutil import parser
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from random import randint
from time import sleep
import requests, zipfile, io
import time
import sys
#sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
sys.path.insert(0, r'C:\Adqvest')
import adqvest_db
import JobLogNew as log
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

options = webdriver.ChromeOptions()

download_file_path = r'C:\Adqvest\CAG'
prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            'profile.default_content_setting_values.automatic_downloads': 1
            }


options = webdriver.ChromeOptions()
options.add_experimental_option('prefs', prefs)
options.add_argument("--disable-infobars")
options.add_argument("start-maximized")
options.add_argument("--disable-extensions")
options.add_argument("--disable-notifications")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--no-sandbox')


def Selectit(param_id,text):

    select = wait(driver, 10).until(EC.presence_of_element_located((By.NAME, param_id)))
    Select(select).select_by_visible_text(text)
    
    return driver

#def tick(text):
#   driver.find_element_by_xpath(
#        ".//*[contains(text(), "+text+")]"
#    ).click()
#
#    return driver
def get_data(driver):
#    print("DATA_GETTING_ACQUIRED")
#    main = []
#    for table in driver.find_elements_by_xpath('//*[contains(@id,"cphBody_ctl07")]//tr'):
#        data = [item.text for item in table.find_elements_by_xpath(".//*[self::td or self::th]")]
#        main.append(data)
#    
#    cols = main[0]    
#    df = pd.DataFrame(columns=cols)
#    for i in range(len(main)):
#        if ((main[i]==cols)|('>>' in main[i])):
#            continue
#        else:
#            df.loc[i] = main[i]
#
    print("DATA ACQUIRED")
    name = []
    for files in os.listdir(r"C:\Adqvest\CAG"):
        if files.endswith(".csv"):
            print(files)
            name.append(files)
    
    df = pd.read_csv("C:/Adqvest/CAG/"+name[0])
    df['Relevant_Date'] = today.date()
    df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
    df.to_sql(name='NVST',con=engine,if_exists='append',index=False)

    os.remove("C:/Adqvest/CAG/"+name[0])
    
    
    return df

def get_table(driver):
    select = wait(driver, 10).until(EC.presence_of_element_located((By.NAME, all_id['Search'])))
#    select.click()
    driver.execute_script("arguments[0].click();", select)
    
    driver.execute_script("document.body.style.zoom='30%'")
    
    driver.execute_script("arguments[0].click();", driver.find_element_by_xpath("//span[@class='icon download']"))
    
#    driver.execute_script("window.scrollTo(10, Y)") 
    driver.execute_script("arguments[0].click();", driver.find_element_by_xpath("//a[@id='cphBody_lbtnCSV']"))
    
#    driver.execute_script("arguments[0].click();", wait(driver, 10).until(EC.presence_of_element_located((By.NAME, "cphBody_lbtnCSV"))))

    
#    select = wait(driver, 10).until(EC.presence_of_element_located((By.NAME, "cphBody_lbtnCSV")))
#    select.click()
#    driver.execute_script("arguments[0].click();", select)
    time.sleep(5)
    return driver
#%%

url = 'https://www.ncvtmis.gov.in/Pages/ITI/Search.aspx'

headers = {
   "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
   }

r = requests.get(url,headers=headers)

soup1 = BeautifulSoup(r.text,'lxml')


'''

step 1

'''

exam = soup1.find_all("select",{"title":"Exam System"})

exam_id = exam[0]['name']

exam_name = exam[0].find_all("option")
exam_name = [x.text for x in exam_name]

exam_value = exam[0].find_all("option")
exam_value = [x['value'] for x in exam_value]

exam_name = [x for x in exam_name if x!= '-Select-']
exam_value = [x for x in exam_value if x!= '-1']
'''

step 2

'''

states = soup1.find_all("select",{"title":"State"})
state_id = states[0]['name']

state_name = states[0].find_all("option")
state_name = [x.text for x in state_name]

state_value = states[0].find_all("option")
state_value = [x['value'] for x in state_value]

state_value = [x for x in state_value if x!= '-Select-']
state_name = [x for x in state_name if x!= '-1']

'''

step 3

'''

catgs = soup1.find_all("select",{"id":"cphBody_ddlOtherCategory"})
catgs_id = catgs[0]['name']

catgs_name = catgs[0].find_all("option")
catgs_name = [x.text for x in catgs_name]

catgs_value = catgs[0].find_all("option")
catgs_value = [x['value'] for x in catgs_value]

catgs_value  = [x for x in catgs_value  if x!= '-Select-']
catgs_name = [x for x in catgs_name if x!= '-1']
'''

Step 4

'''
check_boxes = soup1.find_all("span",{"class":"search checkbox"})

check_box_id = [x.find_all("input") for x in check_boxes]
check_box_id = [x[0]['name'] for x in check_box_id]

check_box_name = [x.find_all("label") for x in check_boxes]
check_box_name = [x[0].text for x in check_box_name]


'''

step 5

'''

district = soup1.find_all("select",{"title":"District"})
district_id = district[0]['name']

district_name = district[0].find_all("option")
district_name = [x.text for x in district_name]

district_value = district[0].find_all("option")
district_value = [x['value'] for x in district_value]

district_value   = [x for x in district_value   if x!= '-Select-']
district_name = [x for x in district_name if x!= '-1']


'''

step 6

'''

trade = soup1.find_all("select",{"title":"Trade"})
trade_id = trade[0]['name']

trade_name = trade[0].find_all("option")
trade_name = [x.text for x in trade_name]

trade_value = trade[0].find_all("option")
trade_value = [x['value'] for x in trade_value]

trade_value   = [x for x in trade_value   if x!= '-Select-']
trade_name = [x for x in trade_name if x!= '-1']

'''

step 7

'''

scheme = soup1.find_all("select",{"title":"Css Scheme"})
scheme_id = scheme[0]['name']

scheme_name = scheme[0].find_all("option")
scheme_name = [x.text for x in scheme_name]

scheme_value = scheme[0].find_all("option")
scheme_value = [x['value'] for x in scheme_value]


all_id = {
        "Exam":exam_id,
        "State":state_id,
        "ITI":"ctl00$cphBody$txtIT",
        "Category":catgs_id ,
        "Code":"ctl00$cphBody$txtCode",
        "District":district_id, 
        "Trade":trade_id ,
        "Scheme":scheme_id,
        "Search":"ctl00$cphBody$btnSearch"
        }


#driver = webdriver.Chrome(executable_path=r"C:\Adqvest\Selenium Extension\chromedriver.exe",options = options)
#
#
#url = 'https://www.ncvtmis.gov.in/Pages/ITI/Search.aspx'
#
#driver.get(url)
#
#driver.find_element_by_xpath('//select[@id="'+all_id['Exam']+'"]//option[@value="'+str(1)+'"]')
#
#
#select = wait(driver, 10).until(EC.presence_of_element_located((By.NAME, all_id['Exam'])))
#Select(select).select_by_visible_text(exam_name[0])
#
#driver.find_element_by_xpath(
#    ".//*[contains(text(), 'Search')]"
#).click()
#
#
#soup = BeautifulSoup(driver.page_source)
#pd.read_html()
#
#main = []
#for table in driver.find_elements_by_xpath('//*[contains(@id,"cphBody_ctl07")]//tr'):
#    data = [item.text for item in table.find_elements_by_xpath(".//*[self::td or self::th]")]
#    main.append(data)
#
#cols = main[0]    
#df = pd.DataFrame(columns=cols)
#for i in range(len(main)):
#    if ((main[i]==cols)|('>>' in main[i])):
#        continue
#    else:
#        df.loc[i] = main[i]
    
#    print(data)    


driver = webdriver.Chrome(executable_path=r"C:\Adqvest\Selenium Extension\chromedriver.exe",options = options)
url = 'https://www.ncvtmis.gov.in/Pages/ITI/Search.aspx'

driver.get(url)
driver.execute_script("document.body.style.zoom='80%'")
for name1,val1 in zip(exam_name,exam_value):
#    driver.execute_script("document.body.style.transform = 'scale(0.8)'")
    time.sleep(2)
    Selectit(all_id['Exam'],name1)
    
    time.sleep(2)
    Selectit(all_id['State'],"KARNATAKA")
#    break
    time.sleep(5)
    #level1
    driver.execute_script("document.body.style.zoom='80%'")
    driver.execute_script("window.scrollTo(0, 50)")
    driver = get_table(driver)    
    df = get_data(driver)
    
#    for name2,val2 in zip(catgs_name,catgs_value):
#        Selectit(all_id['Category'],name2)
#        time.sleep(5)
#        #level2
#        driver = get_table(driver)
#        driver.execute_script("window.scrollTo(0, 80)")
#        df = get_data(driver)        
#        for name3 in ["Government","Private"]:
#            driver.find_element_by_xpath(
#            ".//*[contains(text(), '"+name3+"')]").click()
#            time.sleep(5)
#            #level3
#            driver = get_table(driver)
#            driver.execute_script("window.scrollTo(0, 80)")    
#            df = get_data(driver)            
#            
#            for name4,val4 in zip(district_name,district_value):
#               Selectit(all_id['District'],name4)
#               time.sleep(5)
#               #level4
#               driver.execute_script("window.scrollTo(0, 80)")
#               driver = get_table(driver)
#               df = get_data(driver)
#               
#               
#               for name5,val5 in zip(trade_name,trade_value):
#                   Selectit(all_id['Trade'],name5)
#                   time.sleep(5)
#                   #level5
#                   driver.execute_script("window.scrollTo(0, 80)")
#                   driver = get_table(driver)    
#                   df = get_data(driver)
#                   for name6 in ["Rural","Urban"]:
#                        driver.find_element_by_xpath(
#                        ".//*[contains(text(), '"+name3+"')]").click()
#                        time.sleep(5)
#                        
#                        driver.execute_script("window.scrollTo(0, 80)")
#                        driver = get_table(driver)
#                        df = get_data(driver)
#                        
#                        for name7,val7 in zip(scheme_name,scheme_value):
#                           Selectit(all_id['District'],name4)
#                           time.sleep(5)
#                           driver.execute_script("window.scrollTo(0, 80)")
#                           driver = get_table(driver)
#                           df = get_data(driver)
    
    driver.quit()
    driver = webdriver.Chrome(executable_path=r"C:\Adqvest\Selenium Extension\chromedriver.exe",options = options)
    driver.get(url)

driver.quit()