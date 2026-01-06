# -*- coding: utf-8 -*-
"""
Created on Thurs June 08 17:07:25 2023

@author: Nidhi
"""
import sys
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
import time
import datetime
import calendar
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import re
import undetected_chromedriver as uc 
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite
from adqvest_robotstxt import Robots
robot = Robots(__file__)
from playwright.async_api import async_playwright
import time
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import warnings
warnings.filterwarnings('ignore')
import asyncio


async def start_again_city(city):
    time.sleep(5)
    await page.locator('span >> text="Pick Up/Dine-in"').first.click()
    for i in range(0,3):
        await page.get_by_placeholder("Select City").click()
    time.sleep(1)
    await page.get_by_text(city, exact=True).click()

    
async def data_collection(today):
    final_address = []
    pincode = []
    states = []
    city_list = []
    retries = 0
    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False)
    page = await browser.new_page()
    url = "https://pizzaonline.dominos.co.in/menu"
    await page.goto(url,timeout = 90000)
    robot.add_link(url)
    time.sleep(65)
    await page.locator('span >> text="Pick Up/Dine-in"').click()
    #await page.locator('input >> placeholder="Select City"').click()
    for i in range(0,3):
        await page.get_by_placeholder("Select City").dblclick()


    html = await page.content()
    soup = BeautifulSoup(html)

    cities = [i.text for i in  soup.findAll('span', attrs = {'class' : 'lst-desc-main ellipsis'})]


    for city in cities[3:]:
        store_num = 0
        print(f"In city: {city}")
        await page.locator('span >> text="Delivery"').first.click()
        time.sleep(2)
        await page.locator('span >> text="Pick Up/Dine-in"').first.click()
        time.sleep(2)
        for i in range(0,3):
                try:
                    await page.get_by_placeholder("Select City").dblclick()
                except:
                    continue
        time.sleep(2)
        await page.get_by_text(city, exact=True).click()
        time.sleep(2)
        await page.get_by_placeholder("Select Store").click()
        time.sleep(2)

        html = await page.content()
        soup = BeautifulSoup(html)
        stores = [x.text for x in  soup.findAll('span', attrs = {'class' : 'lst-desc-main ellipsis'})]

        for store in stores:
            store_num+= 1
            if(store_num%10 == 0):
                time.sleep(30)

            print("\t Store: ",store)

            for i in range(0,3):
                try:
                    await page.get_by_placeholder("Select City").dblclick()
                except:
                    pass

            time.sleep(1)
            
            try:
                await page.get_by_text(city, exact=True).click()
            except:
                await page.locator('span >> text="Pick Up/Dine-in"').first.click()
                await page.get_by_placeholder("Select City").click()
                time.sleep(1)
                await page.get_by_text(city, exact=True).click()
            
            time.sleep(2)
                
            for i in range(0,3):
                try:
                    await page.get_by_placeholder("Select Store").dblclick()
                except:
                    pass
            
            try:
                await page.get_by_text(store, exact=True).click()
            except:
                await page.locator('span >> text="Pick Up/Dine-in"').first.click()
                await page.get_by_placeholder("Select City").click()
                try:
                    await page.get_by_text(city, exact=True).click()
                except:
                    await page.locator('span >> text="Pick Up/Dine-in"').first.click()
                    await page.get_by_placeholder("Select City").click()
                    time.sleep(1)
                    await page.get_by_text(city, exact=True).click()
                    await page.get_by_placeholder("Select Store").click()
                    time.sleep(2)
                    await page.get_by_text(store, exact=True).click()

            time.sleep(1)
            html = await page.content()
            soup = BeautifulSoup(html)
            if soup.find('p', attrs = {'class' : 'str--dtl str-adrs'}) is None:
                for i in range(0,3):
                    try:
                        await page.get_by_placeholder("Select City").click(force=True)
                    except:
                        await page.locator('span >> text="Pick Up/Dine-in"').first.click()
                        await page.get_by_placeholder("Select City").click()
                        time.sleep(1)
                        await page.get_by_text(city, exact=True).click()
                        time.sleep(2)
                await page.get_by_placeholder("Select Store").click()
                time.sleep(2)
                for i in range(0,3):
                    try:
                        await page.get_by_text(store, exact=True).click()
                    except:
                        pass
                
                html = await page.content()
                soup = BeautifulSoup(html)
                
            curr_address = soup.find('p', attrs = {'class' : 'str--dtl str-adrs'}).text
            print(curr_address)

            final_address.append(curr_address)
            try:
                curr_pin = re.findall(r'\b[0-9]{6}\b',curr_address)[0].strip()
            except:
                curr_pin = None
            pincode.append(curr_pin)
            curr_state = state_rewrite.state(city)
            curr_state = re.sub("\d+", " ", curr_state).replace("|","").title().strip()
            states.append(curr_state)
            city_list.append(city)
            print(curr_state,curr_pin)

    dominos = pd.DataFrame(np.column_stack([final_address, city_list, states,pincode]), columns=['Address', 'City','Pincode'])
    dominos['Brand'] = "Domino's Pizza"
    dominos['Country'] = 'India'
    dominos["Relevant_Date"] = today.date()
    dominos["Runtime"] = datetime.datetime.now()

    await browser.close()


    return dominos


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    #scheduler = '5_AM_WINDOWS_SERVER_SCHEDULER'
    scheduler = ''
    no_of_ping = 0

    try:
        #if(run_by=='Adqvest_Bot'):
           # log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
       # else:
           # log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand like '%%Domino%%'",engine)
        last_rel_date = last_rel_date["Max"][0]
        print("Last Data Updation Date : ",last_rel_date)
        
        if(today.date()-last_rel_date >= datetime.timedelta(0)):

            loop = asyncio.get_event_loop()
            dominos = loop.run_until_complete(data_collection(today))

            print(dominos)

            dominos.to_sql(name = "STORE_LOCATOR_WEEKLY_CLEAN_DATA_Temp_Nidhi",index = False,con = engine,if_exists = "append")

            #client = ClickHouse_db.db_conn()
            #click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand like '%Domino%'")
            #click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            #query = "select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where lowerUTF8(Brand) like '%Domino%'and Relevant_Date > '"+click_max_date+"'"
            #df = pd.read_sql(query,engine)
            #client.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())

            
        #log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        #log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')







