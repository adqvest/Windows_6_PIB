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
from pytz import timezone
import time
import datetime
import calendar
import re
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
import warnings
warnings.filterwarnings('ignore')
import asyncio

    
async def data_collection(today):
    final_address = []
    states = []
    city_list = []
    retries = 0
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless = False)
    page = await browser.new_page()
    url = "https://pizzaonline.dominos.co.in/menu"
    await page.goto(url,timeout = 90000)
    robot.add_link(url)
    asyncio.sleep(60)
    await page.locator('span >> text="Pick Up/Dine-in"').click()
    for i in range(0,3):
        try:
            await page.get_by_placeholder("Select City").click()
        except:
            pass

    html = await page.content()
    soup = BeautifulSoup(html)

    cities = [i.text for i in  soup.findAll('span', attrs = {'class' : 'lst-desc-main ellipsis'})]


    for city in cities:
        store_num = 0
        print(f"In city: {city}")
        await page.locator('span >> text="Delivery"').first.click()
        await page.locator('span >> text="Pick Up/Dine-in"').first.click()
        for i in range(0,3):
                try:
                    await page.get_by_placeholder("Select City").click()
                except:
                    pass
        await page.get_by_text(city, exact=True).click()
        await page.get_by_placeholder("Select Store").click()

        html = await page.content()
        soup = BeautifulSoup(html)
        stores = [x.text for x in  soup.findAll('span', attrs = {'class' : 'lst-desc-main ellipsis'})]

        for store in stores:

            final_address.append(store)
            try:
                curr_state = state_rewrite.state(city)
                curr_state = re.sub("\d+", " ", curr_state).replace("|","").title().strip()
            except:
                curr_state = None
            states.append(curr_state)
            city_list.append(city)
            print(curr_state)

    dominos = pd.DataFrame(np.column_stack([final_address, city_list, states]), columns=['Address', 'City','State'])
    dominos['Brand'] = "Domino's Pizza"
    dominos['Company'] = "Jubilant FoodWorks Limited"
    dominos['Country'] = 'India'
    dominos["Relevant_Date"] = today.date()
    dominos["Runtime"] = datetime.datetime.now()

    await browser.close()
    asyncio.sleep(1)


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
        if(run_by=='Adqvest_Bot'):
           log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
           log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        # last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand like '%%Domino%%'",engine)
        # last_rel_date = last_rel_date["Max"][0]
        # print("Last Data Updation Date : ",last_rel_date)
        
        # if(today.date()-last_rel_date >= datetime.timedelta(0)):

        loop = asyncio.get_event_loop()
        dominos = loop.run_until_complete(data_collection(today))

        print(dominos)
        asyncio.sleep(1)

            # dominos.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",index = False,con = engine,if_exists = "append")

            # client = ClickHouse_db.db_conn()
            # click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand like '%Domino%'")
            # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            # query = "select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where lowerUTF8(Brand) like '%Domino%'and Relevant_Date > '"+click_max_date+"'"
            # df = pd.read_sql(query,engine)
            # client.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())

            
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')







