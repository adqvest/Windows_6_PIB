#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  4 17:38:18 2024

@author: nidhigoel
"""

import sys
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import datetime
from datetime import datetime,timedelta
import re
from playwright.async_api import async_playwright
import time
import warnings
import asyncio
import nest_asyncio
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
   
async def data_collection(today):
    
    zomato = pd.DataFrame(columns = ['Country','City','Area','Area_Clean','Count','Relevant_Week','Relevant_Date','Runtime'])
    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False)
    context = await browser.new_context(ignore_https_errors=True,no_viewport=True)
    page = await context.new_page()    

    url = "https://www.zomato.com/delivery-cities"
    await page.goto(url,timeout = 150000)
    robot.add_link(url)
    time.sleep(1)
    html = await page.content()
    soup = BeautifulSoup(html)
    
    cities = [i.text for i in soup.find('h1',string='Cities We Deliver To').next_sibling.findAll('a')]
    
    for city in cities:
        await page.get_by_text(f"{city}",exact=True).click()
        time.sleep(3)
        see_more = page.get_by_text("see more",exact=True)
        if await see_more.count() > 0:
            await see_more.click()
        time.sleep(2)
        html = await page.content()
        soup = BeautifulSoup(html)
        title_card = page.get_by_text(f"Popular localities in and around {city}")
        if await title_card.count() == 0:
            await page.get_by_text('Sitemap').hover()
            area_name = city
            area_clean = area_name.split("(")[0].strip()
            area_count = int(len(soup.findAll('img',attrs={'alt':'Restaurant Card'})))
            rel_week = today.date().strftime("%V")
            rel_week = "Week "+ rel_week +"-"+today.date().strftime("%Y")
            zomato.loc[len(zomato)] = ['India',city,area_name,area_clean,area_count,rel_week,today.date(),today]
            zomato = zomato.drop_duplicates(subset=['Country','City','Area','Area_Clean','Count','Relevant_Week'], keep='first')
            await page.goto(url,timeout = 150000)
            continue
        
        try:
            areas = [i for i in soup.find('div',class_='title').next_sibling.findAll('div')]
        except:
            see_more = page.get_by_text("see more",exact=True)
            if await see_more.count() > 0:
                await see_more.click()
            time.sleep(2)
            html = await page.content()
            soup = BeautifulSoup(html)
            areas = [i for i in soup.find('div',class_='title').next_sibling.findAll('div')]
        
        for area in areas:
            if str(type(area.find('h5'))) == "<class 'NoneType'>":
                continue
            area_name = area.find('h5').text
            try:
                area_clean = area_name.split("(")[0].strip()
            except:
                area_clean = area_name
            area_count = int(re.findall('\d+', area.find('p').text )[0])
            rel_week = today.date().strftime("%V")
            rel_week = "Week "+ rel_week +"-"+today.date().strftime("%Y")
            zomato.loc[len(zomato)] = ['India',city,area_name,area_clean,area_count,rel_week,today.date(),today]
            zomato = zomato.drop_duplicates(subset=['Country','City','Area','Area_Clean','Count','Relevant_Week'], keep='first')
        await page.goto(url,timeout = 150000)
    zomato.reset_index(drop=True,inplace=True)
    return zomato
            
    
def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    
    engine = adqvest_db.db_conn()
    
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    
    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '12_PM_WINDOWS_SERVER_2_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        Latest_Date= pd.read_sql('select max(Relevant_Date) as Relevant_Date from ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA WHERE Country IN ("India")',engine)
        Latest_Date=Latest_Date['Relevant_Date'][0]
        print("Last Data Updation Date : ",Latest_Date)
          
        if (today.date() -  Latest_Date >= timedelta(1)):
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            zomato = loop.run_until_complete(data_collection(today))
            
            zomato.to_sql('ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA',index = False ,con =engine,if_exists = "append")
            print("Data uploaded to SQL") 
            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA WHERE Country IN ('India')")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA where Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute("INSERT INTO AdqvestDB.ZOMATO_ALL_CITIES_RESTAURANT_COUNT_WEEKLY_DATA VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')
            print("Data uploaded to SQL & Clickhouse")

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')