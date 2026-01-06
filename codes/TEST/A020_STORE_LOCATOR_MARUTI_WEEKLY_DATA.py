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
    dealer_list = []
    dealer_type = []
    retries = 0
    pw = await async_playwright().start()
    browser = await pw.webkit.launch(headless = False)
    page = await browser.new_page()

    await page.goto("https://www.marutisuzuki.com/dealer-showrooms",timeout = 150000)

    for u in range(0,1):
        try:
            await page.locator('xpath=//select[@id="select-dealer-locator"]').click()
        except:
            pass

    html = await page.content()
    soup = BeautifulSoup(html)
    dealers = [i.text for i in  soup.find('select', attrs = {'id' : 'select-dealer-locator'}).findAll('option')]
    print(dealers)

    for dealer in dealers:

        await page.locator('xpath=//select[@id="select-dealer-locator"]').select_option(dealer)
        print(dealer)

        time.sleep(2)
        for v in range(0,1):
            try:
                await page.locator('xpath=//select[@id="dealer-state"]').click()
            except:
                pass

        time.sleep(2)
        html = await page.content()
        soup = BeautifulSoup(html)
        states = [j.text for j in  soup.find('select', attrs = {'id' : 'dealer-state'}).findAll('option')]
        
        for state in states:
            if "select" not in state.lower():

                await page.locator('xpath=//select[@id="dealer-state"]').select_option(state)

                time.sleep(2)

                for y in range(0,3):
                    try:
                        await page.locator('xpath=//select[@id="dealercity-dealer"]').click()
                    except:
                        pass

                time.sleep(2)
                html = await page.content()
                soup = BeautifulSoup(html)
                cities = [k.text for k in  soup.find('select', attrs = {'id' : 'dealercity-dealer'}).findAll('option')]
            
                for city in cities:
                    flag = 0
                    attempt = 0
                    if "select" not in city.lower():
                        dealer_name = None
                        curr_address = None
                        print(dealer+"-->"+state+"-->"+city)

                        try:
                            await page.locator(f'xpath=//select[@id="dealercity-dealer"]').select_option(city)
                        except:
                            try:
                                page.reload()
                                await page.locator('xpath=//select[@id="dealer-state"]').select_option(state)
                                await page.locator(f'xpath=//select[@id="dealercity-dealer"]').select_option(city)
                            except:
                                pass


                        while flag == 0 and attempt <= 5:
                            print(attempt)
                            for u in range(0,2):
                                try:
                                    await page.locator(f'xpath=//form/a[@id="dealer-search"]').dispatch_event('click')
                                except:
                                    pass
                            time.sleep(10)
                            html = await page.content()
                            soup = BeautifulSoup(html)
                            try:
                                mentioned_city = soup.find('div',attrs = {'class':'delaer-locator-content'}).find('span').text
                            except:
                                page.reload()
                                time.sleep(30)
                                await page.locator('xpath=//select[@id="select-dealer-locator"]').select_option(dealer)
                                await page.locator('xpath=//select[@id="dealer-state"]').select_option(state)
                                await page.locator(f'xpath=//select[@id="dealercity-dealer"]').select_option(city)
                                await page.locator(f'xpath=//form/a[@id="dealer-search"]').dispatch_event('click')
                                time.sleep(10)
                                html = await page.content()
                                soup = BeautifulSoup(html)
                                mentioned_city = soup.find('div',attrs = {'class':'delaer-locator-content'}).find('span').text
                            print(city.lower(),mentioned_city.lower())
                            if city.lower() in mentioned_city.lower():
                                flag = 1 
                                time.sleep(5)
                                address = soup.find('ul',attrs = {'class':'delaer-locator-list'})
                                add = address.find_all('li')
                                for i in add:
                                    dealer_name = i.find('h6').text
                                    curr_address = i.find('div',attrs={'class':'address'}).text
                                    print(dealer_name)
                                    dealer_type.append(dealer)
                                    dealer_list.append(dealer_name)
                                    final_address.append(curr_address)
                                    maruti = pd.DataFrame(np.column_stack([final_address, dealer_list, dealer_type]), columns=['Address', 'City','Pincode'])
                                    maruti['Brand'] = "Maruti"
                                    maruti['Country'] = 'India'
                                    maruti["Relevant_Date"] = today.date()
                                    maruti["Runtime"] = datetime.datetime.now()
                            else:
                                page.reload()
                                time.sleep(60)
                                await page.locator('xpath=//select[@id="select-dealer-locator"]').select_option(dealer)
                                await page.locator('xpath=//select[@id="dealer-state"]').select_option(state)
                                await page.locator(f'xpath=//select[@id="dealercity-dealer"]').select_option(city)
                                await page.locator(f'xpath=//form/a[@id="dealer-search"]').dispatch_event('click')
                                time.sleep(10)
                                html = await page.content()
                                soup = BeautifulSoup(html)
                                mentioned_city = soup.find('div',attrs = {'class':'delaer-locator-content'}).find('span').text
                                if city.lower() in mentioned_city.lower():
                                    address = soup.find('ul',attrs = {'class':'delaer-locator-list'})
                                    add = address.find_all('li')
                                    for i in add:
                                        dealer_name = i.find('h6').text
                                        curr_address = i.find('div',attrs={'class':'address'}).text
                                        print(dealer_name)
                                        dealer_type.append(dealer)
                                        dealer_list.append(dealer_name)
                                        final_address.append(curr_address)
                                        maruti = pd.DataFrame(np.column_stack([final_address, dealer_list, dealer_type]), columns=['Address', 'City','Pincode'])
                                        maruti['Brand'] = "Maruti"
                                        maruti['Country'] = 'India'
                                        maruti["Relevant_Date"] = today.date()
                                        maruti["Runtime"] = datetime.datetime.now()
                                else:
                                    pass


                            attempt += 1 
                        
    await browser.close()


    return maruti


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
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

    #try:
        #if(run_by=='Adqvest_Bot'):
           # log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
       # else:
           # log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        #last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand like '%%Domino%%'",engine)
        #last_rel_date = last_rel_date["Max"][0]
        #print("Last Data Updation Date : ",last_rel_date)
        
        #if(today.date()-last_rel_date >= datetime.timedelta(0)):

    loop = asyncio.get_event_loop()
    maruti = loop.run_until_complete(data_collection(today))

    print(maruti)

    #maruti.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA_Temp_Nidhi",index = False,con = engine,if_exists = "append")

            #client = ClickHouse_db.db_conn()
            #click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand like '%Domino%'")
            #click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            #query = "select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where lowerUTF8(Brand) like '%Domino%'and Relevant_Date > '"+click_max_date+"'"
            #df = pd.read_sql(query,engine)
            #client.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES",df.values.tolist())

            
        #log.job_end_log(table_name,job_start_time,no_of_ping)

    #except:
        #error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        #error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        #print(error_msg)
        #log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')







