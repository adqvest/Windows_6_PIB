# -*- coding: utf-8 -*-
"""
Created on Thu Jan 5 14:32:27 2024

@author: GOKUL
"""

from bs4 import BeautifulSoup
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
import pandas as pd
import time
import re
import sys
import datetime
from pytz import timezone
import warnings
import os
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

async def data_collect():
    try:    
        ky_data = []
        print("Inside the function")
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        url = "https://stores.kalyanjewellers.net/"
        await page.goto(url, timeout=180000)
        robot.add_link(url)
        time.sleep(10)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        reject = await page.query_selector('button:has-text("Reject All")')
        if reject:
            await reject.click()
        time.sleep(20)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        time.sleep(20)

        select_element = soup.find('select', id='country')
        if select_element:
            options = select_element.find_all('option')
            couns = [option.text.strip() for option in options if option.text.strip()]
            print(couns)

            # svg_element = await page.wait_for_selector('svg[width="20"]')
            
            for coun in couns[1:2]:
                print("Processing Country:",coun)
                time.sleep(10)
                await page.select_option('#country', value = coun)
                time.sleep(5)
                html = await page.content()
                soup = BeautifulSoup(html, 'lxml')
                time.sleep(3)
                
                try:
                    select_element = soup.find('select', id='state')
                    time.sleep(4)
                    options = select_element.find_all('option')
                    states = [option.text.strip() for option in options if option.text.strip()]
                    print(states)
                except:
                    await page.reload()
                    time.sleep(10)
                    await page.select_option('#country', value = coun)
                    time.sleep(5)
                    html = await page.content()
                    soup = BeautifulSoup(html, 'lxml')   
                    time.sleep(3)
                    select_element = soup.find('select', id='state')
                    print(select_element)
                    time.sleep(4)
                    options = select_element.find_all('option')
                    states = [option.text.strip() for option in options if option.text.strip()]
                    print(states) 

                for state in states[1:]:
                    print("Processing State:--->", state) 
                    time.sleep(5)
                    await page.select_option('#state', value = state)
                    time.sleep(5)
                    html = await page.content()
                    soup = BeautifulSoup(html, 'lxml')
                    time.sleep(5)
                    
                    citys = [i.text.replace('\n', '').strip() for i in soup.find('select', id='city').findAll('option')]
                    
                    for city in citys[1:]:
                        print("City:", city) 
                        time.sleep(5)
                        try:
                            await page.select_option('#city', value = city)
                        except:
                            await page.reload()
                            time.sleep(5)
                            await page.select_option('#country', value = coun)
                            time.sleep(5)
                            await page.select_option('#state', value = state)
                            time.sleep(5)
                            await page.select_option('#city', value = city)
                        
                        time.sleep(5)
                        element = await page.query_selector('path[d="M7 17L17 7M7 7L17 17L7 7Z"]')
                        if element:
                            await element.click()
                        else:
                            print("Element not found.")  

                        time.sleep(5)
                        html = await page.content()
                        soup = BeautifulSoup(html, 'lxml')
                        time.sleep(5)
                        
                        stores = soup.findAll('div',attrs = {'class':'single-shop-card-upper-part'})
                        print(len(stores))
                        if stores:
                            time.sleep(1)
                            for store in stores:
                                print('------------------------------')
                                ti = store.find('h2', class_='single-shop-card-title').text
                                ti = ti.replace('\n','').strip()
                                address = store.find('span', itemprop='streetAddress').text
                                address = address.replace('\n','').strip()
                                
                                full_address = ti + ', ' + address
                                full_address = full_address.title()
                                
                                pincode_element = soup.find('span', itemprop='postalCode')
                                if pincode_element:
                                    pincode_text = pincode_element.text.strip()
                                    pincode = re.sub(r'\D', '', pincode_text)
                                
                                ky_data.append({'Address':full_address.strip(),'State':state.title(),'City':city.title(),'Country':coun.title(),'Pincode':pincode,'Company':'Kalyan Jewellers India Limited','Brand': 'Kalyan Jewellers'})
                
                            await browser.close()
                            browser = await pw.chromium.launch(headless=False)
                            context = await browser.new_context()
                            page = await context.new_page()
                            await page.goto(url, timeout=240000)
                            time.sleep(8) 
                            html = await page.content()
                            soup = BeautifulSoup(html, 'lxml')
                            time.sleep(8) 
                            await page.select_option('#country', value = coun)
                            time.sleep(8)
                            await page.select_option('#state', value = state)
                            time.sleep(8)
                            
                    await browser.close()
                    browser = await pw.chromium.launch(headless=False)
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(url, timeout=120000)
                    time.sleep(5) 
                    html = await page.content()
                    soup = BeautifulSoup(html, 'lxml')
                    time.sleep(8) 
                    await page.select_option('#country', value = coun)
                    time.sleep(8)

                await page.reload()  
                time.sleep(3)     
                                
        kj = pd.DataFrame(ky_data)                       
        await browser.close()
        return kj
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
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
    scheduler = '4_PM_WINDOWS_SERVER_2_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Kalyan Jewellers'",engine)
        last_rel_date = last_rel_date["Max"][0]
        print("Last Data Updation Date : ",last_rel_date)
        
        if today.date()-last_rel_date >= datetime.timedelta(7):
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            kj_final = loop.run_until_complete(data_collect())            
            kj_final['Relevant_Date'] = today.date()
            kj_final['Runtime'] = today
            kj_final = kj_final.drop_duplicates(subset='Address', keep='last') 
            kj_final.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",index = False,con = engine,if_exists = "append")
            print("Data uploaded to SQL")

            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Kalyan Jewellers'")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Kalyan Jewellers" and Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')
            print("Data uploaded to SQL & Clickhouse")

        else:
            print("Data already present")
        
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
    
if(__name__=='__main__'):
    run_program(run_by='manual')              