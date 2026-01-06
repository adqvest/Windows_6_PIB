#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:45:34 2023

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

def remove_items_from_list(input_list, forbidden_starts, forbidden_substrings):
    result_list = []
    for item in input_list:
        if any(item.lower().startswith(prefix.lower()) for prefix in forbidden_starts):
            continue  

        if any(substring.lower() in item.lower() for substring in forbidden_substrings):
            continue  
        result_list.append(item)

    return result_list

async def collect_initial_data(): #Modified by Gokul | 21-03-2024
    print('Inside Categories collection')
    india_time = timezone('Asia/Kolkata')
    today = datetime.now(india_time)
    
    engine = adqvest_db.db_conn()
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    url = "https://www.nykaa.com/"
    await page.goto(url,timeout = 150000)
    robot.add_link(url)
    time.sleep(5)
    html = await page.content()
    soup = BeautifulSoup(html)
    
    categories = [i.find('a').text.strip() for i in soup.find('ul',class_ = 'MegaDropdownHeading').findAll('li',class_ = 'MegaDropdownHeadingbox') if 'Pop Ups' not in i.find('a').text ]
    forbidden_starts = []
    forbidden_substrings = ["Lingerie","Accessories"]
    filtered_categories = remove_items_from_list(categories, forbidden_starts, forbidden_substrings)
    categories = filtered_categories
    categories = list(set(categories))
    
    data = [{'Category': category, 'Status': None, 'Relevant_Date': today.date(), 'Runtime': today} for category in categories]
        
    df = pd.DataFrame(data)
    df.to_sql(name = "NYKAA_CATEGORIES_WEEKLY_STATUS_DATA", if_exists="replace", index=False, con=engine)
    print('Initial Data Pushed to SQL')  

    await browser.close()
    return df
   
async def data_collection(today,category_chunk,relevant_date):
    print('Inside data_collection')
    
    nykaa = pd.DataFrame(columns = ['Category','Sub_Category','Brand_Name','Count','Relevant_Date','Runtime'])
    nykaa_disc = pd.DataFrame(columns = ['Category','Sub_Category','Discount_Range','Count','Relevant_Date','Runtime'])
    nykaa_price = pd.DataFrame(columns = ['Category','Sub_Category','Price_Range','Count','Relevant_Date','Runtime'])
    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False)
    context = await browser.new_context(ignore_https_errors=True,no_viewport=True)
    page = await context.new_page()    

    url = "https://www.nykaa.com/"
    await page.goto(url,timeout = 150000)
    robot.add_link(url)
    time.sleep(10)
    html = await page.content()
    soup = BeautifulSoup(html)
    
    for category in category_chunk:
        time.sleep(10)
        await page.mouse.click(0,50)
        await page.locator("#my-menu").get_by_role("link", name=f"{category}",exact=True).hover()
        time.sleep(6)
        html = await page.content()
        soup = BeautifulSoup(html,'lxml')
        time.sleep(5)
        
        sub_categories = [i.find('a').text.strip() for i in soup.find('li',class_='MegaDropdownHeadingbox').parent.find('a',text = f'{category}').parent.find('div', class_= 'MegaDropdowntopWrapper').findAll('div',class_= 'MegaDropdown-ContentHeading') if str(type(i.find('a'))) !=  "<class 'NoneType'>" ]
        sub_categories = list(set(sub_categories))
        # print(sub_categories)
        #sub cats to skip
        forbidden_starts = ["Top", "Trend", "Shop", "New","Brand","Quick","CSMS","Types"]
        forbidden_substrings = ["brand","@"]
        filtered_categories = remove_items_from_list(sub_categories, forbidden_starts, forbidden_substrings)
        sub_categories = filtered_categories
        print(sub_categories)
        time.sleep(3)
        await page.locator("#my-menu").get_by_role("link", name=f"{category}",exact=True).hover()
        
        for sub_category in sub_categories:
            print(category,"-->",sub_category)
            page = context.pages[0]
            await page.reload()
            await page.mouse.click(0, 50)
            time.sleep(8)
            await page.locator("#my-menu").get_by_role("link", name=f"{category}",exact=True).hover()
            time.sleep(8)
            try:
                await page.locator("#my-menu").get_by_role("link", name=f"{sub_category}",exact=True).click()
            except:
                await page.locator("#my-menu").get_by_role("link", name=f"{sub_category}",exact=True).first.click()
            time.sleep(6)
            pages = context.pages
            if len(context.pages) < 2:
                await page.locator("#my-menu").get_by_role("link", name=f"{category}",exact=True).hover()
                time.sleep(7)
                try:
                    await page.locator("#my-menu").get_by_role("link", name=f"{sub_category}",exact=True).click()
                    time.sleep(5)
                except:
                    await page.locator("#my-menu").get_by_role("link", name=f"{sub_category}",exact=True).first.click()
                    time.sleep(5)
            for x in context.pages:
                if x.url != 'https://www.nykaa.com/':
                    continue
                else:
                    await x.close()
            time.sleep(5)
            page = pages[-1]
            time.sleep(6)
            try:
                await page.locator('span:has-text("Brand")').click()
            except:
                for x in context.pages:
                    if x.url != 'https://www.nykaa.com/':
                        continue
                    else:
                        await x.close()
                time.sleep(5)
                page = context.pages[-1]
                time.sleep(5)
                try:
                    try:
                        await page.locator('span:has-text("Brand")').click()
                    except:
                        element = await page.query_selector('span.title')
                        time.sleep(5)
                        await element.click()

                        # await page.wait_for_selector('span.title')
                        # await page.click('span.title:nth-match(1)')
                        # await page.get_by_text("Brand", element="span", exact=True).click()
                except:
                    continue
                
            time.sleep(5)
            html = await page.content()
            soup = BeautifulSoup(html)
        
            brands = [i for i in soup.findAll('div',class_ = 'control-value')]
            for brand in brands:
                time.sleep(4)
                brand_name = brand.find('span',class_ = 'title').text
                time.sleep(4)
                brand_count = int(brand.find('span',class_ = 'count').text)
                time.sleep(4)
                nykaa.loc[len(nykaa)] = [category.replace('  ',' ').replace('&','').replace("'"," ").title(),sub_category.replace('  ',' ').replace('&','and').replace("'"," ").title(),brand_name,brand_count,relevant_date,today]
                nykaa = nykaa.drop_duplicates(subset=['Category','Sub_Category','Brand_Name','Count'], keep='first')
                time.sleep(5)
            try:
                await page.locator('span:has-text("Brand")').click()
            except:
                time.sleep(5)
                element = await page.query_selector('span.title')
                time.sleep(4)
                await element.click()

                # await page.wait_for_selector('span.title')
                # await page.click('span.title:nth-match(1)')
                
            time.sleep(5)
            discount_ele = page.get_by_text("Discount", exact=True)
            if await discount_ele.count() > 0:
                await discount_ele.click()
                time.sleep(4)
                html = await page.content()
                soup = BeautifulSoup(html)
                time.sleep(3)
            
                discounts = [i for i in soup.findAll('div',class_ = 'control-value')]
                
                for discount in discounts:
                    time.sleep(3)
                    discount_name = discount.find('span',class_ = 'title').text
                    if discount_name == 'All discounted products':
                        continue
                    discount_count = int(discount.find('span',class_ = 'count').text)
                    nykaa_disc.loc[len(nykaa_disc)] = [category.replace('  ',' ').replace('&','').replace("'"," ").title(),sub_category.replace('  ',' ').replace('&','and').replace("'"," ").title(),discount_name,discount_count,relevant_date,today]
                    nykaa_disc = nykaa_disc.drop_duplicates(subset=['Category','Sub_Category','Discount_Range','Count'], keep='first')
                    
            if await discount_ele.count() == 0:
                pass
            
            time.sleep(5)
            discount_ele = page.get_by_text("Discount", exact=True)
            if await discount_ele.count() > 0:
                await discount_ele.click()
            if await discount_ele.count() == 0:
                pass
            
            time.sleep(3)
            await page.locator('span:has-text("Price")').click()
            
            time.sleep(4)
            html = await page.content()
            soup = BeautifulSoup(html)
        
            prices = [i for i in soup.findAll('div',class_ = 'control-value')]
            
            for price in prices:
                time.sleep(4)
                price_name = price.find('span',class_ = 'title').text
                price_count = int(price.find('span',class_ = 'count').text)
                nykaa_price.loc[len(nykaa_price)] = [category.replace('&','').replace('  ',' ').replace("'"," ").title(),sub_category.replace('  ',' ').replace('&','and').replace("'"," ").title(),price_name,price_count,relevant_date,today]
                nykaa_price = nykaa_price.drop_duplicates(subset=['Category','Sub_Category','Price_Range','Count'], keep='first')
            
            await page.goto(url,timeout = 150000)
            time.sleep(3)
            for x in context.pages:
                if x.url == 'https://www.nykaa.com/':
                    continue
                else:
                    await x.close()
            page = context.pages[0]

    nykaa.reset_index(drop=True,inplace=True)
    nykaa_disc.reset_index(drop=True,inplace=True)
    nykaa_price.reset_index(drop=True,inplace=True)
    
    await browser.close()
    return nykaa,nykaa_disc,nykaa_price
        
def run_program(run_by='Adqvest_Bot', py_file_name=None):    

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client1 = ClickHouse_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    
    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'NYKAA_CATEGORY_WISE_BRANDS_WEEKLY_DATA,NYKAA_CATEGORY_WISE_DISCOUNTS_WEEKLY_DATA,NYKAA_CATEGORY_WISE_PRICING_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '11_AM_WINDOWS_SERVER2_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        Latest_Date= pd.read_sql('select max(Relevant_Date) as Relevant_Date from NYKAA_CATEGORY_WISE_BRANDS_WEEKLY_DATA',engine)
        Latest_Date=Latest_Date['Relevant_Date'][0]
        print("Last Data Updation Date : ",Latest_Date)

        Latest_status_Date= pd.read_sql('select max(Relevant_Date) as Relevant_Date from NYKAA_CATEGORIES_WEEKLY_STATUS_DATA',engine)
        Latest_status_Date=Latest_status_Date['Relevant_Date'][0]
        print("Last Status Updation Date : ",Latest_status_Date)

        if (today.date() - Latest_status_Date >= timedelta(10)):
            loop = asyncio.get_event_loop()
            ny = loop.run_until_complete(collect_initial_data())
            print(ny)
        else:
            print("Categories data already present in the Status Table")   

        query = "SELECT DISTINCT Category AS Category FROM NYKAA_CATEGORIES_WEEKLY_STATUS_DATA WHERE Status is Null;"
        results = pd.read_sql(query,engine)
        categories_to_collect = results['Category'].tolist()
        limit = 1
        category_chunk = categories_to_collect[:limit]
        print(category_chunk)

        if len(category_chunk)>0:
            date_query = "SELECT Relevant_Date FROM NYKAA_CATEGORIES_WEEKLY_STATUS_DATA WHERE Status IS Null"
            date_results = pd.read_sql(date_query, engine)
            relevant_date = date_results['Relevant_Date'].iloc[0]
            
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            nykaa,nykaa_disc,nykaa_price = loop.run_until_complete(data_collection(today,category_chunk,relevant_date))
            print(nykaa)
            print("-------")
            print(nykaa_disc)
            print("-------")
            print(nykaa_price)

            nykaa.to_sql('NYKAA_CATEGORY_WISE_BRANDS_WEEKLY_DATA',index = False ,con =engine,if_exists = "append")
            nykaa_disc.to_sql('NYKAA_CATEGORY_WISE_DISCOUNTS_WEEKLY_DATA',index = False ,con =engine,if_exists = "append")
            nykaa_price.to_sql('NYKAA_CATEGORY_WISE_PRICING_WEEKLY_DATA',index = False ,con =engine,if_exists = "append")
            print("Data uploaded to SQL") 
            nykaa_tables = ['NYKAA_CATEGORY_WISE_BRANDS_WEEKLY_DATA','NYKAA_CATEGORY_WISE_DISCOUNTS_WEEKLY_DATA','NYKAA_CATEGORY_WISE_PRICING_WEEKLY_DATA']
            for table in nykaa_tables:
                click_max_runtime = client1.execute(f"select max(Runtime) as Runtime from AdqvestDB.{table}")
                click_max_runtime = str([a_tuple[0] for a_tuple in click_max_runtime][0])
                query = f'select * from AdqvestDB.{table} where Runtime > "' + click_max_runtime + '"'
                df = pd.read_sql(query, engine)
                client1.execute(f"INSERT INTO AdqvestDB.{table} VALUES", df.values.tolist())
                print(f'Pushing to {table}: {len(df)} rows')
            print("Data uploaded to SQL & Clickhouse")

            for category in category_chunk:
                update_query = f'UPDATE NYKAA_CATEGORIES_WEEKLY_STATUS_DATA SET Status = "Yes" , Runtime = "{today}" WHERE Category = "{category}";'
                connection.execute(update_query)
                # print("Table has been updated for - ",category)  # Updates the status for all the collected category 
        else:
            print('No new data')    
        log.job_end_log(table_name,job_start_time,no_of_ping) 

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')