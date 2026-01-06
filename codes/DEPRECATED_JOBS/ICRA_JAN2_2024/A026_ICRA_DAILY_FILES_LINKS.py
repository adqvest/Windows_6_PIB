#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 11:00:29 2023

@author: nidhigoel
"""
import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from bs4 import BeautifulSoup
from pytz import timezone
import sys
import time
import datetime
from datetime import datetime, timedelta
import re
import asyncio
import calendar
from playwright.async_api import async_playwright
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import JobLogNew as log
import MySql_To_Clickhouse as MySql_CH
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def rename_file(file_name):
    files = os.listdir('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\')
    # Check if the file with the expected name is present
    if "FileName" in files[0]:
        print('RENAMING FILE')
        # Rename the file
        original_path = os.path.join('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\', "FileName.pdf")
        new_path = os.path.join('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\', f"{file_name}")
        os.rename(original_path, new_path)

async def data_collection(today):
    
    days       = timedelta(1)
    yesterday = today - days

    ## job log details
    engine = adqvest_db.db_conn()
    
    query="select max(Relevant_Date) as Max from ICRA_DAILY_FILES_LINKS"
    db_max_date = pd.read_sql(query,engine)["Max"][0]

    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False)
    page = await browser.new_page()
    
    if db_max_date < yesterday.date():

        from_date = db_max_date + timedelta(1)
    
        url = 'https://www.icra.in/Rating/AllRatingRationales'
        await page.goto(url,timeout = 150000)
        # robot.add_link(url)
        await page.locator('xpath=//input[@id="ratingFromDate"]').click()
        await page.locator('xpath=//select[@data-handler="selectYear"]').select_option(f'{from_date.year}')
        await page.locator('xpath=//select[@data-handler="selectMonth"]').select_option(calendar.month_abbr[from_date.month])

        await page.locator('xpath=//input[@id="ratingFromDate"]').click()
        await page.locator(f'xpath=//td[@data-handler="selectDay"]/a[text() = {from_date.day}]').click()

        await page.locator('xpath=//input[@id="ratingToDate"]').click()
        await page.locator('xpath=//select[@data-handler="selectYear"]').select_option(f'{yesterday.year}')
        await page.locator('xpath=//select[@data-handler="selectMonth"]').select_option(calendar.month_abbr[yesterday.month])

        await page.locator('xpath=//input[@id="ratingToDate"]').click()
        await page.locator(f'xpath=//td[@data-handler="selectDay"]/a[text() = {yesterday.day}]').click()
       
        await page.get_by_text('Search', exact=True).click()

        time.sleep(2)
        html = await page.content()
        soup = BeautifulSoup(html,'lxml')
        try:
            last_page = int(soup.find('section',attrs = {'id':'AllRatingRationales'}).find('li',class_ = 'PagedList-skipToLast').find('a')['href'].split('=')[-1])
        except:
            last_page = 1

        for i in range(1,last_page+1):
            time.sleep(2)
            if last_page > 1:
                try:
                    await page.locator("#AllRationalcontentPager").get_by_text(f"{i}").click()
                except:
                    time.sleep(2)
                    await page.locator("#AllRationalcontentPager").get_by_text(f"{i}").click()
            time.sleep(1)
            html = await page.content()
            soup = BeautifulSoup(html,'lxml')
            ratings = soup.find('section',attrs = {'id':'AllRatingRationales'}).find_all('div',class_ = 'cpr_blurb')
            for rating in ratings:
                icra = pd.DataFrame(columns = ['File_Name','Links','Company_Name','Relevant_Date','Runtime'])
                os.chdir('C:\\Users/Administrator\\AdQvestDir\\ICRA_JUNK')
                href = rating.find('a',class_ = 'all_rating_rationale_download')['href']
                time.sleep(2)
                async with page.expect_download() as download_info:
                    # Perform the action that initiates download
                    await page.locator(f'//a[@href = "{href}"]').click()
                download = await download_info.value
                
                # Wait for the download process to complete and save the downloaded file somewhere
                await download.save_as("C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\" + download.suggested_filename)
                
                time.sleep(2)
                file_name = rating.find('p',class_ = 'text_ellipsis').text
                r_date = datetime.strptime(rating.find('div',class_ = 'col-2 date').text.replace('\n','').strip(), '%d %b %Y').date()
                link = 'https://www.icra.in'+rating.find('div',class_ = 'col-6 rep_det_con').find('a')['href']
                company_name = file_name.split(':')[0]
                file_name = company_name.strip().replace(' ','_')+'_' + str(r_date) + '.pdf'
                print(file_name)
                rename_file(file_name)
                #upload to s3 and remove file
                dbfunctions.to_s3bucket(f'C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\{file_name}','ICRA/')
                icra.loc[len(icra)] = [file_name,link,company_name,r_date,today]
                icra.to_sql('ICRA_DAILY_FILES_LINKS', con=engine, if_exists='append', index=False) 
                engine.execute('update ICRA_DAILY_FILES_LINKS set Download_Status = "Yes",Comments=null where File_Name = "'+file_name+'"')
                file_path = f'C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\{file_name}'
                os.remove(file_path)               
        MySql_CH.ch_truncate_and_insert('ICRA_DAILY_FILES_LINKS') 
    else:
        print('Data Upto Date')   

    

def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    
    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'ICRA_DAILY_FILES_LINKS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '1_PM_WINDOWS_SERVER_SCHEDULER'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
          
        loop = asyncio.get_event_loop()
        loop.run_until_complete(data_collection(today))

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')