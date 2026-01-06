"""
Created on Fri Oct 17 2023

@author: Rahul
"""

import sys
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
from datetime import datetime
import warnings
import asyncio

from statistics import mean

warnings.filterwarnings('ignore')
import re
import json
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from playwright.async_api import async_playwright
import time
from adqvest_robotstxt import Robots
robot = Robots(__file__)
engine = adqvest_db.db_conn()




async def world_bank_data(today):
    pw = await async_playwright().start()

    browser = await pw.chromium.launch(headless = False)
    page = await browser.new_page()


    await page.goto("https://databank.worldbank.org/source/health-nutrition-and-population-statistics#", timeout=90000)

    await page.locator('xpath=//*[@data-text="Select All Country"]').dispatch_event('click')

    await page.locator('xpath=//*[@id="panel_HNP_Series"]/div[1]/h4/a').dispatch_event('click')

    await page.locator('xpath=//*[@id="searchBox_HNP_Series"]').fill("immunization")
    await page.keyboard.press('Enter')

    time.sleep(5)
    await page.locator('xpath=//*[@data-text="Select All Series"]').dispatch_event('click')

    await page.locator('xpath=//*[@id="panel_HNP_Time"]/div[1]/h4/a').dispatch_event('click')

    time.sleep(5)

    await page.locator('xpath=//*[@id="chk[HNP_Time].[Year].&[YR2022]"]').dispatch_event('click')

    await page.locator('xpath=//*[@id="applyChangesNoPreview"]').dispatch_event('click')

    time.sleep(5)

    await page.locator('xpath=//span[@class="sprite icon-download"]').dispatch_event('click')

    time.sleep(15)

    # await page.locator('xpath=//*[@id="liExcelDownload"]/a]').dispatch_event('click')

    async with  page.expect_download() as download_info:
        await page.locator('xpath=//*[@id="liExcelDownload"]/a').dispatch_event('click')
        time.sleep(5)
        download = download_info.value
    await download.save_as(download.suggested_filename)

    await browser.close()




def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    # job log details
    job_start_time = datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA;",engine)
        # last_rel_date = last_rel_date["Max"][0]
        # print("Last Data Updation Date : ",last_rel_date)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(world_bank_data(today))




        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
