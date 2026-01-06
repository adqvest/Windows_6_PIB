#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 13:07:13 2024

@author: nidhigoel
"""

import easyocr
import cv2
import sys
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
from datetime import datetime
from playwright.async_api import async_playwright
import warnings
import xlwings as xw
import asyncio
warnings.filterwarnings('ignore')
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
engine = adqvest_db.db_conn()
import JobLogNew as log
import dbfunctions
import re
from calendar import monthrange


def status_table(today):
    engine = adqvest_db.db_conn()
    query = "select Company_Name from NSE_500_DAILY_DATA order by Relevant_Date desc limit 500;"
    company_names = pd.read_sql(query,con=engine)
    company_names['Status'] = None
    company_names['Download_Status'] = None
    company_names['Filename'] = None
    company_names['Relevant_Date'] = today.date()
    company_names['Runtime'] = today
    company_names.to_sql('EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA', con=engine, if_exists='append', index=False)

def check_status(today):
    engine = adqvest_db.db_conn()
    query = "select * from EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA where Status is NULL"
    company_names = pd.read_sql(query,con=engine)
    if company_names.empty:
        engine.execute('TRUNCATE TABLE EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA;')
        status_table(today)

def check_company(comp,est_id,today,sl_no):
    query = f"select * from EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA where Company_Name = '{comp}' and Establishment_ID = '{est_id}'"
    company_names = pd.read_sql(query,con=engine)
    if company_names.empty:
        connection = engine.connect()
        today = today.replace(tzinfo=None)
        q = f"INSERT INTO EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA VALUES ('sl_no','{est_id}','{comp}',NULL, NULL, NULL,NULL,NULL,'{today.date()}','{today}')"
        connection.execute(q)

def captcha_solver():
    image = cv2.imread("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Apply thresholding to binarize the image (convert to black and white)
    _, black_and_white_image = cv2.threshold(gray_image, 127, 255, cv2.THRESH_BINARY)

    # Save the black and white image
    cv2.imwrite("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png", black_and_white_image)

    # Use Tesseract to extract text from the image
    image = cv2.imread("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
    reader = easyocr.Reader(['en'])
    result = reader.readtext(image, detail = 0)
    combined_text = ' '.join(result).replace(' ','').strip().upper()
    return combined_text

def end_date(date):
    end_dt = datetime(date.year, date.month, monthrange(date.year, date.month)[1]).date()
    return end_dt

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

async def data_collection(today):

    async def load_results(name):
        await page.locator("xpath=//input[@id = 'estName']").fill(f"{name}")
        await page.locator("xpath=//img[@id = 'capImg']").screenshot(path="C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
        text = captcha_solver()
        if text == '':
            await page.get_by_role("button", name="Reset").click()
            time.sleep(5)
        else:
            
            text = text.replace(' ','').replace('\n','').strip()
            await page.locator("xpath=//input[@id = 'captcha']").fill(f"{text}")
            await page.get_by_role("button", name="Search").click()
            time.sleep(5)


    pw = await async_playwright().start()
    browser = await pw.firefox.launch(headless = False)
    context = await browser.new_context(ignore_https_errors=True,no_viewport=True)
    page = await context.new_page()

    url = "https://unifiedportal-epfo.epfindia.gov.in/publicPortal/no-auth/misReport/home/loadEstSearchHome"
    try:
        await page.goto(url)
    except:
        pass

    # query = "select Company_Name from EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA where Status is Null limit 10;"
    query = "select * from AdqvestDB.EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA Where Sl_No > 0 and Download_Status is null order by Sl_No"
    company_names = pd.read_sql(query,con=engine)

   

    for i,(name,sl_no) in enumerate(zip(company_names['Company_Name'],company_names['Sl_No'])):
        try:
            co_name = name
            print("Company: ",name)
            name = name.replace('.','')
            page = context.pages[0]
            soup = ''
            await load_results(name)
            await page.wait_for_timeout(9000)
            html = await page.content()
            soup = BeautifulSoup(html)
            while('Please enter valid captcha' in soup.find('div',{'id':'tablecontainer'}).text or soup.find('div',{'id':'tablecontainer'}).text == '' or soup == ''):
                await load_results(name)
                html = await page.content()
                soup = BeautifulSoup(html,'lxml')
            await page.wait_for_timeout(9000)
            html = await page.content()
            soup = BeautifulSoup(html,'lxml')
            if('No details found for this criteria. Please enter valid Establishment name or code number .' in soup.text):
                continue  

            else:
                time.sleep(10)
                page = context.pages[-1]
                html = await page.content()
                soup = BeautifulSoup(html) 
                print("Reached here excel download")

                try:
                    async with page.expect_download() as download_info:
                        await page.get_by_text("Excel").click()    

                except:
                    try:
                        time.sleep(5)
                        async with page.expect_download() as download_info:
                            await page.get_by_text("Excel").click()

                    except:
                        try:
                            async with page.expect_download() as download_info:
                                await page.get_by_text("Excel").click()    
                        except: 
                            print("Error in downloading")
                            continue

                download = await download_info.value
                filename=f'{co_name}.xlsx'

                await download.save_as("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/Establishment Files/" +filename)
                print("Downloaded file")
                engine.execute(f'UPDATE EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA SET Download_Status = "Done" where Company_Name = "{co_name}";')
        except:
            i=i+1
            engine.execute(f'UPDATE EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA SET Download_Status = "Failed" where Company_Name = "{co_name}";')     
            continue 



def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'EPFO_ESTABLISHMENTS_PAYMENT_MONTHLY_PIT_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = 'TEST'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        check_status(today)
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
