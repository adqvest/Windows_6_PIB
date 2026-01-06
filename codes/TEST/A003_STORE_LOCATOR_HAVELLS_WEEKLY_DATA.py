# -*- coding: utf-8 -*-
"""
Created on Friday July 21 2023

@author: Nidhi
"""
import sys
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
from datetime import datetime
import re
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from playwright.sync_api import sync_playwright
import time
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import warnings
import asyncio
warnings.filterwarnings('ignore')
engine = adqvest_db.db_conn()

#url_list = ["https://havells.com/en/havells-galaxy.html", 'https://havells.com/en/dealer-locator.html']
url_list = ["https://havells.com/en/havells-galaxy.html"]
def data_collection(today, link):
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless = True)
    page = browser.new_page()
    url = link
    page.goto(url,timeout = 150000)
    robot.add_link(url)
    
    time.sleep(5)
    html = page.content()
    soup = BeautifulSoup(html)
    
    states = [i.text for i in  soup.find('select', attrs = {'name' : 'p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$State$dropDownList'}).findAll('option')[1:]]
    # print(dealers) 
    dealers = []
    deal_addr = []
    pincodes = []
    state_lst = []
    ct_lst = []

    for state in states:
        print(state)
        page.locator('xpath=//select[@name="p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$State$dropDownList"]').select_option(state)
        time.sleep(3)
        html = page.content()
        soup = BeautifulSoup(html)
        cities = [i.text for i in  soup.find('select', attrs = {'name' : 'p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$City$dropDownList'}).findAll('option')][1:]
        for city in cities:
            print(city)
            page.locator('xpath=//select[@name="p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$City$dropDownList"]').select_option(city)
            time.sleep(2)

            v_soup = BeautifulSoup(page.content())
            if v_soup.find('select', attrs = {'name': 'p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$ProductCategory$dropDownList'}):
                products = [i.text for i in v_soup.find('select', attrs = {'name': 'p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$ProductCategory$dropDownList'}).find_all('option')[1:]]
                for prod in products:
                    print(prod)
                    page.locator('xpath=//select[@name="p$lt$ctl03$pageplaceholder$p$lt$ctl00$Form_Galaxy_Locator$plcUp$form$ProductCategory$dropDownList"]').select_option(prod)
                    time.sleep(3)
                    page.locator('//input[@class="submit locateDealer btn btn-lg btn-block btn-blue"]').click()
                    time.sleep(3)
                    html = page.content()
                    soup = BeautifulSoup(html)
                    data = soup.findAll('div', class_= 'store-address')
                    for i in data:
                        dealer = i.findAll('div')[0].text
                        address = i.findAll('div')[1].text.replace('\n', '')
                        addr = re.sub('\s{2,}', ' ', address)
                        pincode = addr.lower().split('code')[-1].replace(': ', '').strip()
                        dealers.append(dealer)
                        deal_addr.append(addr)
                        pincodes.append(pincode)
                        state_lst.append(state)
                        ct_lst.append(city.split('-')[0])
                        time.sleep(2)
            else:
                page.locator('//input[@class="submit locateDealer btn btn-lg btn-block btn-blue"]').click()
                time.sleep(3)
                html = page.content()
                soup = BeautifulSoup(html)
                data = soup.findAll('div', class_= 'store-address')
                for i in data:
                    dealer = i.findAll('div')[0].text
                    address = i.findAll('div')[1].text.replace('\n', '')
                    addr = re.sub('\s{2,}', ' ', address)
                    pincode = addr.lower().split('code')[-1].replace(': ', '').strip()
                    dealers.append(dealer)
                    deal_addr.append(addr)
                    pincodes.append(pincode)
                    state_lst.append(state)
                    ct_lst.append(city.split('-')[0])
                    time.sleep(2)

    havells_df = pd.DataFrame(columns = ['Company','Brand','Address','State','City','Pincode','Country','Relevant_Date','Runtime'])       
    havells_df['Company'] = "Havell's"
    havells_df['Address'] = deal_addr
    havells_df['State'] = state_lst
    havells_df['City'] = ct_lst
    havells_df['Pincode'] = pincodes
    havells_df['Country'] = 'India'
    havells_df['Relevant_Date'] = today.date()
    havells_df['Runtime'] = today
    browser.close() 
    pw.stop()
    return havells_df

def run_program(run_by='Adqvest_Bot', py_file_name=None):    
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)

    ## job log details
    job_start_time = datetime.now(india_time)
    table_name = 'STORE_LOCATOR_havells_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
           log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
           log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        # last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_havells_WEEKLY_DATA;",engine)
        # last_rel_date = last_rel_date["Max"][0]
        # print("Last Data Updation Date : ",last_rel_date)
        df = pd.DataFrame()
        # if last_rel_date is None or (today.date()-last_rel_date >= timedelta(1)):
            # loop = asyncio.get_event_loop()
        for link in url_list:
            havells_df = data_collection(today, link)
            # df = pd.concat([df, temp])

        # havells_df = loop.run_until_complete(data_collection(today))

        havells_df.to_sql(table_name,index = False ,con =engine,if_exists = "replace")
        print("Data uploaded to SQL") 

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(sys.exc_info())
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')