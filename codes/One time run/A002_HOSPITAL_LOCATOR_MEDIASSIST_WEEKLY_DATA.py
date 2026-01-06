# -*- coding: utf-8 -*-
"""
Created on Sun Jan 21 18:42:29 2024

@author: GOKUL
"""
import requests
import json
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
import datetime
import time
import re
import nest_asyncio
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import asyncio
from playwright.async_api import async_playwright

async def collect_initial_data():
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)
    
    engine = adqvest_db.db_conn()
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False)
    context = await browser.new_context()
    page = await context.new_page()

    url = "https://m.mediassist.in/Hospital-ios.aspx"
    await page.goto(url, timeout=120000)
    await page.wait_for_timeout(1000)
    html = await page.content()
    soup = BeautifulSoup(html, 'lxml')
    
    icomps = [i.text for i in soup.find('select', attrs={'id': 'insuranceComp'}).findAll('option')]
    data = [{'Companies': icomp, 'Status': None, 'Relevant_Date': today.date(), 'Runtime': today} for icomp in icomps[1:]]
        
    df = pd.DataFrame(data)
    df.to_sql(name = "MEDIASSIST_HOSPITAL_LOCATOR_WEEKLY_STATUS_DATA", if_exists="replace", index=False, con=engine)
    print('Initial Data Pushed to SQL')  

    await browser.close()

def data_collect(company_chunk,ma,relevant_date):
    india_time = timezone('Asia/Kolkata')
    today = datetime.datetime.now(india_time)
    print("I'm inside the function")
    
    Company = []
    Insurance_Company = []
    Hospital_Name = []
    Hospital_Address = []
    City = []
    Pincode = []
    State = []
    Relevant_Date = []
    Runtime = []
    
    # Company list
    url_for_icomps = 'https://m.mediassist.in/GetLocations.aspx?insurerName=true&csrfToken='
    r=requests.get(url_for_icomps)
    icomps=json.loads(r.content)
    
    for icomp in icomps:    
        time.sleep(1)
        full_name = icomp.get('fullName', '') 
        if full_name and full_name in company_chunk:
            short_name = icomp['shortName']
            print("--PROCESSING--"+full_name)
            url_for_states = 'https://m.mediassist.in/GetLocations.aspx?Lookup=true&Lookuptype=state&csrfToken='
            time.sleep(2)
            r=requests.get(url_for_states)
            states=json.loads(r.content) 
            
            for state in states:
                print("-->"+ state)
                if ' ' in state:
                    state = state.replace(' ', '%20')
                else:
                    state = state
                time.sleep(1)
                url_for_cities = f"https://m.mediassist.in/GetLocations.aspx?Lookup=true&Lookuptype=district&ParentName={state}&csrfToken="
                time.sleep(1)
                r_cities = requests.get(url_for_cities)
                cities = json.loads(r_cities.content)
    
                for city in cities:
                    if ' ' in city:
                        city = city.replace(' ', '%20')
                    else:
                        city = city
                    time.sleep(1)
                    if ' ' in short_name:
                        short_name = short_name.replace(' ', '%20')
                    else:
                        short_name = short_name
                                     
                    url_for_hosps = f'https://m.mediassist.in/GetLocations.aspx?stateName={state}&cityName={city}&ppn=false&csrfToken=&insurerName={short_name}'
                    time.sleep(1)
                    r_hosps = requests.get(url_for_hosps)
                    hosps = json.loads(r_hosps.content)
    
                    for i in hosps:
                        Company.append('Medi Assist')
                        Insurance_Company.append(full_name)
                        
                        match = re.match(r'^([^,]+)', i['hospitalName'])
                        hospital_name = match.group(1).strip() if match else ''
                        Hospital_Name.append(hospital_name)
                        
                        match_add = re.search(r',\s*(.*)',i['hospitalName'])
                        hospital_add = match_add.group(1).strip() if match_add else ''
                        Hospital_Address.append(hospital_add)
                        
                        if '%20' in city:
                            city = city.replace('%20', ' ')
                        City.append(city.title().strip())
                        Pincode.append(i['pincode'])
                        
                        if '%20' in state:
                            state = state.replace('%20', ' ')
                        State.append(state.title().strip())
                        Relevant_Date.append(relevant_date)
                        Runtime.append(today)
                        
            ma = pd.DataFrame({'Company':Company,'Insurance_Company':Insurance_Company,'Hospital_Name':Hospital_Name,
                                'Hospital_Address':Hospital_Address, 'City':City, 'Pincode':Pincode,
                                'State':State,'Relevant_Date':Relevant_Date, 'Runtime': Runtime})
    return ma

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    engine = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HOSPITAL_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ' '
    no_of_ping = 0
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        # last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from HOSPITAL_LOCATOR_WEEKLY_DATA where Company = 'Medi Assist'",engine)
        # last_rel_date = last_rel_date["Max"][0]
        # print("Last Data Updation Date : ",last_rel_date)
        
        # if (today.date()-last_rel_date >= datetime.timedelta(7)):
        connection = engine.connect()

        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max From MEDIASSIST_HOSPITAL_LOCATOR_WEEKLY_STATUS_DATA ",engine)
        last_rel_date = last_rel_date["Max"][0]
        print("Last Data Updation Date For Company Status Table : ",last_rel_date)
        
        if (today.date()-last_rel_date >= datetime.timedelta(10)):
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            m = loop.run_until_complete(collect_initial_data())
        else:
            print("Company data already present in the Status Table")  

        query = "SELECT DISTINCT Companies AS Companies FROM MEDIASSIST_HOSPITAL_LOCATOR_WEEKLY_STATUS_DATA WHERE Status is Null;"
        results = pd.read_sql(query,engine)
        companies_to_collect = results['Companies'].tolist()
        limit = 2
        company_chunk = companies_to_collect[:limit]
        print(company_chunk)

        date_query = "SELECT Relevant_Date FROM MEDIASSIST_HOSPITAL_LOCATOR_WEEKLY_STATUS_DATA WHERE Status IS Null"
        date_results = pd.read_sql(date_query, engine)
        relevant_date = date_results['Relevant_Date'].iloc[0]

        ma = pd.DataFrame(columns = ['Company','Insurance_Company','Hospital_Name','Hospital_Address','City','Pincode','State']) 
        ma_final = data_collect(company_chunk,ma,relevant_date) 

        ma_final = ma_final.drop_duplicates(subset='Hospital_Address', keep='last')
        ma_final.to_sql(name = "HOSPITAL_LOCATOR_WEEKLY_DATA", if_exists="append", index=False, con=engine)
        india_time = timezone('Asia/Kolkata')
        today = datetime.datetime.now(india_time)

        for full_name in company_chunk:
             update_query = f'UPDATE MEDIASSIST_HOSPITAL_LOCATOR_WEEKLY_STATUS_DATA SET Status = "Yes" , Runtime = "{today}" WHERE Companies = "{full_name}";'
             connection.execute(update_query)
             print("Table has been updated for - ",full_name)  # Updates the status for all the collected companies 

        log.job_end_log(table_name,job_start_time,no_of_ping) 
            
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual') 

    