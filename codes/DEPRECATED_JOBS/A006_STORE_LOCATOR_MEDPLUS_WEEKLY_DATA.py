from bs4 import BeautifulSoup
import asyncio
from playwright.async_api import async_playwright
import nest_asyncio
import pandas as pd
import time
import re
import math
import sys
import datetime
from datetime import datetime,timedelta
from pytz import timezone
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log 
from adqvest_robotstxt import Robots
robot = Robots(__file__)
from GetState import find_state,find_district

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/State_Function')
import state_rewrite

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite

def collect_initial_data(today,engine):
    query = "SELECT DISTINCT City FROM AdqvestDB.INDIA_TOP_CITIES_POPULATION_ONE_TIME;"
    df = pd.read_sql(query, engine)
    df['Status'] = None
    df['Relevant_Date'] = today.date()
    df['Runtime'] = today
    df.to_sql(name = "MEDPLUS_STORE_LOCATOR_WEEKLY_STATUS_DATA", if_exists="replace", index=False, con=engine)
    print('Initial Data Pushed to SQL')  

async def data_collect(cities_to_process,today,existing_data,last_date):
        print("Inside the function")
        med = pd.DataFrame(columns = ['Category','Sub_Category_1','Company','Brand','Address','City','State','Pincode','Country','Relevant_Date','Runtime'])
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        url = "https://www.medplusmart.com/storelocator#pharmacy"
        await page.goto(url, timeout=120000)
        robot.add_link(url)
        time.sleep(6)
        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        for i, city in enumerate(cities_to_process, start=1):
            print("City ------>", city)
            time.sleep(3)
            await page.get_by_placeholder("Type your area name / pincode").fill(f'{city}')
            time.sleep(10)
            dropdown_menu = await page.wait_for_selector('#LocalityName')
            if dropdown_menu:
                options = await dropdown_menu.query_selector_all('.dropdown-item') 
                time.sleep(1)
                for option in options:
                    option_p = await option.query_selector('p')
                    time.sleep(1)
                    if option_p:
                        span_element = await option_p.query_selector('span')
                        time.sleep(1)
                        if span_element:
                            span_text = await span_element.inner_text()
                    option_city_parts = span_text.split(',')
                    value_parts_stripped = [part.strip() for part in option_city_parts]
                    if city.lower() in value_parts_stripped[0].lower():
                        time.sleep(2)
                        await option.click()
                        time.sleep(3)
                        await page.keyboard.press('Enter')
                        time.sleep(4)
                        break
                    else:
                        print('Not matched')

            time.sleep(2)
            html = await page.content()
            soup = BeautifulSoup(html, 'lxml')
            state = None
            address = None
            pincode = None

            tabs = soup.find('div', class_='header p-0 mb-0')
            if tabs:
                ul_tabs = tabs.find('ul')
                options = ul_tabs.find_all('li', {'role': 'tab'}) 
                time.sleep(2)
                for option in options:
                    link = option.find('a') 
                    if link: 
                        text = link.text.strip()
                        time.sleep(1)
                        li_element = await page.query_selector(f"li[title='{text}']")
                        time.sleep(1)
                        a_element = await li_element.query_selector('a')
                        time.sleep(1)
                        await a_element.click()
                        time.sleep(5)
                        html = await page.content()
                        soup = BeautifulSoup(html, 'lxml')
                        add_div = soup.find('div',class_='address-container near-by-store-info py-2')
                        if add_div:
                            addrs_divs = add_div.find_all('address') 
                            for add in addrs_divs:
                                address =  add.find('p', class_='text-capitalize mb-3 text-secondary font-12')
                                address = address.text.strip().replace('(', '').replace(')', '').rstrip(' \t\n\r?.);:!-(')
                                print(address)
                                if address is None or len(address) < 10:
                                    print("Invalid address, skipping...")
                                    continue
                                if not existing_data.empty:    
                                    if address.lower() in existing_data['Address'].str.lower().values:
                                        print(f"Address already exists, Skippping")
                                        continue
                                    else:
                                        print("Address does not exist, proceeding...")
                                pincode_match = re.search(r"(?<!\d)\d{6}(?!\d)|\d{3}\s\d{3}",address)
                                if pincode_match :
                                    pincode = pincode_match.group(0)
                                    print(pincode)     
                                    # city = find_district(pincode,address).title().strip()  
                                    state = find_state(pincode,address).title().strip()  
                                    print(state)
                                    time.sleep(1)
                                else:
                                    pincode = None
                                    try:
                                        city = district_rewrite.district(address)
                                        if city:
                                            city = re.sub(r'^.*\|', '', city).title().strip() 
                                            print('2ND -----',city)
                                        state = state_rewrite.state(address)
                                        if state:
                                            state = re.sub(r'^.*\|', '', state).title().strip() 
                                            print('2ND -----',state) 
                                    except:
                                        print('!! Error in State Function - State not found !!')
                                        state = None      
                                med.loc[len(med)] = ['Healthcare',text,'Medplus Health Services Ltd','Medplus',address,city,state,pincode,'India',last_date,today]
            else:
                print("No information for ",city)                   
                        
            await page.get_by_placeholder("Type your area name / pincode").fill('') 
            time.sleep(1)
            
            if i % 15 == 0:
                await browser.close()
                browser = await pw.chromium.launch(headless=False)
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(url, timeout=120000)

        med = med.drop_duplicates(subset='Address', keep='first')

        await browser.close()
        return med

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    
    job_start_time = datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ' '
    no_of_ping = 0
    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from MEDPLUS_STORE_LOCATOR_WEEKLY_STATUS_DATA",engine)
        last_rel_date = last_rel_date["Max"][0]
        print("Last Data Updation Date for Status Table: ",last_rel_date)

        if (today.date()-last_rel_date >= timedelta(8)):
            collect_initial_data(today,engine)
        else:
            print('City status exists')    

        query = "SELECT DISTINCT City AS Cities FROM MEDPLUS_STORE_LOCATOR_WEEKLY_STATUS_DATA WHERE Status IS Null;"
        results = pd.read_sql(query,engine)
        companies_to_collect = results['Cities'].tolist()
        cities_to_process = companies_to_collect[:51]

        last_date = pd.read_sql("select max(Relevant_Date) as Max from MEDPLUS_STORE_LOCATOR_WEEKLY_STATUS_DATA",engine)
        last_date = last_date["Max"][0]
        
        existing_data_query = f" SELECT DISTINCT Address FROM MEDPLUS_STORE_LOCATOR_WEEKLY_DATA Where Brand = 'Medplus' and Relevant_Date = '{last_date}';"
        existing_data = pd.read_sql(existing_data_query, engine) 
        print(len(existing_data))

        if len(cities_to_process) == 0:  
            total_data_query = f" SELECT * FROM MEDPLUS_STORE_LOCATOR_WEEKLY_DATA Where Relevant_Date = '{last_date}';"
            total_data = pd.read_sql(total_data_query, engine) 
            print(len(total_data))

            max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Medplus'",engine)
            max_rel_date = max_rel_date["Max"][0]
            print("Last Date for Table: ",max_rel_date)

            if last_date > max_rel_date:
                total_data.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA", if_exists="append", index=False, con=engine)
                print('Pushed to Final Table')

                client1 = ClickHouse_db.db_conn()
                click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.MEDPLUS_STORE_LOCATOR_WEEKLY_DATA")
                click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
                query = 'select * from AdqvestDB.MEDPLUS_STORE_LOCATOR_WEEKLY_DATA where Relevant_Date = "' + click_max_date + '"'
                df = pd.read_sql(query, engine)
                client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
                print(f'To CH: {len(df)} rows') 
        else:
            print('Need to wait') 
        
        if len(cities_to_process) != 0:
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            med_final = loop.run_until_complete(data_collect(cities_to_process,today,existing_data,last_date)) 

            med_final = med_final.drop_duplicates(subset='Address', keep='last')
            med_final.to_sql(name = "MEDPLUS_STORE_LOCATOR_WEEKLY_DATA", if_exists="append", index=False, con=engine)
            print(f'To SQL: {len(med_final)} rows')  
            print('Data collection completed.')
            
            for city in cities_to_process:
                connection = engine.connect()
                update_query = f'UPDATE MEDPLUS_STORE_LOCATOR_WEEKLY_STATUS_DATA SET Status = "Done" , Runtime = "{today}" WHERE City = "{city}";'
                connection.execute(update_query)
                print("Table has been updated for - ",city)  # Update the status for all the collected cities
                connection.close()  

            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Runtime) from AdqvestDB.MEDPLUS_STORE_LOCATOR_WEEKLY_DATA ")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.MEDPLUS_STORE_LOCATOR_WEEKLY_DATA where Runtime > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute("INSERT INTO AdqvestDB.MEDPLUS_STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')  
        else:
            print("City needs to get collected")         
                        
        log.job_end_log(table_name,job_start_time,no_of_ping) 
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')  