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
import ClickHouse_db
import JobLogNew as log
from playwright.async_api import async_playwright
import nest_asyncio
import time
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import warnings
import asyncio
warnings.filterwarnings('ignore')
engine = adqvest_db.db_conn()
from GetState import find_district

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/District_Function')
import district_rewrite
   
async def data_collection(today):
    lakme = pd.DataFrame(columns = ['Category','Company','Brand','Address','City','State','Pincode','Country','Relevant_Date','Runtime'])
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless = False)
    page = await browser.new_page()
    
    url = 'https://www.lakmesalon.in/store-locator'
    await page.goto(url,timeout = 180000)
    time.sleep(3)
    robot.add_link(url)
    html = await page.content()
    soup = BeautifulSoup(html)
    
    states = [i.text for i in soup.find('div', class_='container p-5').find('select', id='state').findAll('option')]
    for state in states[1:]:
        print('Processing State -->',state)
        await page.select_option('#state', value = state)
        time.sleep(2)
        html = await page.content()
        soup = BeautifulSoup(html)

        if 'No Stores Exits for the given the location. Search for other nearest location' in soup.text:
            print(f'No stores in "{state}"')
            continue
         
        addrs = soup.find('div', class_='listing').find('ul').findAll('li')
        for addr in addrs:
            address = addr.find('div', class_='col-9').find('p').text.strip()
            matches = re.findall(r'\d{6}', address)
            pincode = matches[0] if matches else None
            if pincode:
                city = find_district(pincode,address).title().strip() 
                print(city)
            else:
                city = district_rewrite.district(address)
                city = re.sub(r'^.*\|', '', city).title().strip() 
                print(city)
        
            lakme.loc[len(lakme)] = ['FMCG','Hindustan Unilever Ltd','Lakme',address,city,state,pincode,'India',today.date(),today]

    lakme = lakme.drop_duplicates(subset='Address', keep='last')
    lakme.reset_index(drop=True,inplace=True)
     
    await browser.close() 
    return lakme

def run_program(run_by='Adqvest_Bot', py_file_name=None):    
#     #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    
    # job log details
    job_start_time = datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Lakme';",engine)
        last_rel_date = last_rel_date["Max"][0]
        print("Last Data Updation Date : ",last_rel_date)
        
        if today.date()-last_rel_date >= timedelta(7):
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            lakme = loop.run_until_complete(data_collection(today))
                
            lakme.to_sql(table_name,index = False ,con =engine,if_exists = "append")
            print("Data uploaded to SQL") 
            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Lakme'")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Lakme" and Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')

        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')