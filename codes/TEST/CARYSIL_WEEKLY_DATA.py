import datetime
import os
import re
import sys
import warnings

import pandas as pd
import requests
from pytz import timezone

warnings.filterwarnings('ignore')
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)



def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    client1 = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'STORE_LOCATOR_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'Carysil'",engine)
        last_rel_date = last_rel_date["Max"][0]

        if(today.date()-last_rel_date >= datetime.timedelta(7)):

            url = 'https://carysil.com/store-locator/'
            headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
            r = requests.get(url, headers=headers, timeout = 60)
            robot.add_link(url)
            soup = BeautifulSoup(r.content, 'html')
            states = [i.text for i in soup.findAll('select', attrs = {'id' : 'state-dropdown'})[0].findAll('option')][1:]

            state_list = []
            city_list = []
            dealers = []
            address = []
            pincodes = []

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until='networkidle')
                time.sleep(2)
                for st in states:
                    page.select_option('//select[@id="state-dropdown"]', label = st)
                    time.sleep(3)
    
                    soup = BeautifulSoup(page.content())
                    cities = [i.text for i in  soup.findAll('select', attrs = {'id' : 'city-dropdown'})[0].findAll('option')][1:]

                    for city in cities:
                        page.select_option('//select[@id="city-dropdown"]', label=city)
                        #page.select_option('//select[@id="city-dropdown"]', label=city)
                        time.sleep(2)
                        data_soup = BeautifulSoup(page.content())
                        total_add = data_soup.findAll('div', class_='card-body')[0].findAll('p')
                        dealer = total_add[0].text
                        add = total_add[1].text.replace('Address - ', '').replace('\xa0', ' ').strip()
                        add2 = ','.join(i.text for i in total_add)
                        try:
                            pin = re.findall('\d{6}', add2)[0]
                        except:
                            pin = re.findall('\d{3}\s\d{3}', add2)[0].replace(' ', '')
                        print(city.title())
                        state_list.append(st.title())
                        city_list.append(city.title())
                        dealers.append(dealer)
                        address.append(add)
                        pincodes.append(pin)

            df = pd.DataFrame()

            df['State'] = state_list
            df['City'] = city_list
            df['Dealer_Name'] = dealers
            df['Address'] = address
            df['Pincode'] = pincodes
            df['Company'] = 'Acrysil'
            df['Brand'] = 'Carysil'
            df['Relevant_Date'] = today.date()
            df['Runtime'] = datetime.datetime.now(india_time).strftime('%Y-%m-%d %H:%M:%S')
            df = df[~df.duplicated()]

            df=drop_duplicates(df)
            df.to_sql(name="STORE_LOCATOR_WEEKLY_DATA", if_exists="append", con=engine, index=False)
            print(f'To Sql: {len(df)} rows')
            client1 = ClickHouse_db.db_conn()
            click_max_date = client1.execute("select max(Relevant_Date) from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = 'Carysil'")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            query = 'select * from AdqvestDB.STORE_LOCATOR_WEEKLY_DATA where Brand = "Carysil" and Relevant_Date > "' + click_max_date + '"'
            df = pd.read_sql(query, engine)
            client1.execute("INSERT INTO AdqvestDB.STORE_LOCATOR_WEEKLY_DATA VALUES", df.values.tolist())
            print(f'To CH: {len(df)} rows')
        else:
            print("Data already updated")
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if (__name__ == '__main__'):
    run_program(run_by='manual')
