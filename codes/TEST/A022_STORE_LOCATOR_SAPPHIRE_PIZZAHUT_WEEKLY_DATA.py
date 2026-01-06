import datetime
import json
import os
import re
import sys
import warnings

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pytz import timezone

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions

warnings.filterwarnings('ignore')
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


def lambda_pincode(x):
    try:
        pin = re.findall(r'[0-9]{6}',x)[0]
        return pin
    except:
        try:
            pin = re.findall(r'[0-9]{3} [0-9]{3}',x)[0]
            pin = pin.replace(" ","")
            return pin
        except:
            return None

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
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
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from STORE_LOCATOR_WEEKLY_DATA where Brand = 'KFC' and Company='Sapphire Foods'",engine)
        last_rel_date = last_rel_date["Max"][0]
        if(today.date()-last_rel_date >= datetime.timedelta(7)):

            url="https://www.sapphirefoods.in/store-locator/pizza-hut"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                   'Host': 'www.sapphirefoods.in',
                   'Sec-Fetch-Dest': 'document',
                   'Sec-Fetch-Mode': 'navigate',
                   'Upgrade-Insecure-Requests': '1'}

            r = requests.get(url, headers=headers, verify=True)
            cookie = [k + '=' + v for k, v in r.cookies.get_dict().items()]
            headers['Cookie'] = cookie[0]

            # r = requests.get(url, headers=headers, verify=True)
            # print(r)
            # print(r.content)
            # soup = BeautifulSoup(r.content, 'html')

            
            # data_soup = soup.findAll('select', attrs = {'id':'state'})[0].findAll('option')[1:]

            chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

            download_file_path = r"C:\Users\Administrator\AdQvestDir\BSE REPORTS"

            prefs = {
                "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True
                }
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            options.add_experimental_option('prefs', prefs)
            driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

            driver.maximize_window()

            driver.get(url)

            soup = BeautifulSoup(driver.page_source, 'lxml')

            
            data_soup = soup.findAll('select', attrs = {'id':'state'})[0].findAll('option')[1:]
            driver.close()
        
            
            x=[]
            address_list=[]
            latitude_list=[]
            longitude_list=[]
            pincode_list=[]
            state_list=[]
            city_list=[]
            for i in data_soup:
                for j in json.loads(i['data-salt']):
                    if j != []:
            #             city_id.append(j['id'])
                        payload = {'city_id': j['id'],
                                   'brand':'pizza-hut'}
                        r = requests.post(url, headers = headers, data=payload, verify=False, timeout = 60)
                        print(r)

                        soup2 = BeautifulSoup(r.text)
                        store_data = json.loads(soup2.findAll('input', attrs = {'type': 'hidden', 'id' : 'lat_lang'})[0]['value'])
                        if store_data!=[]:
                            for store_dict in store_data:
                                x.append(store_dict)
                                print(store_dict['store_name'], store_dict['address'], store_dict['lat'],store_dict['lang'])
                #                 name.append(store_dict['store_name'])
                                addr=store_dict['address']
                                pincode=lambda_pincode(addr)
                                pincode_list.append(pincode)
                                address_list.append(store_dict['address'])
                                latitude_list.append(store_dict['lat'])
                                longitude_list.append(store_dict['lang'])
                                city_list.append(j['city_name'])
                                state_list.append(i.text)

            df_final = pd.DataFrame({'State':state_list,
                        'City':city_list,
                        'Longitude':longitude_list,
                        'Latitude':latitude_list,
                        'Address':address_list,
                        'Pincode':pincode_list})
            df_final['Company']='Sapphire Foods'
            df_final['Brand']='PizzaHut'
            df_final['Relevant_Date']=datetime.datetime.now().date()
            df_final['Runtime']=datetime.datetime.now()
            df_final.to_sql(name = "STORE_LOCATOR_WEEKLY_DATA",if_exists="append",index = False,con = engine)
        else:
            print("Data already present")
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
