"""
Created on 2025-10-30

@author: Varadharajan
"""

import warnings
warnings.filterwarnings('ignore')
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import shutil
import requests
import re
from pytz import timezone
import pandas as pd
import datetime
import os
import calendar
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import dbfunctions
from geoclean import geo_clean
import JobLogNew as log
import adqvest_db
import dbfunctions
import boto3
import ClickHouse_db
import MySql_To_Clickhouse as MySql_CH
from adqvest_robotstxt import Robots
robot = Robots(__file__)
engine = adqvest_db.db_conn()
connection = engine.connect()
import time

client1 = ClickHouse_db.db_conn()

s3_folder='NPPA/PSD_CEILING_PRICES_OF_ESSENTIAL_MEDICINES_DAILY_DATA/'
SQL_TABLE = 'SEBI_PORTFOLIO_MANAGER_TRANSACTION_MONTHLY_DATA'
base_directory = r"C:\Users\Administrator\Junk"
directory = os.path.join(base_directory, SQL_TABLE)
os.makedirs(directory, exist_ok=True)

def get_data(url):
    today = datetime.date.today()
    max_rel_date = pd.read_sql("select max(Relevant_Date) as Max from SEBI_PORTFOLIO_MANAGER_TRANSACTION_MONTHLY_DATA",engine)["Max"][0]
    for f in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, f)):
            os.remove(os.path.join(directory, f))


    query = """
        SELECT *
        FROM    SEBI_PORTFOLIO_MANAGER_TRANSACTION_MONTHLY_DATA
        order by Relevant_Date desc;
    """

    past_data = pd.read_sql(query, con=engine)[['Portfolio_Manager_Name', 'Relevant_Date']].values.tolist()
    prefs = {
        "download.default_directory": directory,
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
    options.add_argument("--allow-running-insecure-content")
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()

    driver.get(url)
    robot.add_link(url)
    driver.implicitly_wait(10)
    Portfolio_Manager_Name_dropdown_element = driver.find_element("name", "pmrId")
    Portfolio_Manager_Name_dropdown_element_select = Select(Portfolio_Manager_Name_dropdown_element)
    Portfolio_Manager_Name_dropdown_options = [o.text for o in Portfolio_Manager_Name_dropdown_element_select.options]
    time.sleep(5)
    month_indices = [10, 7, 4, 1]
    for month_index in month_indices:
        for i, Portfolio_Manager_Name in enumerate(Portfolio_Manager_Name_dropdown_options):
            if i == 0:
                continue    
            Portfolio_Manager_Name_dropdown_element_select.select_by_index(i)
            print(Portfolio_Manager_Name)
            time.sleep(2)

            year_dropdown_element = driver.find_element("name", "year")
            year_dropdown_element_select = Select(year_dropdown_element)
            year_dropdown_options = [o.text for o in year_dropdown_element_select.options]
            time.sleep(5    )
            for j, year in enumerate(year_dropdown_options[:2]):
                if j == 0:    
                    continue    
                year_dropdown_element_select.select_by_index(j)
                print(Portfolio_Manager_Name, year)
                time.sleep(2)

                month_dropdown_element = driver.find_element("name", "month")
                month_dropdown_element_select = Select(month_dropdown_element)
                month_dropdown_options = [o.text for o in month_dropdown_element_select.options]
                time.sleep(5    )
                #for k, month in     enumerate(month_dropdown_options):
                data_points = []
                #if k == 0:
                #    continue
                month_number = datetime.datetime.strptime(month_dropdown_options[month_index], "%B").month
                last_day = calendar.monthrange(int(year), month_number)[1]
                Relevant_Date = datetime.date(int(year), month_number, last_day)
                if Relevant_Date >= today or [Portfolio_Manager_Name, Relevant_Date] in past_data:
                    continue
                month_dropdown_element_select.select_by_index(month_index)
                print(i, Portfolio_Manager_Name, year, month_dropdown_options[month_index])
                time.sleep(2)
                driver.find_element(By.XPATH, "//a[@title='Go']").click()
                time.sleep(5)
                no_record = False
                for l in range(2):
                    if l == 1:
                        no_record =True
                    html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    if 'No Records Found.'.lower() in soup.get_text(strip=True).lower():
                        time.sleep(5)
                        continue
                    else:
                        break
                if no_record:
                    print('no recores found')
                    Portfolio_Manager_Name_dropdown_element = driver.find_element("name", "pmrId")
                    Portfolio_Manager_Name_dropdown_element_select = Select(Portfolio_Manager_Name_dropdown_element)
                    Portfolio_Manager_Name_dropdown_options = [o.text for o in Portfolio_Manager_Name_dropdown_element_select.options]
                    year_dropdown_element = driver.find_element("name", "year")
                    year_dropdown_element_select = Select(year_dropdown_element)
                    year_dropdown_options = [o.text for o in year_dropdown_element_select.options]
                    month_dropdown_element = driver.find_element("name", "month")
                    month_dropdown_element_select = Select(month_dropdown_element)
                    month_dropdown_options = [o.text for o in month_dropdown_element_select.options]
                    continue
                try:
                    discretionary_h3 = [h3 for h3 in soup.find_all('h3') if 'data for discretionary services' in h3.get_text(strip=True).lower()][0]
                    discretionary_transaction_h3 = [h3 for h3 in discretionary_h3.find_all_next('h3') if 'transaction data' in h3.get_text(strip=True).lower()][0]
                    discretionary_table = discretionary_transaction_h3.find_next('table')
                    for discretionary_tr in discretionary_table.find_all('tr'):
                        if 'particulars' in discretionary_tr.get_text(strip=True).lower() or not discretionary_tr.get_text(strip=True):
                            continue
                        discretionary_Type = 'Discretionary Services Data'
                        discretionary_Particular = discretionary_tr.find_all('td')[1].get_text(strip=True)
                        discretionary_Figure = discretionary_tr.find_all('td')[2].get_text(strip=True)
                        data_points.append(
                            [
                                discretionary_Type, discretionary_Particular, discretionary_Figure
                            ]
                        )
                except:
                    pass

                try:
                    non_discretionary_h3 = [h3 for h3 in soup.find_all('h3') if 'data for non-discretionary services' in h3.get_text(strip=True).lower()][0]
                    non_discretionary_transaction_h3 = [h3 for h3 in non_discretionary_h3.find_all_next('h3') if 'transaction data' in h3.get_text(strip=True).lower()][0]
                    non_discretionary_table = non_discretionary_transaction_h3.find_next('table')
                    for non_discretionary_tr in non_discretionary_table.find_all('tr'):
                        if 'particulars' in non_discretionary_tr.get_text(strip=True).lower() or not non_discretionary_tr.get_text(strip=True):
                            continue
                        non_discretionary_Type = 'Non-Discretionary Services Data'
                        non_discretionary_Particular = non_discretionary_tr.find_all('td')[1].get_text(strip=True)
                        non_discretionary_Figure = non_discretionary_tr.find_all('td')[2].get_text(strip=True)
                        data_points.append(
                            [
                                non_discretionary_Type, non_discretionary_Particular, non_discretionary_Figure
                            ]
                        )
                except:
                    pass


                df = pd.DataFrame(data_points, columns=
                    [
                        'Type', 'Particular', 'Figure',
                    ]
                )
                df["Portfolio_Manager_Name"] = Portfolio_Manager_Name
                df["Relevant_Date"] = Relevant_Date
                df["Runtime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if df.empty == False:
                    print('New data found')
                    df.to_sql(SQL_TABLE,index=False, if_exists='append', con=engine)
                    past_data = pd.read_sql(query, con=engine)[['Portfolio_Manager_Name', 'Relevant_Date']].values.tolist()

                    #MySql_CH.ch_truncate_and_insert(SQL_TABLE)
                    print('Data was uploaded to SQL and CH successfully!')
                else:
                    print('No new data found')

                Portfolio_Manager_Name_dropdown_element = driver.find_element("name", "pmrId")
                Portfolio_Manager_Name_dropdown_element_select = Select(Portfolio_Manager_Name_dropdown_element)
                Portfolio_Manager_Name_dropdown_options = [o.text for o in Portfolio_Manager_Name_dropdown_element_select.options]
                year_dropdown_element = driver.find_element("name", "year")
                year_dropdown_element_select = Select(year_dropdown_element)
                year_dropdown_options = [o.text for o in year_dropdown_element_select.options]
                month_dropdown_element = driver.find_element("name", "month")
                month_dropdown_element_select = Select(month_dropdown_element)
                month_dropdown_options = [o.text for o in month_dropdown_element_select.options]


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('C:/Users/Administrator/AdQvestDir/')

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'SEBI_PORTFOLIO_MANAGER_TRANSACTION_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
            #pass
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
            #pass


        url = 'https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes'
        get_data(url)


        #os.chdir(r'C:\Users\Administrator\AdQvestDir\Windows_3\Adqvest_Function')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        import traceback
        traceback.print_exc()
        tb_string = traceback.format_exc()
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,tb_string, no_of_ping)
        #log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')