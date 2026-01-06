import numpy as np
import pandas as pd
import sys
import os

from pytz import timezone
import datetime as datetime

import re

import time
from time import sleep
# from dateutil import parser

from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

pd.options.display.max_columns = None
pd.options.display.max_rows = None

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()

    # ****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday  = today - days

    run_time = pd.to_datetime(today.strftime("%Y/%m/%d %H:%M:%S"))

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_FINANCIAL_RESULTS_XBRL_QUARTERLY_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if (run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)

        df1 = pd.read_excel('C:/Users/Administrator/AdQvestDir/codes/TEST/nse_company_listing.xlsx')
        companies = df1['Symbol']

        failed_company = []
        
        # Setup WebDriver
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        driver = webdriver.Chrome(executable_path=driver_path)
        driver.maximize_window()

        # Navigate to the target webpage
        driver.get("https://www.nseindia.com/companies-listing/corporate-filings-financial-results")
        time.sleep(2)

        for company in companies:
            company = company.strip()
            print(f"Processing company: {company}")

            # Selecting the company
            text_box = driver.find_element("id",'financials_equities_companyName')
            
            text_box.send_keys('\ue003' * 100)  # Sends 50 backspace keys
            time.sleep(1)

            text_box.send_keys(company)
            time.sleep(3)
            
            try:
            
                driver.find_element(By.CSS_SELECTOR, '.tt-menu .tt-suggestion.tt-selectable').click()
                time.sleep(3)
                table = driver.find_element(By.ID, 'CFfinancialequityTable')

                # Extract table contents
                rows = table.find_elements(By.TAG_NAME, 'tr')
                table_data = []
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    row_data = []
                    for cell in cells:
                        cell_text = cell.text
                        link = cell.find_element(By.TAG_NAME, 'a') if cell.find_elements(By.TAG_NAME, 'a') else None
                        if link and link.get_attribute('href') != 'javascript:;':
                            cell_text += link.get_attribute('href')
                        row_data.append(cell_text)
                    table_data.append(row_data)

                df = pd.DataFrame(table_data,columns = ['Company_Name','Audited_UnAudited','Cumulative_NonCumulative',
                                                        'Consolidated_NonConsolidated','IND_AS_NON_IND_AS','Period',
                                                        'Period_Date','Relating_To','Xbrl_File','Broadcast_Date','1','2','3'])

                df = df.drop(['1','2','3'],axis = 1)

                df = df[df["Company_Name"].notna() & (df["Company_Name"] != '')]
                df = df[df["Xbrl_File"].notna() & (df["Xbrl_File"] != '') & (df["Xbrl_File"] != '-')]

                df["Period_Date"] = [datetime.datetime.strptime(x,"%d-%b-%Y").date() for x in df["Period_Date"]]
                df["Broadcast_Date"] = [datetime.datetime.strptime(x,"%d-%b-%Y %H:%M:%S").date() for x in df["Broadcast_Date"] if x != '-']

                df["Status"] = ''
                df["Ticker"] = company
                df["Relevant_Date"] = today.date()
                df["Runtime"] = run_time

                engine = adqvest_db.db_conn()
                connection = engine.connect()

                df.to_sql('NSE_FINANCIAL_RESULTS_XBRL_QUARTERLY_DATA',index=False, if_exists='append', con=engine)

                connection.close()
                engine.dispose()

                print(f"**** Finished Collecing For: {company} ****")
                    
            except Exception as e:
                
                failed_company.append(company)
                
                
        print("ALL COMPANIES COLLECTED")
        driver.quit()

        failed_company = pd.DataFrame(failed_company)
        failed_company.to_excel('C:/Users/Administrator/AdQvestDir/codes/TEST/Failed_Financial_Xbrl.xlsx')

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)


if (__name__ == '__main__'):
    run_program(run_by='manual')
