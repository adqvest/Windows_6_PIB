import os
import re
import sys
import time
import warnings

import numpy as np
import pandas as pd
import datetime as datetime
from dateutil import parser
from pytz import timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
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
    connection = engine.connect()
    # ### DATE TIME VARIABLES

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'DIFC_LISTED_COMPANY_MONTHLY_LINKS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        
        query = "SELECT MAX(Registration_No) AS Reg_No from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_LINKS"
        max_reg_no = pd.read_sql(str(query),engine)['Reg_No'][0] + 1
        print(f'Max Registration Number: {max_reg_no}')

        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"          #@ToDo: Uncomment Later in Prod
        
        prefs = {
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features")
        options.add_argument('--incognito')
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
        
        # Fetching Max Registraion Number From Website
        driver.maximize_window()
        driver.get("https://www.difc.ae/public-register")
        robot.add_link("https://www.difc.ae/public-register/?companyName=&registrationNo=&status=&type=&sortBy=")     #@TOdo: Uncomment in production
        time.sleep(2)
        try:
            driver.execute_script("""return document.querySelector('#usercentrics-root').shadowRoot.querySelector("button[data-testid='uc-deny-all-button']")""").click()
        except:
            pass
        time.sleep(2)
        comp_links = []
        reg_no = []
        
        max_failures = 10
        consecutive_failures = 0

        while consecutive_failures < max_failures:

            driver.find_element(By.XPATH, '//input[@id="searchInput"]').clear()
            time.sleep(2)
            driver.find_element(By.XPATH, '//input[@id="searchInput"]').send_keys(str(max_reg_no))
            time.sleep(4)
            soup = BeautifulSoup(driver.page_source)
            cond = soup.find('div', class_ = 'h3')
            

            if cond != None:
                consecutive_failures += 1
                print(cond.text)
                driver.find_element(By.XPATH, '//input[@id="searchInput"]').clear()
                time.sleep(2)
            else:
                links_ele = soup.find('div', class_ = 'RightArrowSvg-wrapper')
                if links_ele != None:
                    link = links_ele.find('a')['href']
                    # print('Here')
                    reg = soup.find('p', class_='b19 text-body-gray').text.split('#')[-1].strip() # Registration status
                    reg_no.append(reg)
                    comp_links.append(link)

                driver.find_element(By.XPATH, '//input[@id="searchInput"]').clear()
                time.sleep(5)
                consecutive_failures = 0

            max_reg_no += 1

            if consecutive_failures == max_failures:
                print('No further links available')
                driver.quit()
                print(len(comp_links))
                links_df = pd.DataFrame()
                links_df['Registration_No'] = reg_no    
                links_df['Links'] = comp_links
                links_df['Relevant_Date'] = today.date()
                links_df['Runtime'] = today
                links_df = links_df[links_df.Registration_No != 2368]
                print(links_df)
                # engine = adqvest_db.db_conn()
                links_df.to_sql('DIFC_LISTED_COMPANY_MONTHLY_LINKS', index=False, if_exists = 'append', con = engine)
                
            # elif (max_reg_no % 100) == 0:
            #     driver.quit()
            #     print(len(comp_links))
            #     links_df = pd.DataFrame()
            #     links_df['Links'] = comp_links
            #     links_df['Relevant_Date'] = today.date()
            #     links_df['Runtime'] = today
            #     print(links_df)
            #     engine = adqvest_db.db_conn()
            #     links_df.to_sql('DIFC_LISTED_COMPANY_MONTHLY_LINKS', index=False, if_exists = 'append', con = engine)
                
            #     driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
            #     driver.get("https://www.difc.ae/public-register")
            #     time.sleep(5)
            #     try:
            #         driver.execute_script("""return document.querySelector('#usercentrics-root').shadowRoot.querySelector("button[data-testid='uc-deny-all-button']")""").click()
            #     except:
            #         pass
            #     time.sleep(2)
            #     comp_links = []
            #     reg_no = []

        # driver.quit()
        if today.day == 7:
            query = "Update DIFC_LISTED_COMPANY_MONTHLY_LINKS set Status = Null"
            connection.execute(query)
            print('Links status set to null for recollection of data')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
