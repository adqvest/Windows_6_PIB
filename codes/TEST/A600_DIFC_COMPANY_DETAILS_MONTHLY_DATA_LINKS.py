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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
warnings.filterwarnings('ignore')
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

def spaces(x):
  try:
    return re.sub(' +', ' ', x)
  except:
    return None

def splchar(x):
  try:
    return re.sub('[^A-Za-z0-9]+', ' ', x)
  except:
    return None

def run_program(run_by='Adqvest_Bot', py_file_name=None):

    os.chdir('C:/Users/Administrator/AdQvestDir/')

    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    # ### DATE TIME VARIABLES

    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    run_time = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        # connection.close()
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe" #@ToDo: Uncomment Later in Prod
        
        # if (today.date().day == 1):
    
        #     query = "DELETE from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW WHERE Reg_No > '1'"
        #     connection.execute(query)
            
        #     print("DATA DELETED FROM SQL")

        # # Fetching Max Registraion Number From Table

        # query = "SELECT MAX(Reg_No) AS Reg_No from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW"
        # date = pd.read_sql(str(query),engine)
        # date = date.iloc[0,0]

        # Fetching Max Registraion Number From Website

        prefs = {
            "download.default_directory": "C:\\Users\\Administrator\\Junk",
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
        driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)


        driver.maximize_window()
        driver.get("https://www.difc.ae/public-register")
        time.sleep(5)
        # robot.add_link("https://www.difc.ae/public-register/?companyName=&registrationNo=&status=&type=&sortBy=") @TOdo: Uncomment in production
        try:
            driver.execute_script("""return document.querySelector('#usercentrics-root').shadowRoot.querySelector("button[data-testid='uc-deny-all-button']")""").click()
        except:
            pass
        time.sleep(2)
        reg_nos = [i for i in range(1,7547)]
        query = "SELECT Reg_No from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW"
        df = pd.read_sql(str(query),engine)['Reg_No']
        new_reg_no = df.drop_duplicates()

        missingItems = [x for x in reg_nos if not x in sorted(list(new_reg_no))]
        len(missingItems)
        missingitems = ["%04d" % i for i in missingItems]

        comp_links = []
        reg_no = []
        for i in missingitems:
            print(i)
            driver.find_element(By.XPATH, '//input[@id="searchInput"]').clear()
            driver.find_element(By.XPATH, '//input[@id="searchInput"]').send_keys(i)
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source)
            links_ele = soup.find('div', class_ = 'RightArrowSvg-wrapper')
            if links_ele != None:
                link = links_ele.find('a')['href']
                reg = soup.find('p', class_='b19 text-body-gray').text.split('#')[-1].strip() # Registration status
                reg_no.append(reg)
                comp_links.append(link)
            driver.find_element(By.XPATH, '//input[@id="searchInput"]').clear()
            time.sleep(5)
            
        print(len(comp_links))
        links_df = pd.DataFrame()
        links_df['Links'] = comp_links
        links_df['Registration_No'] = reg_no
        links_df['Relevant_Date'] = today.date()
        links_df['Runtime'] = today
        print(links_df)
        engine = adqvest_db.db_conn()
        links_df.to_sql('DIFC_LISTED_COMPANY_MONTHLY_LINKS_Temp_Pushkar', index=False, if_exists = 'append', con = engine)
        
            # if int(i)%100 == 0:
            #     driver.quit()
            #     print(len(comp_links))
            #     links_df = pd.DataFrame()
            #     links_df['Links'] = comp_links
            #     links_df['Registration_No'] = reg_no
            #     links_df['Relevant_Date'] = today.date()
            #     links_df['Runtime'] = today
            #     print(links_df)
            #     engine = adqvest_db.db_conn()
            #     links_df.to_sql('DIFC_LISTED_COMPANY_MONTHLY_LINKS_Temp_Pushkar', index=False, if_exists = 'append', con = engine)
                
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

        driver.quit()

        driver.quit()
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')

#%%
# while True:
#     try:
#         engine = adqvest_db.db_conn()
#         connection = engine.connect()
#         break
#     except:
#         tries += 1
#         if tries > 10:
#             break