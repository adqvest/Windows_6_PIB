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


def Clean_data(df):
    for i in range(len(df)):

        if str(df["License_Validity_Date"][i]) == 'nan':
            df["License_Validity_Date"][i] = '01-01-2100'
        else:
            pass

        df["License_Validity_Date"][i] = parser.parse(df["License_Validity_Date"][i], dayfirst=True).date()

        if str(df["Incorpation_Date"][i]) == 'nan':    
            df["Incorpation_Date"][i] = '01-01-2100'
        else:
            pass
        df["Incorpation_Date"][i] = parser.parse(df["Incorpation_Date"][i], dayfirst=True).date()
    
    df=drop_duplicates(df)
    return df

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
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe" #@ToDo: Uncomment Later in Prod
        
        if (today.day == 7):
            print('Backing Up Old Data')
            query = "DELETE from TempBackUp.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW"
            connection.execute(query)
            del query

            query = "INSERT INTO TempBackUp.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW SELECT * FROM AdqvestDb.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW"
            connection.execute(query)
            del query

            print("DELETED FROM SQL")
            query = "DELETE from AdqvestDB.DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW"
            connection.execute(query)
            del query
            print("Recollecting data from scratch")

            comp_links = pd.read_sql('SELECT Links FROM DIFC_LISTED_COMPANY_MONTHLY_LINKS', con = engine)
            comp_links = comp_links.drop_duplicates()
            parts = len(comp_links)//1000
            div = int(len(comp_links)/parts)
            comp_links = comp_links.iloc[:div, :]
            print(comp_links)

        elif today.day != 7:
            print('Checking for missed companies')
            comp_links = pd.read_sql('SELECT Links FROM DIFC_LISTED_COMPANY_MONTHLY_LINKS WHERE Status is null', con = engine)
            if len(comp_links) >= 1000:
                comp_links = comp_links.drop_duplicates()
                parts = len(comp_links)//1000
                div = int(len(comp_links)/parts)
                comp_links = comp_links.iloc[:div, :]
            else:
                pass
            print(comp_links)

        if len(comp_links) > 0:            
            links_lst = []
            cname = []
            tname = []
            reg = []
            structure = []
            status = []
            business = []
            Type = []
            doid = []
            dovd = []

            count = 0
            driver = webdriver.Chrome(executable_path=driver_path, options=options)
            for link in comp_links['Links']:
                print(link)
                try:
                    driver.get(link)
                    time.sleep(10)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="relative"]')))
                except TimeoutException:
                    driver.refresh()
                    time.sleep(5)
                    driver.get(link)
                    time.sleep(20)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="relative"]')))

                try:
                    driver.execute_script("""return document.querySelector('#usercentrics-root').shadowRoot.querySelector("button[data-testid='uc-deny-all-button']")""").click()
                except:
                    pass
                time.sleep(5)
                soup2 = BeautifulSoup(driver.page_source, 'html')
                
                reg_no = soup2.find('div', class_='registration-number mb-[16px] mt-5').text.strip().split('#')[-1].strip() #Registration no
                
                try:
                    reg_stat = soup2.find('div', class_='Tags-wrapper flex gap-x-[12px]').find('div').text.strip() # Registration status
                except:
                    reg_stat = np.nan

                try:
                    cur_name = soup2.find('span', string=re.compile('Current name.+')).find_next_sibling().text.strip() # Current name
                except:
                    cur_name = np.nan

                try:    
                    trade_name = soup2.find('span', string=re.compile('Trading name.+')).find_next_sibling().text.strip() # Trading name
                except:
                    trade_name = np.nan

                try:
                    lic_type = soup2.find('span', string=re.compile('.+License.+')).find_next_sibling().text.strip() # License type
                except:
                    lic_type = np.nan

                try:
                    struct = soup2.find('span', string=re.compile('.+Structure.+')).find_next_sibling().text.strip() # Structure
                except:
                    struct = np.nan

                try:    
                    activity = soup2.find('span', string=re.compile('Activities.+')).find_next_sibling().find('li').text.strip() # Business activities
                    if activity == '':
                        activity = np.nan
                except:
                    activity = np.nan

                try:
                    reg_dt = soup2.find('span', string=re.compile('Date of registration.+')).find_next_sibling().text.strip() # Incorporation date
                except:
                    try:
                        reg_dt = soup2.find('span', string=re.compile('.+Incorporation.+')).find_next_sibling().text.strip() # Incorporation date
                    except:
                        reg_dt = np.nan
                        # try:
                        #     reg_dt = soup2.find('span', string=re.compile('.+date of registration.+')).find_next_sibling().text.split('-')[-1].strip() # Incorporation date
                        # except:
                        #     reg_dt = np.nan
                try:
                    lic_valid = soup2.find('span', string=re.compile('License validity.+')).find_next_sibling().text.strip() # License validity
                except:
                    try:
                        lic_valid = soup2.find('span', string=re.compile('.+Dissolution.+')).find_next_sibling().text.strip() # License validity
                    except:
                        lic_valid = np.nan

                links_lst.append(link)
                cname.append(cur_name)
                tname.append(trade_name)
                reg.append(reg_no)
                structure.append(struct)
                status.append(reg_stat)
                business.append(activity)
                Type.append(lic_type)
                doid.append(reg_dt)
                dovd.append(lic_valid)
                connection.execute(f'UPDATE DIFC_LISTED_COMPANY_MONTHLY_LINKS_Temp_Pushkar SET Status = "Done" where links = "{link}"')
            
                count += 1
                if count%100 == 0:
                    driver.quit()
                    print(f'------{count} companies done------')
                    print(f'Restarting Chrome driver')
                    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
                    time.sleep(5)

                    df = pd.DataFrame({"Link":links_lst,"Company":cname,"Trading_Name":tname,"Reg_No":reg,"Legal_Structure":structure,"Registration_Status":status,"Business_Activity":business,"License_Type":Type,"Incorpation_Date":doid,"License_Validity_Date":dovd })
                    df["Relevant_Date"] = today.date()
                    df["Runtime"] = run_time
                    df.to_excel(r'C:\Users\Administrator\Junk\DIFC_DATA.xlsx')
                    df = Clean_data(df)
                    engine = adqvest_db.db_conn()
                    df.to_sql(name = "DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW",con = engine,if_exists = 'append',index = False)

                    links_lst = []
                    cname = []
                    tname = []
                    reg = []
                    structure = []
                    status = []
                    business = []
                    Type = []
                    doid = []
                    dovd = []

                elif count == len(comp_links):
                    df = pd.DataFrame({"Link":links_lst,"Company":cname,"Trading_Name":tname,"Reg_No":reg,"Legal_Structure":structure,"Registration_Status":status,"Business_Activity":business,"License_Type":Type,"Incorpation_Date":doid,"License_Validity_Date":dovd })
                    df["Relevant_Date"] = today.date()
                    df["Runtime"] = run_time
                    df.to_excel(r'C:\Users\Administrator\Junk\DIFC_DATA.xlsx')
                    df = Clean_data(df)
                    engine = adqvest_db.db_conn()
                    df.to_sql(name = "DIFC_LISTED_COMPANY_MONTHLY_DATA_NEW",con = engine,if_exists = 'append',index = False)
        else:
            print('No New Data')
        engine = adqvest_db.db_conn()
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        engine = adqvest_db.db_conn()
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
