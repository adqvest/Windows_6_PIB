import datetime
import os
import re
import sys
import time
import warnings

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pytz import timezone
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import ClickHouse_db
import cleancompanies
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '12_PM_WINDOWS_SERVER_CRAWLER_SCHEDULER_ALL_CODES'
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        last_rel_date = pd.read_sql("select max(Relevant_Date) as Max from LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA",engine)
        last_rel_date = last_rel_date["Max"][0]
        if(today.date() - last_rel_date >= datetime.timedelta(3)):

            insurer = pd.read_sql('select distinct Company from LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA where Relevant_Date="2023-06-22"',engine)['Company']
            print(last_rel_date)
            print('Days since last rel date: ', today.date() - last_rel_date)
            chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            prefs = {
                # "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True
                }
            options = webdriver.ChromeOptions()
            options.add_experimental_option('prefs', prefs)
            options.add_argument("--disable-infobars")
            options.add_argument("start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--no-sandbox')
            options.add_experimental_option('prefs', prefs)
            driver = webdriver.Chrome(executable_path=chrome_driver_path, options=options)

            driver.get("https://www.policybazaar.com/life-insurance/term-insurance/1-crore-term-insurance/")
            robot.add_link("https://www.policybazaar.com/life-insurance/term-insurance/1-crore-term-insurance/")
            #old link "http://www.policybazaar.com/life-insurance/term-insurance/"

            driver.maximize_window()
            time.sleep(5)
            ss = BeautifulSoup(driver.page_source,'lxml')
            table = ss.find_all('div',class_='top-plan-content-wrapper')[0]

            company = [i.find('img',alt=True)['alt'] for i in table.find_all('div',class_='logo-block card-top-block')]
            policy = [i.text.strip() for i in table.find_all('span',class_='plan-name')]

            claims = [i.text.replace('\n',' ').strip() for i in table.find_all('div',class_='claim-settlement card-top-block') if 'Claim' in i.text]
            claims = [j.split('nt ')[1] for j in claims]

            price = [i.text.replace('\n',' ').strip() for i in table.find_all('div',class_='claim-settlement card-top-block') if 'Price' in i.text]
            price = [j.strip().split(' ')[-1] for j in price]

            table = pd.DataFrame({"Company":company, "Policy_Name":policy, "Claim_Settlement_Ratio":claims, "Yearly_Premium":price})
            # insurer=pd.read_html(driver.page_source)[0][0]
            comp_clean=[]
            for tab in table['Company']:
                ju = 0
                cn = ''
                for clean in list(insurer):
                    if tab.split('_')[0].lower() in clean.lower():
            #             print(tab.split(' ')[0].lower(),clean)
                        comp_clean.append(clean)
                        cn='Done'
                        continue
                if cn!='Done' :
                    if 'bajaj' in tab.lower():
                        print('bajaaaj')
                        comp_clean.append('Bajaj Allianz Life Insurance')
                    elif 'Term' not in tab:
                        comp_clean.append(tab+' Life Insurance')
                    else:
                        comp.clean.append(tab)

            table['Company'] = comp_clean
            table["Company"] = table["Company"].replace('_','',regex=True).str.strip().str.replace("Term Insurance","")
            table["Claim_Settlement_Ratio"] = table["Claim_Settlement_Ratio"].apply(lambda x: float(x.replace("%","")))

            table["Yearly_Premium"] = table["Yearly_Premium"].replace('-','0')
            table["Yearly_Premium"] = table["Yearly_Premium"].apply(lambda x: float(x.replace("/month","").replace("Rs.","")))
            table["Yearly_Premium"] = np.where(table["Yearly_Premium"]== 0.0, np.nan,table["Yearly_Premium"])
            table["Yearly_Premium"] = 12 * table["Yearly_Premium"]
            table["Relevant_Date"] = today.date()
            table['Runtime'] = pd.to_datetime(datetime.datetime.now(india_time).strftime("%Y-%m-%d %H:%M:%S"))

            table, unmapped = cleancompanies.comp_clean(table, 'Company', 'life_insurance', 'Company_Clean', 'LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA')
            table.drop_duplicates(inplace=True)

            table.to_sql(name = "LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA",con = engine,index = False,if_exists='append')
            
            click_max_date = client.execute("select max(Relevant_Date) from AdqvestDB.LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA")
            click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
            sql_df = pd.read_sql("select * from LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA WHERE  Relevant_Date > '" + str(click_max_date)+ "' ", con=engine)
            client.execute("INSERT INTO LIFE_INSURANCE_CLAIMS_PREMIUM_3_DAYS_DATA VALUES", sql_df.values.tolist())
            print('data inserted into clickhouse')
            if len(unmapped) > 0:
                raise Exception(f'Company Clean Mapping not available for few companies for + {table_name} + Please check GENERIC_COMPANY_UNMAPPED_TABLE for more details')
        else:
            print('No new data')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
