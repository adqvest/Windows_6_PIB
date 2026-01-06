import os
import re
import sys
import warnings
from calendar import monthrange
from datetime import datetime, timedelta
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
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
from playwright.sync_api import sync_playwright
import pandas as pd
import requests
from pytz import timezone

warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import adqvest_s3
import boto3
from botocore.config import Config
import JobLogNew as log
import ClickHouse_db
#import Sql_To_Clickhouse
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def end_date(date):
    year=date.split("-")[1]
    year=int(year)
    month=date.split("-")[0]
    month_number = datetime.strptime(month, '%B').month
    print(month_number)
    end_dt = datetime(year, month_number, monthrange(year, month_number)[1]).date()
    return end_dt


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()

    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    days       = timedelta(1)
    yesterday = today - days


    job_start_time = datetime.now(india_time)
    table_name = 'HPX_GTAM_TRADE_DETAILS_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

   
        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
        BUCKET_NAME = 'adqvests3bucket'

        s3 = boto3.resource('s3',
                            aws_access_key_id=ACCESS_KEY_ID,
                            aws_secret_access_key=ACCESS_SECRET_KEY,
                            config=Config(signature_version='s3v4', region_name='ap-south-1')
                            )
        max_rel_date=pd.read_sql('Select max(Relevant_Date) as Max from HPX_GTAM_TRADE_DETAILS_DAILY_DATA',engine)['Max'][0]
        # chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        # # download_file_path = r"C:\\Users\\Administrator\\HPX"
        # download_file_path = r"C:\Users\Administrator\AdQvestDir\HPX"
        
        # os.chdir(r"C:\Users\Administrator\AdQvestDir\HPX")
        # for file in os.listdir():
        #     if file.endswith('.xls'):
        #         os.remove(file)


        # prefs = {
        #     "download.default_directory": download_file_path,
        #     "download.prompt_for_download": False,
        #     "download.directory_upgrade": True
        #     }
        # options = webdriver.ChromeOptions()
        # options.add_argument("--disable-infobars")
        # options.add_argument("start-maximized")
        # options.add_argument("--disable-extensions")
        # options.add_argument("--disable-notifications")
        # options.add_argument('--ignore-certificate-errors')
        # options.add_argument('--no-sandbox')
        # options.add_argument("--allow-running-insecure-content")
        # options.add_experimental_option('prefs', prefs)
        # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

        # driver.maximize_window()


        # driver.get("https://www.hpxindia.com/MarketDepth/TAM/g-tam_details.html")
        # driver.implicitly_wait(10)

        # driver.find_element("xpath",'//*[@id="Contingency"]').click()
        # time.sleep(10)

        # sel = Select(driver.find_element("xpath",'//*[@id="tradepriod"]'))
        # time.sleep(5)
        # sel.select_by_visible_text("Last 31 Days")
        # time.sleep(10)


        # elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
        # elem.click()
        # time.sleep(5)
        # soup = BeautifulSoup(driver.page_source, 'html')

        # time.sleep(10)
        
        # elem = driver.find_element("xpath",'/html/body/div[4]/div/div[1]/center/div[2]')
        # elem.click()
        # time.sleep(10)

        # for file in os.listdir():
        #     if file.endswith('.xls'):
        #         print(file)
        #         file_name=file
        # df = pd.read_html(file_name)
        # df_final=df[0] 
         
        # changes made here kamalinee
        url='https://www.hpxindia.com/MarketDepth/TAM/g-tam_details.html'
        robot.add_link(url)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=50)
            page = browser.new_page()
            # page.setDefaultTimeout(timeout=30000*5);
            page.goto(url, wait_until='networkidle',timeout=60000*5)
            time.sleep(5)
            page.locator('//*[@id="Contingency"]').click()
            page.select_option('//*[@id="tradepriod"]', 'Last 31 Days')
            time.sleep(5)
            page.locator('//*[@id="btnSubmit"]').click()
            time.sleep(5)
            soup = BeautifulSoup(page.content(), 'lxml')
            with page.expect_download() as download_info:
                #Modified   |   Joe
                page.wait_for_selector('div[ng-click="export(\'excel\')"]', timeout = 120)
                time.sleep(5)
                page.locator('div[ng-click="export(\'excel\')"]') .click()               
                time.sleep(4)   #
            download = download_info.value
            file_name = f'hpx_gdam.xls'
            download.save_as(os.path.join('C:/Users/Administrator/AdQvestDir/HPX',file_name))
            time.sleep(10)
# 
            for file in os.listdir():
                if file.endswith('.xls'):
                    print(file)
                    file_name=file


        print(file_name)
        time.sleep(5)
        os.chdir('C:/Users/Administrator/AdQvestDir/HPX')
        data_s3 =  open(file_name, 'rb')
        s3.Bucket(BUCKET_NAME).put_object(Key='HPX/GTAM/'+ str(today.date())+'_'+file_name, Body=data_s3)
        data_s3.close()
        df = pd.read_html(file_name)
        df_final=df[0]        

       
        if len(df_final)==0:
            print("New data not yet published")

        else:

            df_final.columns=['Trade_Date','CONTRACTTYPE','INSTRUMENT_NAME','TOTAL_TRADED_VOLUME_IN_MWh','EQUILIBRIUM_PRICE_RS_PER_MWh','MINIMUM_PRICE_IN_RS_PER_MWh','MAXIMUM_PRICE_IN_RS_PER_MWh','WEIGHTED_AVG_PRICE_IN_RS_PER_MWh']
            df_final['Relevant_Date']=pd.to_datetime(df_final['Trade_Date'],format="%d/%m/%Y").dt.date
            df_final['Runtime']=pd.to_datetime('now')
            df_final['TOTAL_TRADED_VOLUME_IN_MU']=df_final['TOTAL_TRADED_VOLUME_IN_MWh']*0.001
            df_final=df_final[['Trade_Date','CONTRACTTYPE','INSTRUMENT_NAME','TOTAL_TRADED_VOLUME_IN_MWh','TOTAL_TRADED_VOLUME_IN_MU','EQUILIBRIUM_PRICE_RS_PER_MWh','MINIMUM_PRICE_IN_RS_PER_MWh','MAXIMUM_PRICE_IN_RS_PER_MWh','WEIGHTED_AVG_PRICE_IN_RS_PER_MWh','Relevant_Date','Runtime']]
            df_final=df_final[df_final['Relevant_Date']>max_rel_date]
            print(df_final)
            if df_final.shape[0]==0:

                print("No new data")

            else:

                df_final.to_sql(name='HPX_GTAM_TRADE_DETAILS_DAILY_DATA',con=engine,if_exists='append',index=False)
                # client = ClickHouse_db.db_conn()
                # click_max_date = client.execute("select max(Relevant_Date) from HPX_GTAM_TRADE_DETAILS_DAILY_DATA ")
                # click_max_date = str([a_tuple[0] for a_tuple in click_max_date][0])
                # query = 'select * from AdqvestDB.HPX_GTAM_TRADE_DETAILS_DAILY_DATA  where Relevant_Date > "' + click_max_date + '"'
                # df = pd.read_sql(query,engine)
                # client.execute("INSERT INTO HPX_GTAM_TRADE_DETAILS_DAILY_DATA VALUES", df.values.tolist())
        
        os.remove(f'C:/Users/Administrator/AdQvestDir/HPX/{file_name}')

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
