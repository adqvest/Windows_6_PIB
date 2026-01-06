import os
import re
import sys
import warnings
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
import pandas as pd
from pytz import timezone
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.now(india_time)
    days       = timedelta(1)
    yesterday = today - days


    job_start_time = datetime.now(india_time)
    table_name = 'HPX_TAM_MONTHLY_TRADE_DETAILS_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date=pd.read_sql('Select max(Relevant_Date) as Max from HPX_TAM_MONTHLY_TRADE_DETAILS_DATA',engine)['Max'][0]
        chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AdQvestDir\HPX"

        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
        driver.maximize_window()

        st_date = max_rel_date + days
        final_df = pd.DataFrame()
        driver.get("https://www.hpxindia.com/MarketDepth/TAM/TAM_Monthly.html")
        robot.add_link("https://www.hpxindia.com/MarketDepth/TAM/TAM_Monthly.html")
        driver.implicitly_wait(5)

        # driver.find_element(By.XPATH,'//*[@id="tradepriod"]').click()
        # time.sleep(5)

        sel = Select(driver.find_element(By.XPATH,'//*[@id="tradepriod"]'))
        time.sleep(5)
        sel.select_by_visible_text("Trade Date Period")
        sel = Select(driver.find_element(By.XPATH,'//*[@id="Select"]'))
        sel.select_by_visible_text("Select Range")

        time.sleep(5)
        while st_date < today.date():
            driver.find_element(By.XPATH,'//input[@id="startdate"]').click()
            driver.find_element(By.XPATH,'//input[@id="startdate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            # time.sleep(5)
            end_date = st_date + relativedelta(months=2, days = -1, day=1)
            driver.find_element(By.XPATH,'//input[@id="startdate"]')
            driver.find_element(By.XPATH,'//input[@id="enddate"]').send_keys((end_date).strftime('%d/%m/%Y'))            
            # time.sleep(2)
            elem = driver.find_element(By.XPATH,'//*[@id="btnSubmit"]')
            elem.click()
            time.sleep(5)
            
            soup = BeautifulSoup(driver.page_source)
            check = soup.find(string = 'No Records Found')
            tables = pd.read_html(driver.page_source)

            df = tables[1]  
            # print(check)
            print(df)
            if (len(df) > 0):
                print('Records Found')
                df.columns=['Trade_Date','Instrument_Name','Buy_BID_MUs','Sell_BID_MUs','Tradeed_Volume_MUs','Price_Discovered_RS_PER_MWh','Delivery_Month']
                
                df = df.dropna(how = 'all', axis = 1)
                df['Relevant_Date'] = pd.to_datetime(df.Trade_Date, dayfirst=True).dt.date
                print(df)
                final_df = pd.concat([final_df, df])
            st_date = end_date + days
        print(final_df)
        if len(final_df) > 0:
            final_df = final_df.drop_duplicates()
            final_df['Runtime']=pd.to_datetime('now')
            final_df=final_df[final_df['Relevant_Date']>max_rel_date]
            final_df.to_sql(name='HPX_TAM_MONTHLY_TRADE_DETAILS_DATA',con=engine,if_exists='append',index=False)
        else:
            print("No new data")
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
