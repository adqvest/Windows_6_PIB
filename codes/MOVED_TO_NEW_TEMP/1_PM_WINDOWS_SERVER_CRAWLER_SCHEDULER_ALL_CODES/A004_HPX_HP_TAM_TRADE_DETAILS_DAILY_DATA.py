import os
import re
import sys
import warnings
from datetime import datetime, timedelta
from selenium import webdriver
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.select import Select
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
    table_name = 'HPX_HP_TAM_TRADE_DETAILS_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date=pd.read_sql('Select max(Relevant_Date) as Max from HPX_HP_TAM_TRADE_DETAILS_DAILY_DATA',engine)['Max'][0]
        chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\AdQvestDir\HPX"

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
        options.add_argument("--allow-running-insecure-content")
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
        driver.maximize_window()

        st_date = max_rel_date + days
        final_df = pd.DataFrame()
        driver.get("https://www.hpxindia.com/MarketDepth/HPTAM/HPTAM_TradeDetails.html")
        robot.add_link("https://www.hpxindia.com/MarketDepth/HPTAM/HPTAM_TradeDetails.html")
        driver.implicitly_wait(10)
        driver.find_element("xpath",'//*[@id="All"]').click()
        time.sleep(5)
        sel = Select(driver.find_element("xpath",'//*[@id="tradepriod"]'))
        time.sleep(5)
        sel.select_by_visible_text("Select Range")
        time.sleep(5)
        while st_date < today.date():
            driver.find_element('xpath', '//input[@id="startdate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            driver.find_element('xpath', '//input[@id="enddate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            time.sleep(2)
            elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
            elem.click()
            time.sleep(2)
            tables = pd.read_html(driver.page_source)
            df = tables[0]
            if df.empty != True:
                df.columns=['Trade_Date','CONTRACTTYPE','INSTRUMENT_NAME','TOTAL_TRADED_VOLUME_IN_MWh','EQUILIBRIUM_PRICE_RS_PER_MWh','MINIMUM_PRICE_IN_RS_PER_MWh','MAXIMUM_PRICE_IN_RS_PER_MWh','WEIGHTED_AVG_PRICE_IN_RS_PER_MWh']
                df = df.dropna(how = 'all', axis = 1)
                df['TOTAL_TRADED_VOLUME_IN_MU'] = df['TOTAL_TRADED_VOLUME_IN_MWh']*0.001
                df['Relevant_Date'] = pd.to_datetime(df.Trade_Date, dayfirst=True).dt.date
                final_df = pd.concat([final_df, df])    
                final_df = final_df.drop_duplicates()
                final_df['Runtime']=datetime.now(india_time)
                final_df=final_df[final_df['Relevant_Date']>max_rel_date]
                final_df.to_sql(name='HPX_HP_TAM_TRADE_DETAILS_DAILY_DATA',con=engine,if_exists='append',index=False)
            else:
                print("No new data")
            st_date = st_date + days
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
