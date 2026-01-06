from selenium import webdriver
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.select import Select
import sys
import os
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
from adqvest_robotstxt import Robots
robot = Robots(__file__)
import adqvest_db
import JobLogNew as log
import ClickHouse_db

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()

    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days


    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HPX_GDAM_MARKET_SNAPSHOT_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_rel_date=pd.read_sql('Select max(Relevant_Date) as Max from HPX_GDAM_MARKET_SNAPSHOT_DAILY_DATA',engine)['Max'][0]
        max_rel_date
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
        driver.get("https://www.hpxindia.com/MarketDepth/GDAM/GDAM_market_snapshot.html")
        robot.add_link("https://www.hpxindia.com/MarketDepth/GDAM/GDAM_market_snapshot.html")
        driver.implicitly_wait(10)
        time.sleep(5)
        driver.find_element("xpath",'//*[@value="3"]').click()
        
        time.sleep(5)
        sel = Select(driver.find_element("xpath",'//*[@id="ddldelper"]'))
        time.sleep(5)
        sel.select_by_visible_text("Select Range")
        time.sleep(5)
        while st_date < today.date():
            print(st_date.strftime('%d/%m/%Y'))
            driver.find_element('xpath', '//input[@id="startdate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            driver.find_element('xpath', '//input[@id="enddate"]').send_keys(st_date.strftime('%d/%m/%Y'))
            time.sleep(2)
            elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
            elem.click()
            time.sleep(2)
            tables = pd.read_html(driver.page_source)
            df = tables[4]
            df = df.dropna(how = 'all', axis = 1)
            df = df.dropna(how = 'all', axis = 0)
            df.columns=['Date','PURCHASE_BID', 'MW_TOTAL', 'MW_SOLAR', 'MW_NSOLAR', 'MW_HYDRO','MCV_TOTAL', 'MCV_SOLAR', 'MCV_NSOLAR', 'MCV_HYDRO', 'CLEVOL_TOTAL','CLEVOL_SOLAR', 'CLEVOL_NSOLAR', 'CLEVOL_HYDRO', 'CURTAIL_TOTAL','CURTAIL_SOLAR', 'CURTAIL_NSOLAR', 'CURTAIL_HYDRO', 'SCHEDVOL_TOTAL','SCHEDVOL_SOLAR', 'SCHEDVOL_NSOLAR', 'SCHEDVOL_HYDRO', 'MCP']            
            
            df['Relevant_Date'] = pd.to_datetime(df.Date, dayfirst=True).dt.date
            df = df.drop('Date',axis=1)
            final_df = pd.concat([final_df, df])
            st_date = st_date + days

        if len(final_df) != 0:
            final_df = final_df.drop_duplicates()
            final_df['Runtime']=datetime.datetime.now(india_time)
            final_df=final_df[final_df['Relevant_Date']>max_rel_date]
            final_df.to_sql(name='HPX_GDAM_MARKET_SNAPSHOT_DAILY_DATA',con=engine,if_exists='append',index=False)

        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
             driver.quit()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
