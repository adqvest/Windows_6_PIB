import datetime as datetime
import os
import re
import sys
import warnings
import time
import pandas as pd
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.support.select import Select
from fiscalyear import *
import fiscalyear
from dateutil import relativedelta

warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)
#%%
def get_financial_year(dt):
    fiscalyear.setup_fiscal_calendar(start_month=4)
    if dt.month>=4:
        yr=dt.year+1
    else:
        yr=dt.year
    
    fy = fiscalyear.FiscalYear(yr)
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    fy_year=str(fy.start.year)+'-'+str(fy.end.year)
    return fy_year
    
        
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine     = adqvest_db.db_conn()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        query = "Select max(Relevant_Date) as Relevant_Date from NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA"
        max_date = pd.read_sql(query, con=engine)['Relevant_Date'][0]
        next_mon=max_date+relativedelta.relativedelta(months=1, day=31)
        fy_yr=get_financial_year(next_mon)

        prefs = {
                "download.default_directory": driver_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
                }

        options = webdriver.ChromeOptions()

        options.add_experimental_option('prefs', prefs) 
        options.add_experimental_option('excludeSwitches', ['enable-automation']) 

        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument('--incognito')
        # options.add_argument('--headless') #It stops opening of browser"
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("--use-fake-ui-for-media-stream")
        options.add_argument("--use-fake-device-for-media-stream")
        limit = 0
        if (today.date() - max_date).days >= 31:
            while True:
                try:
                    driver = webdriver.Chrome(executable_path=driver_path,chrome_options=options)
                    url = "https://www.nseindia.com/historical/top-n-securities-members"
                    robot.add_link(url)
                    driver.get(url)
                    no_of_ping += 1
                    time.sleep(15)
                    
                    ## modified by Santonu | Jan 08,2024|-------------------------------------
                    driver.find_element(By.XPATH,'//select[@id="select_year"]').click()
                    sel = Select(driver.find_element(By.XPATH,'//select[@id="select_year"]'))
                    time.sleep(5)
                    sel.select_by_value(fy_yr)
                    time.sleep(20)
                    ##-----------------------Done-------------------------------------

                    df = pd.read_html(driver.page_source, attrs = {'id': 'top-n-members-monthly-reference'}) # Added id attrs | Pushkar | 14 Mar 2024                    
                    # Added | Pushkar | 31 Jan 2024
                    cond = False
                    for i in df:
                        if all(i.iloc[0, :] == 'No Records'):
                            print('No Records found')
                            cond = True
                        else:
                            cond = False
                    
                    if cond != False:
                        df = pd.DataFrame()
                    else:
                    # Addition ends
                        df = df[0]
                        df.columns = ['Relevant_Date', 'Top_5', 'Top_10', 'Top_25', 'Top_50', 'Top_100']
                        df.reset_index(drop=True,inplace=True)

                        df = df[:10]  #take latest 10 months data
                        df['Comments'] = ''
                        df['Relevant_Date'] = df['Relevant_Date'].apply(lambda d:datetime.datetime.strptime(d,'%b-%y'))
                        df['Relevant_Date'] = df['Relevant_Date'].apply(lambda d:(pd.to_datetime(d) - MonthEnd(0)).date())
                        df['Runtime']       = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                        df['Last_Updated']  = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
                    break
                except:
                    limit += 1
                    time.sleep(10)
                    driver.delete_all_cookies()
                    driver.quit()
                    if limit >= 7:
                        print('7 iterations exhausted. Site loading issue')
                        break
        
            if len(df) != 0:
                print(df)
                df = df[df['Relevant_Date'] > max_date]
                df.to_sql(name='NSE_EQUTY_PCT_SHARE_OF_TOP_CM_MONTHLY_DATA',con=engine,if_exists='append',index=False)
                print('To SQL | Rows: ', len(df))
        
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except :
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
