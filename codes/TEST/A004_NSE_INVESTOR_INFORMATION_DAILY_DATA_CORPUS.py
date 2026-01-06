# done by kama

import sys
import time
import os

import pandas as pd
from pytz import timezone
import datetime
import datetime as dt
import re

#seleniuum packages
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

pd.options.display.max_columns = None
pd.options.display.max_rows = None

#Adqvest packages
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

import warnings
warnings.filterwarnings('ignore')

def driver_1w_data(driver):
    
    wait = WebDriverWait(driver, 10)
    # Modified | Gokul | 3/3/25
    # Click on 1 week instead of 1 day
    data_1d = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[12]/div/div/section/div/div/div[2]/div/div[3]/div[1]/div[2]/div/div/div/div/ul/li[2]/a')))
    
    data_1d.click()
    time.sleep(5)

    return driver

def remove_existing_file(file_path):
    
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Existing file '{file_path}' removed.")

# Function to wait until file is downloaded
def wait_for_download(download_directory, timeout=60):
    
    seconds = 0
    while seconds < timeout:
        time.sleep(1)
        seconds += 1
        files = os.listdir(download_directory)
        if any(file.endswith('.csv') for file in files):
            return True
    return False

def sanitize_filename(filename):
    return filename.replace(':', '_')
        
def company_clean(df):
    
    df['Company_Name']=df['Company_Name'].str.replace("'s","s").str.replace('Ltd','Limited').str.replace('Ltd.','Limited').str.replace('ltd','Limited').str.replace('ltd.','Limited').str.replace('.','')
    
    df['Company_Name']=df['Company_Name'].str.title()    
    return df

def get_relevant_quater(date):   
    relevant_year = date
    if relevant_year.month >=7 and relevant_year.month <= 9:
        relevant_year = f"Q1 FY{str(relevant_year.year+1)[2:]}"
    elif relevant_year.month >=10 and relevant_year.month <= 12:
        relevant_year = f"Q2 FY{str(relevant_year.year+1)[2:]}"
    elif relevant_year.month >=1 and relevant_year.month <= 3:
        relevant_year = f"Q3 FY{str(relevant_year.year)[2:]}"
    elif relevant_year.month >=4 and relevant_year.month <= 6:
        relevant_year = f"Q4 FY{str(relevant_year.year)[2:]}"
    else:
        pass
    return relevant_year

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    
    india_time = timezone('Asia/Kolkata')

    job_start_time = dt.datetime.now(india_time)
    table_name = "NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS"
    scheduler = ''
    no_of_ping = 0

    if py_file_name is None:
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if run_by == 'Adqvest_Bot':
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)    

        download_directory = "C:\\Users\\Administrator\\AdQvestDir\\Junk_One_Time"
        final_file_name = "NSE_file.csv"
        final_file_path = os.path.join(download_directory, final_file_name)
        print(download_directory)
        
        import tempfile

        # Create a temporary clean profile
        user_data_dir = tempfile.mkdtemp()
        
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}") 

        prefs = {'download.default_directory': download_directory}
        chrome_options.add_experimental_option('prefs', {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "profile.default_content_settings.popups": 0,
    "safebrowsing.enabled": True
})

        # Initialize the undetected Chrome driver with options
        driver = uc.Chrome(options=chrome_options)
        driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
        driver.execute_cdp_cmd('Network.clearBrowserCache', {})

        driver.delete_all_cookies()  # Optional, still useful
        driver.maximize_window()

        driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
        robot.add_link("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
        wait = WebDriverWait(driver, 10)

        try:
            driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
            wait = WebDriverWait(driver, 10)
            time.sleep(10)
            # time.sleep(5)
        except:
            driver.quit()
            driver = uc.Chrome()
            driver.maximize_window()
            driver.get("https://www.nseindia.com/companies-listing/corporate-filings-announcements")
            wait = WebDriverWait(driver, 10)
            time.sleep(10)
        driver=driver_1w_data(driver)
        wait = WebDriverWait(driver, 10)

        download_link = wait.until(EC.element_to_be_clickable((By.ID, 'CFanncEquity-download')))

        download_link.click()

        time.sleep(3)

        # Get the current windows and switch to the new one (if a new window opens)
        windows = driver.window_handles
        driver.switch_to.window(windows[-1])
        
        remove_existing_file(final_file_path)
        
        time.sleep(2)
            
        if wait_for_download(download_directory):
            
            # Get the most recent downloaded file
            files = os.listdir(download_directory)
            full_file_paths = [os.path.join(download_directory, f) for f in files]

            latest_file = max(full_file_paths, key=os.path.getctime)
            print(files)
            
            # Rename the downloaded file to final file name
            time.sleep(5)
            os.rename(os.path.join(download_directory, latest_file), final_file_path)
            print(f"File successfully downloaded and renamed as '{final_file_path}'.")

        else:
            print("Download failed or timeout.")
       
        driver.quit()
        time.sleep(3)
      
        df_table=pd.read_csv('C:\\Users\\Administrator\\AdQvestDir\\Junk_One_Time\\NSE_file.csv')        
        # df_table = df_table.drop(columns=['FILE SIZE'])

        #Checking whether data is there or not
        if len(df_table)>1:
            df_table.columns=['Symbol','Company_Name','Subject','Details','Broadcast_Date','Receipt','Dissemination','Difference','File_Link']
            # df_table=df_table[['Symbol','Company_Name','Type','Details','Broadcast_Date','Links']]
            df_table=df_table[~df_table['File_Link'].str.contains('.zip')]
            
            df_table['Broadcast_Date']=pd.to_datetime(df_table['Broadcast_Date'])
            df_table['Receipt']=pd.to_datetime(df_table['Receipt'])
            df_table['Dissemination'] = pd.to_datetime(df_table['Dissemination'], errors='coerce')
            df_table['Difference'] = pd.to_datetime(df_table['Difference'], errors='coerce')
            df_table['Relevant_Date'] = df_table['Broadcast_Date'].dt.date
            df_table['Runtime']=pd.to_datetime('now')
            df_table=company_clean(df_table)
            df_table['Relevant_Quarter']=df_table['Broadcast_Date'].apply(get_relevant_quater)
            
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            file_id=pd.read_sql('select max(File_ID) as max from NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS WHERE File_ID like "O%%"',engine)['max'][0]
            
            numeric_str = re.sub(r'\D', '', file_id)  # removes all non-digit characters
            numeric_part = int(numeric_str)
            
            df_table['File_ID'] = ['O' + str(i) for i in range(numeric_part + 1, numeric_part + 1 + len(df_table))]
            
            df_table = df_table[df_table['File_Link'].str.contains('.pdf', case=False)].reset_index(drop=True)

            df_table = df_table.drop_duplicates(subset='File_Link', keep='first')
            print(df_table.head())
            
            df_table['File_Name'] = (
                df_table['Company_Name'].str.replace(r'[ /]', '_', regex=True) + '_0_' +
                df_table['Subject'].str.replace(r'[ /]', '_', regex=True) + '_' +
                df_table['Relevant_Quarter'].str.replace(r'[ /]', '_', regex=True) + '_' +
                df_table['File_ID']+ '.pdf'
            )
            
            #Handling duplicates
            # Modified | Gokul | 3/3/25
            # Takes previous date linnks and filters out the duplicates
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            max_broadcast_date = pd.read_sql("SELECT MAX(Broadcast_Date) as Max FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS", engine)['Max'][0]
            df_table = df_table[df_table['Broadcast_Date'] >= max_broadcast_date]
            
            previous_broadcast_date = (max_broadcast_date - pd.Timedelta(days=1))
            existing_links = pd.read_sql(f"SELECT File_Link FROM NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS WHERE Broadcast_Date >= '{previous_broadcast_date}'",engine)['File_Link'].tolist()
            
            df_table = df_table[~df_table['File_Link'].isin(existing_links)]
            df_table = df_table.sort_values(by='Broadcast_Date', ascending = True)
            print(df_table)
            #############################
            
            engine = adqvest_db.db_conn()
            connection = engine.connect()
            df_table = df_table.drop_duplicates(subset='File_Link', keep='first')
            # df_table.to_sql('NSE_INVESTOR_INFORMATION_DAILY_DATA_CORPUS',index=False, if_exists='append', con=engine)
           
        else:
            print('No new data')

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'", str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name, job_start_time, error_type, error_msg, no_of_ping)
if (__name__ == '__main__'):
    run_program(run_by='manual')
