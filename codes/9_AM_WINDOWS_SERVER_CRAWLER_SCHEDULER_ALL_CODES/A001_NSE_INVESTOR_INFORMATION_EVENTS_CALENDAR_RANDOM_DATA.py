import sys
import time
import os

import pandas as pd
from pytz import timezone
import datetime
import datetime as dt
import re

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

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

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    # os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir(r'C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    
    india_time = timezone('Asia/Kolkata')

    job_start_time = dt.datetime.now(india_time)
    table_name = "NSE_INVESTOR_INFORMATION_EVENTS_CALENDAR_RANDOM_DATA"
    scheduler = ''
    no_of_ping = 0

    if py_file_name is None:
        py_file_name = sys.argv[0].split('.')[0]

    try:
        if run_by == 'Adqvest_Bot':
            log.job_start_log_by_bot(table_name, py_file_name, job_start_time)
        else:
            log.job_start_log(table_name, py_file_name, job_start_time, scheduler)    

        download_directory = r"C:\Users\Administrator\AdQvestDir\Junk_nse"
        final_file_name = "NSE_file.csv"
        final_file_path = os.path.join(download_directory, final_file_name)
        print(download_directory)
        
        chrome_options = uc.ChromeOptions()

        prefs = {'download.default_directory': download_directory}
        chrome_options.add_experimental_option('prefs', {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "profile.default_content_settings.popups": 0,
    "safebrowsing.enabled": True
})

        driver = uc.Chrome(options=chrome_options)
        driver.maximize_window()

        driver.get("https://www.nseindia.com/companies-listing/corporate-filings-event-calendar")
        robot.add_link("https://www.nseindia.com/companies-listing/corporate-filings-event-calendar")
        time.sleep(30)
        
        select_element = driver.find_element(By.ID, "eventCalender_equities_fourthComing")
        select = Select(select_element)
        time.sleep(15)
        
        select.select_by_visible_text("Last 3 Months")
        time.sleep(15)

        wait = WebDriverWait(driver, 10)

        download_link = wait.until(EC.element_to_be_clickable((By.ID, 'CFeventCalendar-download')))

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

        df_table=pd.read_csv(final_file_path)        

        #Checking whether data is there or not
        if len(df_table)>1:
            df_table.columns=['Symbol','Company_Name','Subject','Details','Broadcast_Date']
            
            df_table.columns = df_table.columns.str.strip()

            pattern = (
                        r'(Mar(ch)? 31|Jun(e)? 30|Sep(t)?(ember)? 30|Dec(ember)? 31), \d{4}'
                        r'|'
                        r'\b(30(th)?|31(st)?)\s*(of\s*)?(Mar(ch)?|Jun(e)?|Sep(t)?(ember)?|Dec(ember)?)\,?\s*\d{4}\b'
                    )

            filtered_df = df_table[df_table['Details'].str.contains(pattern, case=False, na=False, regex=True)]
            
            filtered_df = filtered_df[filtered_df['Subject'].str.contains('Financial Results', case=False, na=False)]

            filtered_df['Broadcast_Date'] = pd.to_datetime(filtered_df['Broadcast_Date'], format='%d-%b-%Y')
            
            filtered_df['Relevant_Date'] = filtered_df['Broadcast_Date'].dt.date
            filtered_df['Runtime']=pd.to_datetime('now')
            
            existing_df = pd.read_sql("SELECT Company_Name, Subject, Broadcast_Date FROM NSE_INVESTOR_INFORMATION_EVENTS_CALENDAR_RANDOM_DATA",engine)
            
            existing_df['Broadcast_Date'] = pd.to_datetime(existing_df['Broadcast_Date'])
            filtered_df['Broadcast_Date'] = pd.to_datetime(filtered_df['Broadcast_Date'], format='%d-%b-%Y')
            
            filtered_df1 = filtered_df.merge(existing_df, on=['Company_Name', 'Subject', 'Broadcast_Date'], how='left',indicator=True)

            # Step 3: Keep only the new rows (not in existing_df)
            filtered_df = filtered_df1[filtered_df1['_merge'] == 'left_only'].drop(columns=['_merge'])

            # Month mapping
            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'sept': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12,
            }

            quarter_months_pattern = r'(?:mar|march|jun|june|sep|sept|september|dec|december)'
            ordinal = r'(?:\s*(?:st|nd|rd|th))?'
            space_opt = r'\s*'

            def extract_ended_date(text):
                if not isinstance(text, str):
                    return None
                text = text.lower()

                # 1. ended / ending + Day–Month–Year
                pattern1 = rf'(?:ended|ending)(?:\s+(?:on|for|the|period|quarter|financial|year|month|half|and))*\s+(\d{{1,2}}){ordinal}{space_opt}({quarter_months_pattern})[.,]?{space_opt}(\d{{4}})'
                # 2. ended / ending + Month–Day–Year
                pattern2 = rf'(?:ended|ending)(?:\s+(?:on|for|the|period|quarter|financial|year|month|half|and))*\s+({quarter_months_pattern})[.,]?{space_opt}(\d{{1,2}}){ordinal}[.,]?{space_opt}(\d{{4}})'

                # 3. financial year / quarter + Day–Month–Year
                pattern3 = rf'(?:financial\s+year|quarter|year|half\s+year){space_opt}(\d{{1,2}}){ordinal}{space_opt}({quarter_months_pattern})[.,]?{space_opt}(\d{{4}})'
                # 4. financial year / quarter + Month–Day–Year
                pattern4 = rf'(?:financial\s+year|quarter|year|half\s+year){space_opt}({quarter_months_pattern})[.,]?{space_opt}(\d{{1,2}}){ordinal}[.,]?{space_opt}(\d{{4}})'

                patterns = [pattern1, pattern2, pattern3, pattern4]

                for pat in patterns:
                    m = re.search(pat, text)
                    if m:
                        g = m.groups()
                        try:
                            if g[0] in month_map:
                                # Month first
                                return datetime(int(g[2]), month_map[g[0]], int(g[1]))
                            else:
                                # Day first
                                return datetime(int(g[2]), month_map[g[1]], int(g[0]))
                        except:
                            return None
                return None


            def get_q_fy_format(date):
                if pd.isnull(date):
                    return ''
                m = date.month
                y = date.year
                if m in [4, 5, 6]: return f"Q1 FY{str(y+1)[-2:]}"
                if m in [7, 8, 9]: return f"Q2 FY{str(y+1)[-2:]}"
                if m in [10, 11, 12]: return f"Q3 FY{str(y+1)[-2:]}"
                return f"Q4 FY{str(y)[-2:]}"  # Jan-Mar


            filtered_df['ExtractedDate'] = filtered_df['Details'].apply(extract_ended_date)
            filtered_df['Relevant_Quarter'] = filtered_df['ExtractedDate'].apply(get_q_fy_format)

            # 4. Drop intermediate column if needed
            filtered_df.drop(columns=['ExtractedDate'], inplace=True) 
                        
            if len(filtered_df)>1:
                engine = adqvest_db.db_conn()
                filtered_df.to_sql('NSE_INVESTOR_INFORMATION_EVENTS_CALENDAR_RANDOM_DATA',index=False, if_exists='append', con=engine)
                print('Pushed to the table', len(filtered_df))
            else:
                print('Nothing new to push')    

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