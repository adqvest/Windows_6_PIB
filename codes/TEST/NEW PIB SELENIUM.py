from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import time

import warnings
warnings.filterwarnings('ignore')
import re
import sys
from pytz import timezone

from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

engine = adqvest_db.db_conn()
connection = engine.connect()
# ****   Date Time *****
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
days = datetime.timedelta(1)
yesterday = today - days

chrome_version = driver.capabilities['browserVersion']
print(f"Chrome Browser Version: {chrome_version}")


def get_quarter(p_date):
    return (p_date.month - 1) // 3 + 1

def getEndQuarter(p_date):
    quarter = get_quarter(p_date)
    return datetime.date(p_date.year + 3 * quarter // 12, 3 * quarter % 12 + 1, 1) + datetime.timedelta(days=-1)

def getFYQtr(date):
    month=date.month
    year=date.year
    if month in (4, 5, 6):
        fin_quarter = 'Q1'
        year=year+1
    elif month in (7, 8, 9):
        fin_quarter = 'Q2'
        year=year+1
    elif month in (10, 11, 12):
        fin_quarter = 'Q3'
        year=year+1
    else:
        fin_quarter = 'Q4'

    return f"{fin_quarter}_FY{str(year)[2:]}"

def remove_single_quotes(x):
    val = x.replace("'", "").replace("-"," ").replace(',','')
    return val

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        driver.get('https://pib.gov.in/allRel.aspx')
        driver.refresh()
        time.sleep(5)
        driver.maximize_window()
        
        # Set date range
        end_date = datetime.datetime(2024, 1, 1).date()
        start_date = datetime.datetime(2025, 1, 2).date()
        
        all_data = []
        
        # Loop from start_date to end_date (backward)
        current_date = start_date
        while current_date >= end_date:
            day = str(current_date.day)
            month = str(current_date.month)
            year = str(current_date.year)
        
            # Select the dropdowns
            Select(driver.find_element(By.ID, "ContentPlaceHolder1_ddlday")).select_by_value(day)
            time.sleep(2)
            Select(driver.find_element(By.ID, "ContentPlaceHolder1_ddlMonth")).select_by_value(month)
            time.sleep(2)
            Select(driver.find_element(By.ID, "ContentPlaceHolder1_ddlYear")).select_by_value(year)
            time.sleep(5)  # Allow content to reload
        
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'lxml')
        
            content_area = soup.find('div', class_='content-area')
            if not content_area:
                print(f"No content found for {current_date}")
                current_date -= timedelta(days=1)
                continue
        
            all_sections = content_area.find_all('ul', recursive=False)
            for section in all_sections:
                ministry_tag = section.find('h3')
                if not ministry_tag:
                    continue
                ministry = ministry_tag.text.strip()
        
                links = section.find_all('a')
                for a in links:
                    file_name = a.text.strip()
                    href = a['href']
                    full_url = "https://pib.gov.in" + href
                    all_data.append({
                        'Ministry': ministry,
                        'Links_Text': file_name,
                        'Links': full_url,
                        'Relevant_Date': current_date
                    })
        
            print(f"✅ Processed {current_date}")
            current_date -= timedelta(days=1)
        
        driver.quit()
        
        df = pd.DataFrame(all_data)
        
        df = df.sort_values(by='Relevant_Date').reset_index(drop=True)
        
        existing_links = pd.read_sql("SELECT distinct Links as Links FROM PIB_REPORTS_DAILY_DATA_CORPUS ",engine)['Links'].tolist()
        
        df_new = df[~df['Links'].isin(existing_links)]
        if len(df_new) >0:
            
            max_index= pd.read_sql("select max(Index_No) as Max from AdqvestDB.PIB_REPORTS_DAILY_DATA_CORPUS",engine)['Max'][0]
            
            start_num=max_index+1
            unique_index=[i for i in range (start_num, start_num+len(df_new))]
            
            df_new['Index_No']=unique_index
            
            df_new = df_new.rename(columns={'File Name': 'File_Name'})
            
            df_new['Relevant_Quarter']=df_new['Relevant_Date'].apply(lambda x:getEndQuarter(x))
            df_new['Relevant_Quarter']=df_new['Relevant_Quarter'].apply(lambda x:getFYQtr(x))
            df_new['Ministry']=df_new['Ministry'].apply(lambda x:remove_single_quotes(x))
            df_new['File_Name'] = (
                'Press_Information_Bureau_0_' +
                df_new['Ministry'].str.replace(' ', '_') + '_' +
                df_new['Relevant_Quarter'] + '_' +
                df_new['Index_No'].astype(str) + '.pdf'
            )
            
            df_new=df_new[['Index_No', 'Ministry', 'Links', 'Links_Text', 'File_Name', 'Relevant_Quarter', 'Relevant_Date']]
            
            df_new['Runtime']=datetime.datetime.now()
            
            df_new = df_new.drop_duplicates(subset = 'Links')
            
            if len(df_new) >0:
                df_new.to_sql(name = "PIB_REPORTS_DAILY_DATA_CORPUS",if_exists = 'append',con = engine,index = False)
            else:
                print('No data')
        else:
            print('Nothing to Push')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')