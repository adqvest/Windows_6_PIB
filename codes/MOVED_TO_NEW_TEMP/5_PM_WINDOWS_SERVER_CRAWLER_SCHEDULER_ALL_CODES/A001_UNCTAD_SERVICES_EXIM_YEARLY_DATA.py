import pandas as pd
import os
import re
import datetime as datetime
from dateutil import relativedelta
from pytz import timezone
from zipfile import ZipFile 
import sys
import time
from selenium import webdriver
from py7zr import SevenZipFile
from selenium.webdriver.common.by import By
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import MySql_To_Clickhouse as sql_ch
import time
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'UNCTAD_SERVICES_EXIM_YEARLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = '5_PM_WINDOWS_SERVER_2_SCHEDULER'
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_date=pd.read_sql(f'select MAX(Relevant_Date) as MAX from {table_name}',engine)['MAX'][0]

        # if relativedelta.relativedelta(today.date(),max_date).years > 1:
        driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_path = r"C:\Users\Administrator\Junk\Unctad_exim"
        os.chdir(download_path)
        options = webdriver.ChromeOptions()
        prefs = {"download.default_directory": download_path, #Change default directory for downloads
                 "download.prompt_for_download": False, #To auto download the file
                 "download.directory_upgrade": True}
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(driver_path,chrome_options=options)
        driver.maximize_window()
        url = 'https://unctadstat.unctad.org/datacentre/dataviewer/shared-report/59c55c9c-528d-44c0-ba15-54a3046760d4'
        driver.get(url)
        robot.add_link(url)
        time.sleep(5)
        driver.find_element(By.XPATH,'//button[@title="Bulk dataset download"]').click()
        time.sleep(5)
        driver.find_element(By.XPATH, "//input[@class='me-2']").click()
        time.sleep(5)
        driver.find_element(By.XPATH, "//*[contains(text(), 'Download')]").click()
        time.sleep(8)
        for i in os.listdir(r"C:\Users\Administrator\Junk\Unctad_exim"):
            if '7z' in i:
                filename = i
                filepath = r"C:\Users\Administrator\Junk\Unctad_exim" + '\\' + i
        SevenZipFile(filepath, mode = 'r').extractall(r"C:\Users\Administrator\Junk\Unctad_exim")
        time.sleep(5)
        df = pd.read_csv(fr'C:\Users\Administrator\Junk\Unctad_exim\{filename.split(".7z")[0]}')

        df = df[['Year', 'Economy Label', 'Flow Label', 'Category', 'Category Label', 'US dollars at current prices in millions']]
        df_india = df[df['Economy Label'] == 'India']
        df_world = df[df['Economy Label'] == 'World']
        df = pd.concat([df_india, df_world], ignore_index=True)
        df['Relevant_Date'] = pd.to_datetime(df.Year.astype(str) + '-12-31').dt.date
        df = df.drop('Year', axis=1)
        df.columns = ["Economy_Label", "Flow_label", "Category_Cd",	"Category_Label", "USD_Mn_at_Current_Prices", 'Relevant_Date']
        map_df = pd.read_sql('Select * from UNCTAD_SERVICES_EXIM_MAPPING_ONE_TIME ORDER BY Relevant_Date desc;', con=engine)
        map_df = map_df.drop(['Relevant_Date','Runtime'], axis=1)

        final_df = pd.merge(df, map_df, on='Category_Cd', how='left')
        final_df = final_df[['Economy_Label', 'Flow_label', 'Category_Cd', 'Category_Label_x', 'Category', 'Sub_Category1', 'Sub_Category2', 'Sub_Category3', 'Sub_Category4', 'Sub_Category5','USD_Mn_at_Current_Prices','Relevant_Date']]
        final_df.columns = ['Economy_Label', 'Flow_label', 'Category_Cd', 'Category_Label', 'Category', 'Sub_Category1', 'Sub_Category2', 'Sub_Category3', 'Sub_Category4', 'Sub_Category5','USD_Mn_at_Current_Prices','Relevant_Date']
        final_df['Runtime'] = datetime.datetime.now(india_time)
        final_df = final_df[final_df.Relevant_Date > max_date]
        if len(final_df) != 0:
            final_df.to_sql(table_name, index=False, if_exists='append', con=engine)
        else:
            print('No New Data')
        os.remove(fr"C:\Users\Administrator\Junk\Unctad_exim\{filename.split('.7z')[0]}")
        os.remove(filepath)
        # else:
        #     print('No New Data')
        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
    