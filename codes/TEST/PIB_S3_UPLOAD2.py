import warnings
warnings.filterwarnings('ignore')
import os
import re
import sys
import requests
import time
import pandas as pd
from pytz import timezone
import datetime as datetime
from bs4 import BeautifulSoup
from dateutil import relativedelta, parser

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import boto3
import os
import ntpath
from botocore.config import Config

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

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

    return x.replace("'", "")


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

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

        chrome_options = Options()
        prefs = {
            "printing.print_preview_sticky_settings.appState": '{"recentDestinations":[{"id":"Save as PDF","origin":"local","account":""}],"selectedDestinationId":"Save as PDF","version":2}',
            "savefile.default_directory": r"C:\Users\Administrator\AdQvestDir\PIB Corpus2",  # Adjust as needed
            "printing.default_destination_selection_rules": {
                "kind": "local",
                "idPattern": "Save as PDF",
            },
        }

        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--kiosk-printing")

        key1='PIB_CORPUS/'
        key2='PIB_CORPUS_TO_BE_CHUNKED/'
        path=r'C:/Users/Administrator/AdQvestDir/PIB Corpus2/'

        df_links=pd.read_sql("select * from AdqvestDB.PIB_REPORTS_DAILY_DATA where S3_Upload='Failed'",engine)

        df_links=df_links[18000:]

        for i, row in df_links.iterrows():
            try:

                for file in os.listdir(path):
                    if file.endswith('.pdf'):
                        os.remove(path+file)
                file_name=row['File_Name']
                print("File Name : ", file_name)
                link= row['Links']
                service = Service(r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                driver = webdriver.Chrome(service=service, options=chrome_options)

                try:
                    # Navigate to the website
                    driver.get(link)
                    wait = WebDriverWait(driver, 5)

                    # Wait for and click the print button
                    print_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Print1_print"]')))
                    print_button.click()

                    # Wait for a short duration to ensure print action completes
                    time.sleep(5)  # Headless doesn't show print dialogs; saving happens silently

                finally:
                    driver.quit()

                os.rename(path+os.listdir(path)[0],path+file_name)
                time.sleep(2)
                data = open(path + file_name,'rb')
                dbfunctions.to_s3bucket(path+file_name, key1)
                data.close()

                data = open(path + file_name,'rb')
                dbfunctions.to_s3bucket(path+file_name, key2)
                data.close()
                os.remove(path+file_name)

                connection=engine.connect()
                connection.execute("update PIB_REPORTS_DAILY_DATA set S3_Upload = 'Done' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")
            except:

                connection=engine.connect()
                connection.execute("update PIB_REPORTS_DAILY_DATA set S3_Upload = 'Failed' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")




        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
