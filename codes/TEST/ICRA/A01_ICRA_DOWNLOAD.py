import pandas as pd
import datetime as datetime
from pytz import timezone
import time
import sys
import re
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import dbfunctions
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = "ICRA_DAILY_DATA_CORPUS_NEW"

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        df_links=pd.read_sql("select * from AdqvestDB.ICRA_DAILY_DATA_CORPUS where S3_Upload_Status IS Null",engine)
      
        # chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

        options = Options()
        prefs = {"printing.print_preview_sticky_settings.appState": '{"recentDestinations":[{"id":"Save as PDF","origin":"local","account":""}],"selectedDestinationId":"Save as PDF","version":2}',
            "savefile.default_directory": r"C:\Users\Administrator\AdQvestDir\Junk_ICRA", # Adjust as needed
            "printing.default_destination_selection_rules": {"kind": "local","idPattern": "Save as PDF",},}

        options.add_experimental_option("prefs", prefs)
        options.add_argument("--kiosk-printing")
        options.add_argument("--start-maximized")

        for i, row in df_links.iterrows():
            file_name=row['Generated_File_Name']
            link= row['Links']
            robot.add_link(link)
            path='C:/Users/Administrator/AdQvestDir/Junk_ICRA/'
            for file in os.listdir(path):
                if file.endswith('.pdf'):
                    os.remove(path+file)
            # service = Service(chrome_driver)  # Update with your chromedriver path
            # driver = webdriver.Chrome()
            driver = webdriver.Chrome(options=options)
            driver.get(link)
            time.sleep(45)

            try:
                
                download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "DownloadRatingReport")))
                download_button.click()
                print("Download button clicked successfully!")
                
                time.sleep(20)
                driver.switch_to.default_content()
                filename=[file for file in os.listdir(path) if file.endswith('.pdf')][0]
                os.rename(path+filename,path+file_name)

                '''S3 Upload'''

                # key1='ICRA_CORPUS/'
                key2='ICRA_CORPUS_TO_BE_CHUNKED_1/'
                # dbfunctions.to_s3bucket(path + file_name, key1)
                time.sleep(5)
                dbfunctions.to_s3bucket(path + file_name, key2)
                time.sleep(2)

                os.remove(path+file_name)

                connection=engine.connect()
                connection.execute("update ICRA_DAILY_DATA_CORPUS_NEW set S3_Upload_Status = 'Done' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")

            except Exception as e:
                connection=engine.connect()
                # connection.execute("update ICRA_DAILY_DATA_CORPUS_NEW set S3_Upload_Status = 'Failed' where File_Name = '" +str(row['File_Name'])+"'")
                # connection.execute("commit")
                connection=engine.connect()
                connection.execute(f"update ICRA_DAILY_DATA_CORPUS_NEW set S3_Upload_Comments = 'Failed' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")
                print(f"Error occurred: {e}")

            finally:
                driver.quit()
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')