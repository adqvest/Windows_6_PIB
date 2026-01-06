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

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = "SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS"

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        delete_path='C:/Users/Administrator/Downloads/'
        for file in os.listdir(delete_path):
            if file.endswith('.pdf'):
                os.remove(delete_path+file)
        
        df_links=pd.read_sql("select * from AdqvestDB.SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS where S3_Upload_Status is Null AND File_ID LIKE '%%BE%%' AND Relevant_Date > '2017-03-31' limit 20",engine)
        print(len(df_links))
        # df_links=pd.read_sql("select * from AdqvestDB.SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA where Relevant_Date>='2025-01-29'",engine)

        # chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

        chrome_options = Options()
        prefs = {
            "printing.print_preview_sticky_settings.appState": '{"recentDestinations":[{"id":"Save as PDF","origin":"local","account":""}],"selectedDestinationId":"Save as PDF","version":2}',
            "savefile.default_directory": r"C:\Users\Administrator\AdQvestDir\SEBI RHP CORPUS", # Adjust as needed
            "printing.default_destination_selection_rules": {
                "kind": "local",
                "idPattern": "Save as PDF",
            },
        }

        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--kiosk-printing")
        chrome_options.add_argument("--start-maximized")

        for i, row in df_links.iterrows():
            file_name=row['File_Name']
            link= row['File_Link']
            robot.add_link(link)
            path='C:/Users/Administrator/AdQvestDir/SEBI RHP CORPUS/'
            for file in os.listdir(path):
                if file.endswith('.pdf'):
                    os.remove(path+file)
            # service = Service(chrome_driver)  # Update with your chromedriver path
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(link)
            time.sleep(20)
            
            timeout = 60
            downloaded_file = None
            print("Clicking download...")
            
            download_dir = r"C:\Users\Administrator\Downloads"  
            target_dir   = r"C:/Users/Administrator/AdQvestDir/SEBI RHP CORPUS/"
           
            try:
                # Wait for the iframe to load and switch to it
                WebDriverWait(driver, 10).until(
                    EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//*[@id="member-wrapper"]/section[2]/div[1]/section/div[2]/div/iframe')))

                time.sleep(10)

                element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//*[@id='download']")))
                element.click()
                time.sleep(120)
                
                driver.switch_to.default_content()
                
                for i in range(timeout):
                    pdf_files = [f for f in os.listdir(download_dir) if f.endswith(".pdf")]
                    if pdf_files :
                        pdf_paths = [os.path.join(download_dir, f) for f in pdf_files ]
                        pdf_paths.sort(key=os.path.getmtime)
                        downloaded_file = pdf_paths[-1]
                        break
                    
                    time.sleep(15)
                
                if not downloaded_file:
                    raise Exception("File did not download in 60 seconds.")
                
                os.rename(os.path.join(download_dir, downloaded_file),os.path.join(target_dir, file_name))
                print("File moved successfully.")
            
                new_file_path = os.path.join(target_dir, file_name)
                print(f"File moved successfully to: {new_file_path}")
                
                
                # filename=[file for file in os.listdir(path) if file.endswith('.pdf')][0]
                # os.rename(path+filename,path+file_name)

                '''S3 Upload'''

                key1='SEBI_RHP_DRHP_CORPUS_2/'
                key2='SEBI_RHP_DRHP_CORPUS_TO_BE_CHUNKED_2/'
                dbfunctions.to_s3bucket(path + file_name, key1)
                time.sleep(5)
                dbfunctions.to_s3bucket(path + file_name, key2)

                time.sleep(2)

                os.remove(path+file_name)
                print("Element clicked successfully!")

                connection=engine.connect()
                connection.execute("update SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS set S3_Upload_Status = 'Done' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")
                
                connection.execute("update SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS set S3_Upload_Comments = Null where S3_Upload_Status = 'Done'")
                connection.execute("commit")

            except Exception as e:
                connection=engine.connect()
                connection.execute("update SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS set S3_Upload_Comments = 'Failed' where File_Name = '" +str(row['File_Name'])+"'")
                connection.execute("commit")
                print(f"Error occurred: {e}")
            driver.quit() 
               
        df_links=pd.read_sql("select * from AdqvestDB.SEBI_FINAL_OFFER_DOCUMENTS_RHP_DRHP_DAILY_DATA_CORPUS where S3_Upload_Status is Null and (S3_Upload_Comments IS NULL OR S3_Upload_Comments != 'Failed')",engine)
        
        if len(df_links)> 2:
            raise Exception(f'There are {len(df_links)} files from links that are not downloaded')
                

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')