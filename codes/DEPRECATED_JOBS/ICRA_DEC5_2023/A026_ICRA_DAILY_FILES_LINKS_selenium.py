import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from bs4 import BeautifulSoup
from pytz import timezone
import sys
import time
import datetime
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import dbfunctions
import adqvest_db
import JobLogNew as log
import MySql_To_Clickhouse as MySql_CH
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def rename_file(file_name):
    files = os.listdir('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\')
    print(files)

    # Check if the file with the expected name is present
    if "FileName" in files[0]:
        print('renaming')
        # Rename the file
        original_path = os.path.join('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\', "FileName.pdf")
        new_path = os.path.join('C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\', f"{file_name}")
        os.rename(original_path, new_path)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    engine = adqvest_db.db_conn()
    job_start_time = datetime.datetime.now(india_time)
    #add table name
    table_name = 'ICRA_DAILY_FILES_LINKS'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        query="select max(Relevant_Date) as Max from ICRA_DAILY_FILES_LINKS"
        db_max_date = pd.read_sql(query,engine)["Max"][0]

        if db_max_date < yesterday.date():

            from_date = db_max_date + datetime.timedelta(1)
           
            prefs = {
            "download.default_directory": 'C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\',
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
            options.add_experimental_option('prefs', prefs)
            chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

            driver = webdriver.Chrome(executable_path=chrome_driver_path,options = options)
            url = 'https://www.icra.in/Rating/AllRatingRationales'
            driver.get(url)
            robot.add_link(url)
            driver.find_element(By.XPATH, '//input[@id = "ratingFromDate"]').click()
            driver.find_element(By.XPATH, '//select[@data-handler = "selectMonth"]').click()
            driver.find_element(By.XPATH, f'//select[@data-handler = "selectMonth"]/option[@value = "{from_date.month - 1}"]').click()

            driver.find_element(By.XPATH, '//input[@id = "ratingFromDate"]').click()
            driver.find_element(By.XPATH, '//select[@data-handler = "selectYear"]').click()
            driver.find_element(By.XPATH, f'//select[@data-handler = "selectYear"]/option[@value = "{from_date.year}"]').click()

            driver.find_element(By.XPATH, f'//td[@data-handler = "selectDay"]/a[text() = {from_date.day}]').click()


            driver.find_element(By.XPATH, '//input[@id = "ratingToDate"]').click()
            driver.find_element(By.XPATH, '//select[@data-handler = "selectMonth"]').click()
            driver.find_element(By.XPATH, f'//select[@data-handler = "selectMonth"]/option[@value = "{yesterday.month - 1}"]').click()

            driver.find_element(By.XPATH, '//input[@id = "ratingToDate"]').click()
            driver.find_element(By.XPATH, '//select[@data-handler = "selectYear"]').click()
            driver.find_element(By.XPATH, f'//select[@data-handler = "selectYear"]/option[@value = "{yesterday.year}"]').click()

            driver.find_element(By.XPATH, f'//td[@data-handler = "selectDay"]/a[text() = {yesterday.day}]').click()

            driver.find_element(By.XPATH, '//span[text() = "Search"]').click()

            time.sleep(2)
            soup = BeautifulSoup(driver.page_source,'lxml')

            icra = pd.DataFrame(columns = ['File_Name','Links','Company_Name','Relevant_Date','Runtime'])

            try:
                last_page = int(soup.find('section',attrs = {'id':'AllRatingRationales'}).find('li',class_ = 'PagedList-skipToLast').find('a')['href'].split('=')[-1])
            except:
                last_page = 1

            for i in range(1,last_page+1):
                time.sleep(2)
                if last_page > 1:
                    try:
                        driver.find_element(By.XPATH, f'//a[text() = "{i}"]').click()
                    except:
                        time.sleep(2)
                        driver.find_element(By.XPATH, f'//a[text() = "{i}"]').click()
                time.sleep(1)
                soup = BeautifulSoup(driver.page_source,'lxml')
                ratings = soup.find('section',attrs = {'id':'AllRatingRationales'}).find_all('div',class_ = 'cpr_blurb')
                for rating in ratings:
                    os.chdir('C:\\Users/Administrator\\AdQvestDir\\ICRA_JUNK')
                    href = rating.find('a',class_ = 'all_rating_rationale_download')['href']
                    time.sleep(2)
                    driver.find_element(By.XPATH, f'//a[@href = "{href}"]').click()
                    time.sleep(2)
                    file_name = rating.find('p',class_ = 'text_ellipsis').text
                    r_date = datetime.datetime.strptime(rating.find('div',class_ = 'col-2 date').text.replace('\n','').strip(), '%d %b %Y').date()
                    link = 'https://www.icra.in'+rating.find('div',class_ = 'col-6 rep_det_con').find('a')['href']
                    company_name = file_name.split(':')[0]
                    file_name = company_name.strip().replace(' ','_')+'_' + str(r_date) + '.pdf'
                    print(file_name)
                    rename_file(file_name)
                    #upload to s3 and remove file
                    dbfunctions.to_s3bucket(f'C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\{file_name}','ICRA/')
                    engine.execute('update ICRA_DAILY_FILES_LINKS set Download_Status = "Yes",Comments=null where File_Name = "'+file_name+'"')
                    file_path = f'C:\\Users\\Administrator\\AdQvestDir\\ICRA_JUNK\\{file_name}'
                    os.remove(file_path)
                    
                    icra.loc[len(icra)] = [file_name,link,company_name,r_date,today]
                    
            icra.drop_duplicates(inplace=True)      
            icra.to_sql('ICRA_DAILY_FILES_LINKS', con=engine, if_exists='append', index=False)        
            MySql_CH.ch_truncate_and_insert('ICRA_DAILY_FILES_LINKS') 
        else:
            print('Data Upto Date')   

        log.job_end_log(table_name,job_start_time, no_of_ping)
        
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line no " + str(sys.exc_info()[2].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
