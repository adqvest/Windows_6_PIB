import pandas as pd
#import camelot
import warnings
warnings.filterwarnings('ignore')
from dateutil import parser
import numpy as np
import sqlalchemy
from pandas.io import sql
from bs4 import BeautifulSoup
import calendar
import datetime as datetime
from pytz import timezone
import requests
import sys
import time
import os
import re
import sqlalchemy
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from pandas.core.common import flatten
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
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


        chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\Junk"

        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }


        options = webdriver.ChromeOptions()
        option = Options()
        option.add_argument("--disable-infobars")
        option.add_argument("start-maximized")
        option.add_argument("--disable-extensions")
        option.add_argument("--disable-notifications")
        option.add_argument('--ignore-certificate-errors')
        option.add_argument('--no-sandbox')


        options.add_experimental_option('prefs', prefs)
        #driver = webdriver.Chrome(executable_path=chrome_driver_path,chrome_options=options,options=option)
        driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=option)
        #driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = options)
        driver.get(url='https://www.icra.in/Rationale/Index')
        robot.add_link('https://www.icra.in/Rationale/Index')
        links=[]
        text=[]
        names=[]
        dates=[]

        max_date = pd.read_sql("SELECT max(Relevant_Date) as Max from ICRA_DAILY_FILES_LINKS",engine)['Max'][0]
        start_date = max_date + days
        end_date = yesterday.date() - 2*days
        print(end_date)

        while start_date < end_date:

            start_date_1 = start_date.strftime('%d %b %Y')
            time.sleep(16)

            start = driver.find_element(By.XPATH, '//*[@id="FromDate"]')
            time.sleep(4)
            start.clear()
            start.send_keys(start_date_1)
            time.sleep(2)
            driver.find_element(By.XPATH, '//*[@id="ToDate"]').click()
            time.sleep(2)
            end = driver.find_element(By.XPATH, '//*[@id="ToDate"]')
            end.clear()
            end.send_keys(start_date_1)
            time.sleep(1)
            driver.find_element(By.XPATH, '//*[@id="btnSearch"]').click()

            time.sleep(5)

            while True:


                time.sleep(10)
                soup=BeautifulSoup(driver.page_source)
                print('found soup')

                address1=soup.find_all("div",{"class":"list-nw"})
                if address1 != []:
                    print('found content')
                    for i in address1:
                        links.append(i.find("a",{"class":"size-sm-nw"}).get('href'))
                        text.append(i.find("span",{"data-bind":re.compile(r'text: CompanyName')}).get_text())
                        dates.append(i.find("span",{"class":"size-sm"}).get_text())
                else:
                    print('None found')
                    break

                print('Done')


                try:
                    driver.find_element(By.LINK_TEXT, '»').click()
                    print('Next page')
                    time.sleep(4)
                    htmlelement= driver.find_element(By.TAG_NAME,'html')
                    htmlelement.send_keys(Keys.HOME)
                    time.sleep(2)
                    continue
                except:
                    break

            start_date += days

        driver.close()

        try:
            print(dates)
            final_dates=[]
            operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
            for i in range(len(dates)):
                month = re.search(operation, dates[i].strip()).group(0)
                year=re.findall(r'\d+',dates[i].strip())[1]
                day=re.findall(r'\d+',dates[i].strip())[0]
                try:
                    month_number = datetime.datetime.strptime(month, '%B').month
                except:
                    month_number = datetime.datetime.strptime(month, '%b').month
                final_dates.append(year+'-'+str(month_number)+'-'+day)

            final_links=['https://www.icra.in/'+link if 'icraanalytics' not in link else link for link in links]
            robot.add_link(final_links[0])

            df=pd.DataFrame()
            df['Company_Name']=text
            df['Links']=final_links
            df['Download_Status']=np.nan
            df['Relevant_Date']=final_dates
            df['Runtime']=datetime.datetime.now()
            df.insert(2,'File_Name',np.nan)
            df['File_Name']=df['Company_Name'].str.replace(' ','_')+'_'+df['Relevant_Date'].astype(str)+'.pdf'


            df['Relevant_Date']=pd.to_datetime(df['Relevant_Date']).dt.date
            df.to_sql("ICRA_DAILY_FILES_LINKS", index=False, if_exists='append', con=engine)
        except:
            print('No Data found')

        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line no " + str(sys.exc_info()[2].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
