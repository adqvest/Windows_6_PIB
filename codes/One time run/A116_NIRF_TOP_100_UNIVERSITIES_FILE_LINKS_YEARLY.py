import warnings
warnings.filterwarnings('ignore')
import os
import re
import sys
import time
import requests
import datetime
import numpy as np
import pandas as pd
from pytz import timezone
from bs4 import BeautifulSoup
from selenium import webdriver
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import dbfunctions
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = "NIRF_TOP_100_UNIVERSITIES_FILE_LINKS_YEARLY"

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        headers = {'authority': 'www.nirfindia.org',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

        wd = 'C:\\Users\\Administrator\\Junk\\NIRF\\'

        max_status_date = pd.read_sql('SELECT MAX(Relevant_Date) as max from NIRF_TOP_100_UNIVERSITIES_FILE_LINKS_YEARLY', con = engine)['max'][0]        
        r = requests.get('https://www.nirfindia.org/Home',headers = headers,timeout=60)
        soup = BeautifulSoup(r.content)
        years = [i.text for i in soup.findAll('a') if 'ranking' in i['href'].lower()]
        hrefs = ['https://www.nirfindia.org' + i['href'] for i in soup.findAll('a') if 'ranking' in i['href'].lower()]
        
        for yr, lnk in zip(years[:-1], hrefs[:-1]):
            df = pd.DataFrame()
            File_Name = []
            Link = []
            Institute_ID = []
            Relevant_Date = []
            if int(yr) > max_status_date.year:
                print(yr, lnk)
                r = requests.get(lnk.replace('Ranking.html', 'UniversityRanking.html'),headers = headers,timeout=60)
                soup = BeautifulSoup(r.content)
                a_tag = [i for i in soup.find_all('a',href=True) if '.pdf' in i['href']]
                links = [i['href'] for i in a_tag]

                print(len(links))
                try:
                    os.mkdir(wd + yr)
                except:
                    pass
                os.chdir(wd + yr)
                key = f'NIRF/{yr}/'
                
                for link in links:
                    file = os.path.basename(link)
                    print(file)
                    r1 = requests.get(link,headers,timeout=60)
                    with open(file,'wb') as f:
                        f.write(r1.content)
                        f.close()
                    File_Name.append(file.replace('.pdf', '.xlsx'))
                    Link.append(link)
                    Institute_ID.append(file.replace('.pdf', '').strip())
                    Relevant_Date.append(datetime.date(int(yr), 3, 31))
                df['File_Name'] = File_Name
                df['Link'] = Link
                df['Institute_ID'] = Institute_ID
                df['Relevant_Date'] = Relevant_Date
                df['Runtime'] = datetime.datetime.now(india_time)
                df.to_sql('NIRF_TOP_100_UNIVERSITIES_FILE_LINKS_YEARLY',index=False, if_exists='append', con = engine)

                os.chdir(wd + yr)
                files = os.listdir()
                pdf = [i.replace('.pdf','') for i in files if '.pdf' in i]
                excel = [i.replace('.xlsx','') for i in files if '.xlsx' in i]
                remaining_files = list(set(pdf)-set(excel))
                remaining_files = [i+'.pdf' for i in remaining_files]
                len(remaining_files)
                download_file_path = wd + yr + '\\'
                chrome_driver_path = 'C:/Users/Administrator/AdQvestDir/chromedriver.exe'
                i = 0 

                for file_name in remaining_files:
                    prefs = {'download.default_directory': download_file_path}
                    chrome_options = webdriver.ChromeOptions()
                    chrome_options.add_experimental_option('prefs', prefs)
                    driver = webdriver.Chrome(executable_path=chrome_driver_path,options=chrome_options)
                    
                    driver.get("https://www.ilovepdf.com/")
                    driver.find_element("xpath", "//*[contains(text(),'Login')]").click()
                    email = driver.find_element("xpath", "//*[@id='loginEmail']")
                    email.send_keys("kartmrinal101@outlook.com")
                    password = driver.find_element("xpath", "//*[@id='inputPasswordAuth']")
                    password.send_keys("zugsik-zuqzuH-jyvno4")
                    time.sleep(3)
                    driver.find_element("xpath", "//*[@id='loginBtn']").click()
                    time.sleep(10)
                    driver.find_element("xpath", "//*[contains(text(),'PDF to Excel')]").click()
                    time.sleep(3)
                    input_element = driver.find_element("xpath", "//*[@type='file']")
                    time.sleep(5)
                    os.chdir(wd + yr)
                    input_element.send_keys(download_file_path + file_name)
                    time.sleep(10)
                    driver.find_element("xpath", "//*[@id='processTask']").click()
                    time.sleep(10)
                    driver.close()
                    print('Done for :',file_name)
                    filepath = download_file_path + file_name.replace('.pdf', '.xlsx')
                    print(filepath)
                    dbfunctions.to_s3bucket(filepath, key)
                    os.remove(download_file_path + file_name)
                    i += 1
                    if i % 5 == 0:
                        time.sleep(30)
                    else:
                        pass
                    # os.remove(wd + '\\' + file_name)
            else:
                print('No New Data')
        log.job_end_log(table_name,job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')