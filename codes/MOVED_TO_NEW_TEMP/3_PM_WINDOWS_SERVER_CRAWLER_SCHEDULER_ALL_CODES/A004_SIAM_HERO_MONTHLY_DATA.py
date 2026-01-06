import datetime as datetime
import os
import re
import sys
import time
import warnings
import pandas as pd
import requests
from pytz import timezone

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings('ignore')
import numpy as np
import camelot
from dateutil.relativedelta import *
import boto3
from botocore.config import Config
from calendar import monthrange
from requests_html import HTMLSession

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions
import time
from dateutil.relativedelta import relativedelta

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')

import adqvest_db
import JobLogNew as log
import MySql_To_Clickhouse as MySql_CH
import adqvest_s3
import ClickHouse_db
from adqvest_robotstxt import Robots
robot = Robots(__file__)

def get_data(single_date):
    year_from = str(single_date.strftime('%Y'))
    month_from = str(single_date.strftime('%b'))
    day_from = str(single_date.strftime('%d')).lstrip('0')

    year_to = str(single_date.strftime('%Y'))
    month_to = str(single_date.strftime('%b'))
    day_to = str(single_date.strftime('%d')).lstrip('0')

    chrome_driver = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
    os.chdir(r'C:\Users\Administrator\SIAM')
    path=os.getcwd()
    download_file_path = path

    prefs = {
        "download.default_directory": download_file_path,
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
    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

    driver.maximize_window()
    url='https://www.bseindia.com/stock-share-price/hero-motocorp-ltd/heromotoco/500182/corp-announcements/'
    driver.get(url)

    robot.add_link(url)
    driver.implicitly_wait(10)

    driver.execute_script("window.scrollTo(0, window.scrollY + 400)")
    #----inputing from_date and to_date
    date=driver.find_element(By.XPATH,"//*[@id='txtFromDt']")
    time.sleep(5)
    date.click()

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_from)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_from)
    time.sleep(2)

    from_day = driver.find_element('xpath',"//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_from+"']")
    from_day.click()
    time.sleep(5)

    #---to date--

    date=driver.find_element(By.XPATH,"//*[@id='txtToDt']")
    time.sleep(5)
    date.click()

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-month"]'))
    time.sleep(2)
    sel.select_by_visible_text(month_to)
    time.sleep(2)

    sel = Select(driver.find_element("xpath",'//*[@class="ui-datepicker-year"]'))
    time.sleep(2)
    sel.select_by_visible_text(year_to)
    time.sleep(2)

    to_day = driver.find_element("xpath","//td[not(contains(@class,'ui-datepicker-month'))]/a[text()='"+day_to+"']")
    to_day.click()
    time.sleep(5)

    elem = driver.find_element("xpath",'//*[@id="btnSubmit"]')
    elem.click()
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html')
    data=pd.read_html(driver.page_source)
    driver.close()

    return soup,data

def end_date(date):
    end_dt = datetime.datetime(date.year, date.month, monthrange(date.year, date.month)[1]).date()
    return end_dt

def create_table(links, pdf, date,file_name,engine):
    links_table = pd.DataFrame()
    links_table['File_Name'] = pdf
    links_table['File_Name_Reference'] = file_name
    links_table['Link'] = links
    links_table['Status'] = np.nan
    links_table['Download_Status'] = np.nan
    links_table['Comments'] = np.nan
    links_table['Relevant_Date'] = date
    links_table['Runtime'] = datetime.datetime.now()
    links_table["File_Name_Reference"] = links_table["File_Name_Reference"].apply(lambda x: x + ".pdf")
    max_rel_date=pd.read_sql("SELECT max(Relevant_Date) as Max FROM AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS where File_Name_Reference like '%%hero%%'",engine)['Max'][0]
    links_table = links_table[links_table["Relevant_Date"]> max_rel_date]
    if(links_table.empty == False):
        print(links_table)
        links_table.to_sql(name='BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS', con=engine, if_exists='append', index=False)

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    client = ClickHouse_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'BSE_SIAM_AUTO_MONTHLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        today=datetime.datetime(2024, 5, 2)
        # max_rel_date=pd.read_sql("SELECT max(Relevant_Date) as Max FROM AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS where File_Name_Reference like '%%hero%%'",engine)['Max'][0]
        max_rel_date=pd.read_sql("SELECT max(Relevant_Date) as Max FROM AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA WHERE Company = 'HERO MOTOCORP'",engine)['Max'][0]
        max_rel_date=end_date(max_rel_date)+relativedelta(months=2)
        single_date=max_rel_date.replace(day=1)

        pdf=[]
        links=[]
        file_name=[]
        relevant_date=[]

        while today.date() >= single_date:
            soup,data=get_data(single_date)
            text_data=[]
            links_data=[]
            text=soup.find_all('span',{'class':'ng-binding'})[:]

            for t in text:
                t=t.text
                if 'Announcement under Regulation 30 (LODR)-Press Release / Media Release' in t:
                    text_data.append(t)

            link=soup.find_all('a',{'class':'tablebluelink'})[1].get('href')
            links_data.append(link)        
                    

            # for i in text:
            #     text_data.append(i.get_text())

            # link=soup.find_all('a',{'class':'tablebluelink'})[1:]

            # for i in link:
            #     links_data.append(i['href'])
            cond = False
            for text,link in zip(text_data,links_data):
                if 'Announcement under Regulation 30 (LODR)-Press Release / Media Release' in text:
                    print("found")
                    cond = True
                    pdf.append(link.split("/")[-1])
                    dt2=end_date(datetime.datetime.strptime(datetime.datetime.strftime(single_date,'%Y%m%d'),'%Y%m%d')+relativedelta(months=-1))
                    file_name.append('Hero_'+datetime.datetime.strftime(dt2,'%Y%m%d'))
                    print(dt2)
                    relevant_date.append(dt2)
                    
                else:
                    print("not found")
                    # continue
            if cond == True:
                break
            else:
                
                time.sleep(5)
            single_date += datetime.timedelta(1)
        for link,rel_date in zip(pdf,relevant_date):
            rel_date=rel_date+relativedelta(months=1)
            if rel_date.year==2018:
                links.append('https://www.bseindia.com/xml-data/corpfiling/CorpAttachment/'+str(rel_date.year)+'/'+str(rel_date.month)+'/'+link)
            else:
                links.append('https://www.bseindia.com/xml-data/corpfiling/AttachHis/'+link)

        create_table(links,pdf,relevant_date,file_name,engine)

        headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 GTB7.1 (.NET CLR 3.5.30729)", "Referer": "http://example.com"}
        links = pd.read_sql('select File_Name,File_Name_Reference,Link,Relevant_Date from AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS where Download_Status is NULL and Comments is Null and File_Name_Reference like "%%hero%%"', engine)
        if (links.empty):
            print('no new data')
            pass
        else:
            for a, values in links.iterrows():
                file = values['File_Name']
                file_new=values['File_Name_Reference']
                link = values['Link']
                print(link)
                date = values['Relevant_Date']
                ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]
                BUCKET_NAME = 'adqvests3bucket'
                r = requests.get(link, verify=False,headers=headers, timeout = 60)
                robot.add_link(link)
                print(r.status_code)
                no_of_ping +=1
                with open(file, 'wb') as f:
                    f.write(r.content)
                    f.close()
                file_name = file
                os.rename(file, file_new)
                data = open(file_new, 'rb')
                s3 = boto3.resource(
                    's3',
                    aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=ACCESS_SECRET_KEY,
                    config=Config(signature_version='s3v4', region_name='ap-south-1')
                )
                # Uploading the file to S3 bucket
                s3.Bucket(BUCKET_NAME).put_object(Key='SIAM_DATA/' + file_new, Body=data)
                data.close()
                if os.path.exists(file_new):
                    os.remove(file_new)
                connection = engine.connect()
                connection.execute("UPDATE AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS set Download_Status='Yes' where Link='" + link + "'")


        links = pd.read_sql('select File_Name_Reference,Relevant_Date,Link from AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS where Status is NULL and Download_Status is not NULL and Comments is NULL and File_Name_Reference like "%%hero%%" order by Relevant_Date ',engine)
        if (links.empty):
            print('no new data')
            pass
        else:
            for a, values in links.iterrows():

                file = values['File_Name_Reference']
                date = values['Relevant_Date']
                link = values['Link']
                print(date)
                print(link)
            
                try:
                    hero=camelot.read_pdf(link,pages='1-end')

                except:
                    r=requests.get(link,verify=False,headers=headers, timeout = 60)
                    print(r.status_code)
                    file='hero_temp.pdf'
                    with open(file, 'wb') as f:
                        f.write(r.content)
                        f.close()
                
                    hero=camelot.read_pdf(file,pages='1-end')
                    # os.rename(file, file_new)    
                    os.remove(file)
                try:
                    df1=hero[0].df
                    start_index=df1[df1[0].str.lower().str.contains('motor')].index[0]
                    end_index=df1[df1[0].str.lower().str.contains('scooter')].index[0]

                    df2=df1.loc[[start_index,end_index]].iloc[:,:2]
                    df2.columns=['Sub_Segment','Value']
                    # hero_df=df2.copy()
                    df2['Company'] = 'HERO MOTOCORP'
                    df2['Segment'] = '2W'
                    df2['Model'] = None
                    df2['Sale_Production'] = 'Sales'
                    df2['Sale_Type'] = 'Overall'

                    start_index = df1[df1[0].str.lower().str.contains('domestic')].index[0]

                    df3 = df1.iloc[start_index:, :2]
                    df3.columns = ['Sale_Type', 'Value']
                    df3['Company'] = 'HERO MOTOCORP'
                    df3['Segment'] = '2W'
                    df3['Model'] = None
                    df3['Sale_Production'] = 'Sales'
                    df3['Sub_Segment'] = None

                    hero_df = pd.concat([df2, df3])
                    hero_df['Relevant_Date']=date
                    hero_df['Runtime']=datetime.datetime.now()
                    hero_df['Value']=hero_df['Value'].apply(lambda x:x.replace(',',''))
                    print(hero_df)
                    hero_df.to_sql('BSE_SIAM_AUTO_MONTHLY_DATA',index=False, if_exists='append', con=engine)
                    connection = engine.connect()
                    connection.execute("UPDATE AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS set Status='Yes' where Link='" + link + "'")
                    s3.Bucket(BUCKET_NAME).put_object(Key='SIAM_DATA/' + file_new, Body=data)
                except:
                    connection = engine.connect()
                    connection.execute(f'DELETE FROM AdqvestDB.BSE_SIAM_AUTO_MONTHLY_DATA_FILE_LINKS WHERE File_Name = "{file_name}"')
                    connection.execute('commit')

        MySql_CH.ch_truncate_and_insert('BSE_SIAM_AUTO_MONTHLY_DATA')

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
