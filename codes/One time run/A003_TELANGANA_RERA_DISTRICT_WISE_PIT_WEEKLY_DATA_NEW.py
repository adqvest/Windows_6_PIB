import pandas as pd
import os
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import sys
import warnings
import time
from requests_html import HTMLSession
warnings.filterwarnings('ignore')
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from statistics import mean
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
import warnings
from dateutil import parser
warnings.filterwarnings('ignore')
from adqvest_robotstxt import Robots
robot = Robots(__file__)

# code you want to evaluate
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def tableDataText(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.find_all(coltag)]
    rows = []
    trs = table.find_all('tr')
    headerow = rowgetDataText(trs[0], 'th')
    if headerow: # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs: # for every table row
        rows.append(rowgetDataText(tr, 'td') ) # data row
    return rows

def get_l2(link):
    url = link
    headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
    while True:
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                break
        except:
            continue
    soup = BeautifulSoup(r.content,'lxml')
    try:
        district = soup.select('label:contains(District)')[0].find_next('div').text.strip()
    except:
        district = None
    print(district)
    try:
        df = pd.read_html(r.text)
        df1 = [x for x in df if "number of apartment" in [y.lower() for y in x.columns]]
        if df1 != []:
            df1 = df1[0]
            total_apt = sum(df1[[x for x in df1.columns if x.lower()=='number of apartment'][0]])
            total_floors = len(set(df1[[x for x in df1.columns if 'floor' in x.lower()][0]]))
            total_booked = sum(df1[[x for x in df1.columns if 'booked' in x.lower()][0]])
            total_area = sum(df1[[x for x in df1.columns if 'saleable' in x.lower()][0]])
        else:
            total_apt = None
            total_floors = None
            total_booked = None
            total_area = None
            df2 = [x for x in df if "Percentage of Work" in [y for y in x.columns]]
        if df2!=[]:
            df2 = df2[0]
            percentage_work = mean(df2['Percentage of Work'])
        else:
            percentage_work = None
            df = pd.DataFrame({"Total_Apt":total_apt,
                            "Total_Floors":total_floors,
                            "Total_Booked":total_booked,
                            "Percentage_Completed":percentage_work,
                            "Total_Area_In_Sqmts":total_area,
                            "Total_Area_In_Sqft":total_area*10.7639,
                            "District":district},index=[0])
    except:
        df =  pd.DataFrame({"Total_Apt":None,
                        "Total_Floors":None,
                        "Total_Booked":None,
                        "Percentage_Completed":None,
                        "Total_Area_In_Sqmts":None,
                        "Total_Area_In_Sqft":None,
                        "District":None},index=[0])
    return df

def get_date(x):
  try:
    return parser.parse(x).date()
  except:
    return None

def run_program(run_by='Adqvest_Bot', py_file_name=None):
#    os.chdir('/home/ubuntu/AdQvestDir')
    os.chdir('C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        max_db_date=pd.read_sql('select max(Relevant_Date) as MAX from TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA',engine)['MAX'][0]
        tdy=today.date()
        print(tdy)
        day=(tdy - max_db_date).days
        print(day)

        if day >= 7:
            url = 'http://rerait.telangana.gov.in/SearchList/Search'            
            robot.add_link(url)
            driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
            prefs = {"download.prompt_for_download": False,
                    "download.directory_upgrade": True}
            options = webdriver.ChromeOptions()
            options.add_argument("start-maximized")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument("--disable-blink-features")
            options.add_argument('--incognito')
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
            
            delay = 3 
            while True:
                try:
                    driver.get(url)
                    WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[contains(text(), "Telangana State Real Estate Regulatory Authority")]')))
                    print("Page is ready!")
                    break
                except TimeoutException:
                    pass
            total_records = int(driver.find_element(By.XPATH, '//input[@id="TotalRecords"]').get_attribute('value').strip())
            

            while True:
                try: 
                    driver.find_element(By.XPATH, '//input[@id="PageSize"]').clear()
                    driver.find_element(By.XPATH, '//input[@id="PageSize"]').send_keys(total_records)
                    WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[contains(text(), "Telangana State Real Estate Regulatory Authority")]')))
                    time.sleep(5)
                    break
                except:
                    pass

            driver.quit()
            links = []
            raw_df = pd.read_html(driver.page_source, extract_links='body')[0]
            links = raw_df['View Details'].apply(lambda x: x[1])
            raw_df = raw_df.applymap(lambda x: x[0])
            raw_df['View Details'] = links
            raw_df['View Details'] = "http://rerait.telangana.gov.in" + raw_df['View Details']
            sub = pd.DataFrame()
            for _, row  in raw_df.iterrows():
                url = row['View Details']
                robot.add_link(url)
                l2 = get_l2(url)
                row = row.to_frame().T
                row = pd.concat([row.reset_index(drop=True),l2],axis=1)
                sub = pd.concat([sub,row])
                    
            df = sub
            print(df.iloc[:,0])
            df['Last Modified Date'] = df['Last Modified Date'].apply(lambda x : get_date(x))
            df['Relevant_Date'] = today.date()
            df['Runtime'] = datetime.datetime.now()
            df = df[[x for x in df.columns if x != 'View_Details']]
            df.columns = [i.strip().replace(' ','_') for i in df.columns]
            df = df.rename(columns={'Sr_No.': 'Sr_No'})
            df = df[['Sr_No','Project_Name','Promoter_Name','Last_Modified_Date','Total_Apt','Total_Floors','Total_Booked','Percentage_Completed','Total_Area_In_Sqmts','Total_Area_In_Sqft','District','Relevant_Date','Runtime']]
            df = drop_duplicates(df)
            engine = adqvest_db.db_conn() ##Added by Nidhi on 11-7-23
            connection = engine.connect()  ##Added by Nidhi on 11-7-23
            assert total_records == len(df), f'Collected {len(df)} out of {total_records} records'

            df.to_sql(name='TELANGANA_RERA_DISTRICT_WISE_PIT_WEEKLY_DATA',con=engine,if_exists='append',index=False)
        else:
            print('Not in date interval')
        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')