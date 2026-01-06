
import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import zipfile
import re
import ast
import datetime as datetime
from pytz import timezone
import requests
from urllib.parse import quote
import numpy as np
import time
from calendar import monthrange
import sys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import json
import requests
from json import JSONDecoder
import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from pyxlsb import open_workbook as open_xlsb
import ssl

from selenium.webdriver.common.keys import Keys

import xlwings

from dateutil.relativedelta import relativedelta
from dateutil import parser

import boto3

from botocore.config import Config
from fiscalyear import *
import fiscalyear
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import Cleaner as cleaner
import JobLogNew as log
import adqvest_s3
from adqvest_robotstxt import Robots

robot = Robots(__file__)

#functions
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days
no_of_ping = 0

# job log details
job_start_time = datetime.datetime.now(india_time)



import io
import zipfile

from lxml import etree
from pandas import read_csv, to_numeric

def log_amc_wise_errors(error_type1,error_msg1,run):
    global job_start_time
    global no_of_ping
     
    relevant_date = job_start_time.strftime("%Y-%m-%d")
    job_end_time       = datetime.datetime.now(india_time)
    job_execution_time = (job_end_time - job_start_time).total_seconds()
    job_start_time1 = job_start_time.strftime("%Y-%m-%d %H:%M:%S")
     
    df=pd.DataFrame()
    df['Table_Name']=['AMC_MONTHLY_AAUM']
    df['Python_File_Name']=['A004_AMC_MONTHLY_AAUM']
    df['Scheduler']=['10_AM_WINDOWS_SERVER_2_SCHEDULER']
    df['Run_By']=run
    df['Schedular_Start_Time']=str(today.strftime('%Y-%m-%d %H:%M:%S'))
    df['Start_Time']=job_start_time1
     
    df['End_Time']=job_end_time.strftime("%Y-%m-%d %H:%M:%S")
    df['Execution_Time_Seconds']=job_execution_time
    df['No_Of_Ping']=no_of_ping
    df['Error_Type']=error_type1
     
    df['Error_Msg']=error_msg1
    df['Relevant_Date']=relevant_date
    df['Runtime']=datetime.datetime.now(india_time)
    df.to_sql('TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA', con=engine, if_exists='append', index=False)
     
def drop_duplicates(df):
    columns = df.columns.to_list()
    columns.remove('Runtime')
    df = df.drop_duplicates(subset=columns, keep='last')
    return df

def get_financial_year(dt):

    fiscalyear.setup_fiscal_calendar(start_month=4)
    if dt.month<4:
        yr=dt.year
    else:
        yr=dt.year+1
        
    fy = FiscalYear(yr)
    fystart = fy.start.strftime("%Y-%m-%d")
    fyend = fy.end.strftime("%Y-%m-%d")
    fy_year=str(fy.start.year)+'-'+str(fy.end.year)
    return fy_year


class XLSX:

    sheet_xslt = etree.XML('''
        <xsl:stylesheet version="1.0"
            xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
            xmlns:sp="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
            >
            <xsl:output method="text"/>
            <xsl:template match="sp:row">
               <xsl:for-each select="sp:c">
                <xsl:value-of select="parent::*/@r"/> <!-- ROW -->
                <xsl:text>,</xsl:text>
                <xsl:value-of select="@r"/> <!--REMOVEME-->
                <xsl:text>,</xsl:text>
                <xsl:value-of select="@t"/> <!-- TYPE -->
                <xsl:text>,</xsl:text>
                <xsl:value-of select="sp:v/text()"/> <!-- VALUE -->
               <xsl:text>\n</xsl:text>
               </xsl:for-each>
            </xsl:template>
        </xsl:stylesheet>
    ''')

    def __init__(self, file_path):
        self.ns = {
            'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
        }
        self.fh = zipfile.ZipFile(file_path)
        self.shared = self.load_shared()
        self.workbook = self.load_workbook()

    def load_workbook(self):
        # Load workbook
        name = 'xl/workbook.xml'
        root = etree.parse(self.fh.open(name))
        res = []
        for el in etree.XPath("//ns:sheet", namespaces=self.ns)(root):
            res.append({'Sheet_Name' : el.attrib['name'],
                        'Sheet_id' : el.attrib['sheetId']})
            # res[el.attrib['name']] = el.attrib['sheetId']
        return res

    def load_shared(self):
        # Load shared strings
        name = 'xl/sharedStrings.xml'
        root = etree.parse(self.fh.open(name))
        res = etree.XPath("/ns:sst/ns:si/ns:t", namespaces=self.ns)(root)
        return {
            str(pos): el.text
            for pos, el in enumerate(res)
        }

    def _parse_sheet(self, root):
        transform = etree.XSLT(self.sheet_xslt)
        result = transform(root)
        df = read_csv(io.StringIO(str(result)),
                      header=None, dtype=str,
                      names=['row', 'cell', 'type', 'value'],
        )
        return df

    def read(self, sheet_number):
        sheet_id = self.workbook[sheet_number]['Sheet_id']
        sheet_path = 'xl/worksheets/sheet%s.xml' % sheet_id
        root = etree.parse(self.fh.open(sheet_path))
        df = self._parse_sheet(root)

        # First row numbers are filled with nan
        df['row'] = to_numeric(df['row'].fillna(0))

        # Translate string contents
        cond = (df.type == 's') & (~df.value.isnull())
        df.loc[cond, 'value'] = df[cond]['value'].map(self.shared)
        # Add column number and sort rows
        df['col'] = df.cell.str.replace(r'[0-9]+', '')
        df = df.sort_values(by='row')

        # Pivot everything
        df = df.pivot(
            index='row', columns='col', values='value'
        ).reset_index(drop=True).reset_index(drop=True)
        df.columns.name = None  # pivot adds a name to the "columns" array
        # Sort columns (pivot will put AA before B)
        cols = sorted(df.columns, key=lambda x: (len(x), x))
        df = df[cols]
        df = df.dropna(how='all')  # Ignore empty lines
        df = df.dropna(how='all', axis=1)  # Ignore empty cols
        return df


def run_program(run_by='Adqvest_Bot', py_file_name=None):

    table_name = 'AMC_MONTHLY_AAUM'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    # no_of_ping = 0
    Adqvest_Bot = False


    try:
        if(run_by=='Adqvest_Bot'):
            Adqvest_Bot = True
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            Adqvest_Bot = False
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        def end_date(date):
            end_dt = datetime.datetime(date.year, date.month, monthrange(date.year, date.month)[1]).date()
            return end_dt
        def find_month(x):
            if("jan" in x.lower()):
                month = "January"
            elif("feb" in x.lower()):
                month = "February"
            elif("mar" in x.lower()):
                month = "March"
            elif("apr" in x.lower()):
                month = "April"
            elif("may" in x.lower()):
                month = "May"
            elif("jun" in x.lower()):
                month = "Jun"
            elif("jul" in x.lower()):
                month = "July"
            elif("aug" in x.lower()):
                month = "August"
            elif("sep" in x.lower()):
                month = "September"
            elif("oct" in x.lower()):
                month = "October"
            elif("nov" in x.lower()):
                month = "November"
            elif("dec" in x.lower()):
                month = "December"



            return month

        def date_value(x):
            x = x.strip()
            try:
                x = datetime.datetime.strptime(x, '%B %Y').date()
            except:
                try:
                    x = datetime.datetime.strptime(x, '%b %Y').date()
                except:
                    try:
                        x = datetime.datetime.strptime(x, '%B %y').date()
                    except:
                        try:
                            x = datetime.datetime.strptime(x, '%b %y').date()
                        except:
                            try:
                                x = datetime.datetime.strptime(x, '%B%Y').date()
                            except:
                                try:
                                    x = datetime.datetime.strptime(x, '%b%Y').date()
                                except:
                                    try:
                                        x = datetime.datetime.strptime(x, '%B%y').date()
                                    except:
                                        x = datetime.datetime.strptime(x, '%b%y').date()


            return datetime.date(x.year, x.month, calendar.monthrange(x.year, x.month)[1])

        def clean_col(x):
            x = re.sub(r'  +',' ',x)
            x = x.strip()
            x = x.replace(' ','_')
            return x

        def find_date(x):
            try:
                x = x.replace('"','')
                x = x.replace('–','')
                x = x.replace('-','')
                x = re.sub(r'  +',' ',x)
                x = x.replace('\u200b','')
                x = x.strip()
                try:
                    date = datetime.datetime.strptime(x,'%B %Y').date()
                except:
                    try:
                        date = datetime.datetime.strptime(x,'%b %Y').date()
                    except:
                        try:
                            date = datetime.datetime.strptime(x,'%B%Y').date()
                        except:
                            date = datetime.datetime.strptime(x,'%b%Y').date()

                y = int(date.strftime('%Y'))
                m = int(date.strftime('%m'))
                d = calendar.monthrange(y, m)[1]

                return datetime.date(y,m,d)
            except:
                raise Exception("Error in find_date")

        def UTI(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'UTI MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                #     chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                #     download_file_path = r"C:\Users\Administrator\AMC"
                # #     os.chdir('C:/Users/Administrator/AMC')
                #     prefs = {
                #         "download.default_directory": download_file_path,
                #         "download.prompt_for_download": False,
                #         "download.directory_upgrade": True
                #         }
                #     options = webdriver.ChromeOptions()
                #     options.add_argument("--disable-infobars")
                #     options.add_argument("start-maximized")
                #     options.add_argument("--disable-extensions")
                #     options.add_argument("--disable-notifications")
                #     options.add_argument('--ignore-certificate-errors')
                #     options.add_argument('--no-sandbox')
                #
                #     driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
                #
                #     driver.get(url)
                    #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    # time.sleep(1)

                    # soup = BeautifulSoup(driver.page_source)

                    # r = requests.get(url)
                    robot.add_link(url)

                    # soup = BeautifulSoup(r.content,'lxml')
                    # driver.quit()
                    # links = soup.findAll("a",href = True)
                    # title = [x.text for x in links]
                    # links_df = pd.DataFrame({"Links":links,"Title":title})
                    # links_df = links_df[(links_df["Title"].str.lower().str.contains("scheme")) & (links_df["Title"].str.lower().str.contains("aaum"))]
                    # links_df["Links"] = links_df["Links"].apply(lambda x: x.get("href"))
                    # links_df.reset_index(drop = True,inplace = True)
                    # links_df = links_df[:links_df[links_df["Title"].str.lower().str.contains("march 2019")].index[0] + 1]

                    # data = pd.DataFrame((r.json()['rows']))
                    # links_df = data[['title', 'file_url']]
                    # links_df.columns = ['Title', 'Links']
                    # links_df["Title"] = links_df["Title"].apply(lambda x: x.split("Schemewise")[0].strip())
                    #
                    # links_df["Year"] = links_df["Title"].apply(lambda x: re.findall(r'[0-9]{4}',x)[0])
                    # links_df["Year"] = links_df["Year"].apply(lambda x: str(x))
                    #
                    # links_df["Month"] = links_df["Title"].apply(lambda x: re.findall(r'[A-Za-z]{3}',x)[0])
                    # links_df["Relevant_Date"] = links_df["Month"] + " " + links_df["Year"]
                    # links_df["Relevant_Date"] = links_df["Relevant_Date"].apply(lambda x: date_value(x))
                    #
                    # links_df["Links"] = links_df["Links"].apply(lambda x: x.replace(" ","%20"))
                    #
                    # print(links_df)
                    # links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    # links_df.reset_index(drop = True,inplace = True)

                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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
                    driver.get(url)
                    soup=BeautifulSoup(driver.page_source,'lxml')

                    data = soup.findAll("a",attrs={'class':'file-card row align-items-center'})

                    links=[link['href'] for link in data]
                    text=[link.get_text() for link in data]

                    links_df=pd.DataFrame({'Text':text,'Link':links})

                    links_df=links_df[links_df['Text'].str.contains('schemewise',case=False)]
                    links_df['Relevant_Date']=links_df['Text'].apply(lambda x:x.split("Schemewise")[0].strip().replace("-",' ').strip())
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:date_value(x))

                    links_df.rename(columns={'Text':'Title','Link':'Links'},inplace=True)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    links_df.reset_index(drop=True, inplace=True)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in UTI)"
                    error_msg = error_msg + " (Error in UTI)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        print(today)
        def ICICI(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'ICICI Prudential MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    for file in os.listdir():
                        os.remove(file)

                    driver.get(url)
                    robot.add_link(url)
                    #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    time.sleep(10)
                    no_of_ping += 1
                    driver.maximize_window()

                    time.sleep(1)
                    driver.find_element(By.XPATH,"//*[@id='divDisclosure']/div[2]/div/div[2]/div/div/div[2]/div[1]/div[1]/div[1]/div/button/span[1]").click()

                    time.sleep(1)
                    present_year = int(today.year)
                    if(today.month < 5):
                        driver.find_element(By.XPATH,f"//*[text() = '{present_year - 1}-{present_year}']").click()
                    else:
                        driver.find_element(By.XPATH,f"//*[text() = '{present_year}-{present_year + 1}']").click()
                    soup = BeautifulSoup(driver.page_source)
                    links_list = [x["href"] for x in soup.findAll("a",href = True) if "xls" in x["href"]]
                    links_df = pd.DataFrame({"Links":links_list})
                    #links_df = links_df[:4]
                    links_df["Month"] = links_df["Links"].apply(lambda x: find_month(x))
                    links_df["Year"] = links_df["Links"].apply(lambda x : re.findall(r'[0-9]{4}',x)[0])
                    links_df['Date'] = links_df['Month'] + ' ' + links_df['Year'].astype(str)
                    links_df["Relevant_Date"] = links_df['Date'].apply(date_value)

                    print(links_df)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    links_df = links_df.drop(columns=["Year","Month"])
                    links_df = links_df.drop_duplicates()
                    driver.quit()
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in ICICI)"
                    error_msg = error_msg + " (Error in ICICI)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)


        def SBI(url): #not being used currently
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'SBI MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:

                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    for file in os.listdir():
                        os.remove(file)

                    driver.get(url)
                    robot.add_link(url)
                    #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    time.sleep(10)
                    no_of_ping += 1
                    driver.maximize_window()

                    time.sleep(1)
                    driver.find_element_by_link_text("Monthly Disclosure of AAUM").click()
                    time.sleep(2)

                    # global today
                    present_year=str(today.year)
                    years=[present_year]
                    #years=['2021']
                    links=[]
                    final_months=[]
                    final_years=[]
                    for year in years:
                        try:
                            driver.find_element_by_id('ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMYear').send_keys(year)
                        except:
                            time.sleep(3)
                            # driver.find_element_by_id('ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMYear').send_keys(year)
                            # print('click')
                            driver.execute_script("document.body.style.zoom='40%'")
                            state = 'ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMYear'
                            element = driver.find_element_by_id(state).send_keys(year)
                            driver.execute_script("arguments[0].click();", element)
                            print('click')
                        time.sleep(3)
                        months=[]
                        driver.execute_script("document.body.style.zoom='50%'")
                        state = 'ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMMonth'
                        month_elements=driver.find_element_by_id(state)


                        options=[x for x in month_elements.find_elements_by_tag_name("option")]
                        options=options[1:]
                        for element in options:
                            months.append(element.get_attribute("value"))

                        for month in months:
                            #driver.find_element_by_id('ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMMonth').send_keys(month)
                            driver.execute_script("document.body.style.zoom='50%'")
                            state = 'ctl00_PlaceHolderMain_SBIMFControlSelectorID_ctl00_ucDisclosureOfAAUM1_ddlAAUMMonth'
                            element = driver.find_element_by_id(state).send_keys(month)
                            #driver.execute_script("arguments[0].click();", element)
                            print('click')
                            time.sleep(3)
                            links.append(driver.find_element_by_css_selector(".Title [href]").get_attribute('href'))
                            final_months.append(month)
                            final_years.append(year)


                    final_links_df=pd.DataFrame()
                    final_links_df['Links']=links
                    final_links_df['Month']=final_months
                    final_links_df['Year']=final_years
                    final_links_df['Date'] = final_links_df['Month'] + ' ' + final_links_df['Year'].astype(str)
                    final_links_df["Relevant_Date"] = final_links_df['Date'].apply(date_value)
                    links_df=final_links_df.copy()

                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    links_df = links_df.drop(columns=["Year","Month"])
                    driver.quit()
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in SBI)"
                    error_msg = error_msg + " (Error in SBI)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def SBI_NEW(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'SBI MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:

                try:
                    links = []
                    final_date = []
                    Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'SBI MF'",engine)
                    max_date = Latest_Date["Max"][0]

                    while max_date < today.date():
                        max_date = max_date + relativedelta(months=1)

                        if max_date.month == today.month and max_date.year == today.year:
                            break
                        else:
                            pass

                        run_date = max_date.strftime('%B %Y').lower().split()

                        # url = f'https://www.sbimf.com/en-us/lists/monthlydisclosureofaaum/monthly%20average%20asset%20under%20management%20{run_date[0]}%20{run_date[1]}.xls'
                        payload = {
                            'Month': run_date[0].title(),
                            'ReportName': "Monthly-Disclosure-of-AAUM",
                            'Year': run_date[1],
                        }
                        response = requests.post("https://www.sbimf.com/ajaxcall/CMS/GetDisclosureData", data = payload)
                        robot.add_link(url)
                        if 'No Records Found' in response.text:
                            continue
                        else:
                            pass
                        # if response.status_code == 200:
                        #     print('Web site exists')
                        #     pass
                        # else:
                        #     continue
                        soup = BeautifulSoup(response.content, 'lxml')
                        url = [i['href'] for i in soup.findAll('a') if 'Monthly Average Asset Under Management' in i.text][0]

                        links.append(url)
                        final_date.append(max_date.strftime('%B %Y'))

                    #     final_months.append(max_date.month)
                    #     final_years.append(max_date.year)

                    final_links_df=pd.DataFrame()
                    final_links_df['Links']=links
                    final_links_df['Date'] = final_date
                    # final_links_df['Month']=final_months
                    # final_links_df['Year']=final_years
                    # final_links_df['Date'] = final_links_df['Month'] + ' ' + final_links_df['Year'].astype(str)
                    final_links_df["Relevant_Date"] = final_links_df['Date'].apply(date_value)
                    links_df=final_links_df.copy()
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in SBI_NEW)"
                    error_msg = error_msg + " (Error in SBI_NEW)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)



        def Bandhan(url):
            #url = "https://www.idfcmf.com/download-centre/disclosures"
            global today
            global no_of_ping
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Bandhan MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:

                try:
                    # chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    # download_file_path = r"C:\Users\Administrator\AMC"
                    # os.chdir('C:/Users/Administrator/AMC')
                    # prefs = {
                    #     "download.default_directory": download_file_path,
                    #     "download.prompt_for_download": False,
                    #     "download.directory_upgrade": True
                    #     }
                    # options = webdriver.ChromeOptions()
                    # options.add_argument("--disable-infobars")
                    # options.add_argument("start-maximized")
                    # options.add_argument("--disable-extensions")
                    # options.add_argument("--disable-notifications")
                    # options.add_argument('--ignore-certificate-errors')
                    # options.add_argument('--no-sandbox')
                    #
                    # options.add_experimental_option('prefs', prefs)
                    #
                    # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
                    #
                    # for file in os.listdir():
                    #     os.remove(file)
                    #
                    # driver.get(url)
                    # #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    # time.sleep(10)
                    # no_of_ping += 1
                    # driver.maximize_window()
                    #
                    # present_year=str(today.year)
                    #
                    # limit  = 0
                    # while True:
                    #     try:
                    #         driver.get(url)
                    #         no_of_ping += 1
                    #         time.sleep(10)
                    #         try:
                    #             driver.find_element_by_xpath("//*[@id='app']/div/div[5]/div[1]/div/div/div[2]/div/span[2]/button").click()
                    #         except:
                    #             pass
                    #         # try:
                    #         #     driver.find_element_by_xpath("//*[@id='app']/div/div[5]/div[1]/div/div/span/i").click()
                    #         # except:
                    #         #     pass
                    #         time.sleep(2)
                    #         try:
                    #             driver.find_element_by_xpath(f"//*[contains(text(),'{present_year}')]").click()
                    #         except:
                    #             pass
                    #         break
                    #     except:
                    #         limit += 1
                    #         if(limit < 10):
                    #             continue
                    #         else:
                    #             break
                    #
                    # time.sleep(30)
                    # soup = BeautifulSoup(driver.page_source, 'lxml')
                    # files_list = soup.findAll(class_ = "list-name")
                    # files_list = [x.text for x in files_list if "AAUM" in x.text]
                    # #files_list = [files_list]
                    #
                    # for file in files_list:
                    #     time.sleep(3)
                    #     element = driver.find_element_by_xpath("//*[text() = '"+file+"']")
                    #     desired_y = (element.size['height'] / 2) + element.location['y']
                    #     current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
                    #     scroll_y_by = desired_y - current_y
                    #     driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
                    #     time.sleep(5)
                    #     element.click()
                    #     time.sleep(1)
                    #
                    # time.sleep(3)
                    # downloaded_files_list = os.listdir()
                    # links_df = pd.DataFrame({"Links" : downloaded_files_list})
                    # links_df["Month"] = links_df["Links"].apply(lambda x: find_month(x))
                    # links_df["Year"] = links_df["Links"].apply(lambda x : re.findall(r'[0-9]{4}',x)[0])
                    # links_df['Date'] = links_df['Month'] + ' ' + links_df['Year'].astype(str)
                    # links_df["Relevant_Date"] = links_df['Date'].apply(date_value)
                    # links_df.drop(columns = ["Month","Year"],inplace = True)

                    ######################################################################################3


                    if today.month == 1:
                        present_year = (today.year)
                    else:
                        present_year = today.year

                    headers = {
                            'authority': 'cms.bandhanmutual.com',
                            'accept': '*/*',
                            'accept-language': 'en-US,en;q=0.9',
                            'application-platform': 'M',
                            'origin': 'https://bandhanmutual.com',
                            'referer': 'https://bandhanmutual.com/',
                            'sec-ch-ua': '"Microsoft Edge";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                            'sec-ch-ua-mobile': '?1',
                            'sec-ch-ua-platform': '"Android"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'same-site',
                            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36 Edg/119.0.0.0',
                        }

                    params = {
                        '_format': 'json',
                    }

                    json_data = {}

                    json_data = {
                            'type': 'year',
                            'key': present_year,
                        }


                    r = requests.post(url,  params=params,headers=headers,json=json_data,)
                    robot.add_link(url)
                    if r.status_code != 200:
                        raise Exception('Invalid Response: Status code not 200')
                    all_data = json.loads(r.content)
                    all_links = []
                    all_dates = []
                    operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    pattern = r"(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|April)-\d{4}"
                    year_pattern = r'\b20\d{2}(?=\.XLS)\b'
                    for i in all_data['data']['files']:
                        # if 'AAUM' in i['name']:
                        if 'AAUM' in i['name'] and 'Scheme Category wise AAUM' not in i['name']:
                            print(i)
                            month = re.search(operation, i['name'].strip()).group(0)
                            print(month)
                            match = re.search(r'(\d{4})[._]', i['url'])
                            year = match.group(1)
                            print(year)
                            date = month + ' ' + str(year)
                            link = i['url']
                            matches = re.findall(pattern, link)
                            # date=matches[0].title()
                            all_links.append(link)
                            all_dates.append(date)

                #################################################################################

                # download_file_path = r"C:\Users\Administrator\AMC"
                # os.chdir('C:/Users/Administrator/AMC')
                # prefs = {
                #     "download.default_directory": download_file_path,
                #     "download.prompt_for_download": False,
                #     "download.directory_upgrade": True
                #     }
                # options = webdriver.ChromeOptions()
                # options.add_argument("--disable-infobars")
                # options.add_argument("start-maximized")
                # options.add_argument("--disable-extensions")
                # options.add_argument("--disable-notifications")
                # options.add_argument('--ignore-certificate-errors')
                # options.add_argument('--no-sandbox')
                # options.add_experimental_option('prefs', prefs)

                # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)


                # driver.get(url)
                # robot.add_link(url)

                # time.sleep(15)

                # driver.find_element(By.XPATH, '//span[@class="right-button"]//button').click()

                # max_date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Bandhan MF'",engine)['Max'][0]
                # start_year = max_date.year
                # present_year = today.year


                # years = [i.get_attribute('value') for i in driver.find_elements(By.XPATH,'//ul[@class="yearly"]//li')]

                # links = []
                # dates = []
                # operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                # time.sleep(10)
                # while start_year <= present_year:

                #     driver.find_element(By.XPATH,f'//ul[@class="bottomSpace"]//span[text()="{str(start_year)}"]').click()

                #     time.sleep(10)
                #     docs = driver.find_elements(By.XPATH, '//ul[@class="files-list"]//span[@class="list-name"]')

                #     for doc in docs:

                #         if 'AAUM' in doc.text:

                #             doc_name = doc.text
                #             print(doc_name)

                #             month = re.search(operation, doc_name.strip()).group(0)

                #             date = date_value(month + ' ' + str(start_year))

                #             if date > max_date:

                #                 desired_y = (doc.size['height'] / 2) + doc.location['y']
                #                 current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
                #                 scroll_y_by = desired_y - current_y
                #                 driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
                #                 time.sleep(10)
                #                 doc.click()

                #                 time.sleep(10)

                #                 Initial_path = 'C:/Users/Administrator/AMC'
                #                 filename = max([Initial_path + "/" + f for f in os.listdir(Initial_path)],key=os.path.getctime)

                #                 newfilename = "Bandhan-AAUM-" + str(date) + '.' + filename.split('.')[-1]
                #                 try:
                #                     os.remove(newfilename)
                #                 except:
                #                     pass
                #                 shutil.move(filename,os.path.join(Initial_path,newfilename))

                #                 links.append(Initial_path + '/' + newfilename)
                #                 dates.append(date)



                #     start_year += 1


                # driver.quit()

                    links_df = pd.DataFrame({'Links' : all_links, 'Relevant_Date' : all_dates})
                    links_df['Relevant_Date'] = pd.to_datetime(links_df.Relevant_Date).dt.date + relativedelta(months=1, days=-1, day=1)

                    print(links_df)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
            #         if len(links_df)==1:
            #             links_df.drop(0,axis=0,inplace=True)
            # #         driver.quit()
                    print(links_df)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Bandhan)"
                    error_msg = error_msg + " (Error in Bandhan)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''

                    log_amc_wise_errors(error_type,error_msg,run)

        def Reliance_Nippon(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Nippon MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    no_of_ping += 1
                    print(url)
                    r = requests.get(url)
                    robot.add_link(url)
                    soup=BeautifulSoup(r.content)
                    links_list=[]
                    for i in range(0,5):
                        try:
                            a=soup.find_all(class_="rhsWrapper")[0].find_all(class_='rhsLbl')[i].find(class_='xls').get('href')
                            #print(a)
                            links_list.append(a)
                        except:
                            continue
                    month_str=[]
                    for i in range(0,5):
                        try:
                            month_str2=soup.find_all(class_="rhsWrapper")[0].find_all(class_='lhsLbl')[i].text.split('Month')[-1].split('.')[0].replace(' ','-')
                            if 'disclosure-of-scheme-category' in  str(month_str2).lower():               #Added Condition | JOE
                                continue
                            month_str.append(month_str2)
                        except:
                            continue
                    month_str=pd.DataFrame(month_str)
                    month_str=np.where((month_str[0].str.contains('Quarterly')),np.nan,month_str[0])
                    month_str=pd.DataFrame(month_str)
                    month_str.dropna(inplace=True)
                    # Added 691, 692 | Pushkar | 13 APR 2K23
                    month_str[0] = month_str[0].str.replace('[^a-zA-Z0-9]+', ' ').str.strip()
                    month_str[0] = month_str[0].str.replace('of', '').str.strip()
                    month_str[0]=month_str[0].str.replace('--','-')
                    month_str[0]=month_str[0].str.replace('-of-','')
                    month_str[0]=month_str[0].str.replace('--','-')
                    month_str[0]=month_str[0].str.replace(' ','')
                    month_list = []
                    month_list.extend(month_str[0].tolist())
                    links_list = [link for link in links_list if 'quarterly' not in link.lower()]
                    data = {"Month":month_list,"Links":links_list}
                    links_df = pd.DataFrame(data)
                    links_df["Relevant_Date"] = links_df["Month"].apply(lambda x:find_date(x))
                    links_df.drop(columns = ["Month"],inplace = True)
                    links_df["Links"] = links_df["Links"].apply(lambda x:"https://mf.nipponindiaim.com"+x)

                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Reliance_Nippon)"
                    error_msg = error_msg + " (Error in Reliance_Nippon)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def Aditya_Birla(url):
            global no_of_ping
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Aditya Birla Sun Life MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
                    # limit = 0
                    # while True:
                    #     try:
                    #         driver.get(url)#='https://mutualfund.adityabirlacapital.com/forms-and-downloads/disclosures')\
                    #         #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    #         time.sleep(20)
                    #         print('Click')
                    #         #driver.find_element_by_xpath("//*[text()='Monthly AAUM']").click()
                    #         #driver.find_element_by_xpath("/html/body/div[4]/div[8]/div[3]/div/div/div/div/div/ul/li[5]/h3").click()
                    #         print('Clicked')
                    #         time.sleep(5)
                    #
                    #         #driver.close()
                    #         # links_list = [x["href"] for x in soup.findAll("a",href = True) if "zip" in x["href"]]
                    #
                    #         time.sleep(20)
                    #         element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH("//*[text()='Monthly AAUM']"))))
                    #
                    #         # desired_y = (element.size['height'] / 2) + element.location['y']
                    #         # current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
                    #         # scroll_y_by = desired_y - current_y
                    #         # driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
                    #         time.sleep(10)
                    #         element.click()
                    #         time.sleep(10)
                    #         soup = BeautifulSoup(driver.page_source)
                    #         time.sleep(2)
                    #         date_links = soup.findAll('div', class_ = 'afTab')
                    #         links = soup.findAll('a', class_ = 'afDowloadCss')
                    #
                    #         dates_list = []
                    #         for i in date_links:
                    #             a = i.find('a').text
                    #             a = parser.parse(a, fuzzy = True).strftime('%Y-%m-%d')
                    #             dates_list.append(a)
                    #
                    #         links_list = []
                    #         for link in links:
                    #             a = link['href']
                    #             links_list.append(a)
                    #
                    #         driver.close()
                    #         break
                    #     except:
                    #         try:
                    #             driver.close()
                    #         except:
                    #             pass
                    #         limit += 1
                    #         if limit > 3:
                    #             raise Exception('Aditya Birla Website not loading')
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=False)
                        page = browser.new_page()
                        page.set_default_timeout(0)
                        page.goto(url, wait_until="networkidle")
                        # time.sleep(10)
                        # page.goto('https://mutualfund.adityabirlacapital.com/forms-and-downloads/disclosures', wait_until="networkidle")
                        # time.sleep(10)
                        ele = page.locator('text=Monthly AAUM')
                        ele.click()
                        time.sleep(10)
                        soup = BeautifulSoup(page.content(), 'lxml')
                        date_links = soup.findAll('div', class_='afTab')
                        links = soup.findAll('a', class_='afDowloadCss')
                        # print(links)
                        dates_list = []
                        for i in date_links:
                            a = i.find('a').text
                            if 'February 29' in a:
                                a = a.replace('2023', '2024')
                            a = parser.parse(a, fuzzy=True).strftime('%Y-%m-%d')
                            dates_list.append(a)

                        links_list = []
                        for link in links:
                            a = link['href']
                            links_list.append(a)

                    robot.add_link(url)
                    links_df=pd.DataFrame()
                    links_df['Links']=links_list
                    links_df['Relevant_Date']=dates_list
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())
                    links_df['Links']=links_df['Links'].apply(lambda x:"https://mutualfund.adityabirlacapital.com"+x)
                    # links_df.iloc[0,1]=datetime.date(2024,2,29)
                    

                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    links_df.reset_index(drop=True, inplace=True)

                    print(links_df['Links'][0])

                    file_name=[]
                    os.chdir('C:/Users/Administrator/AMC')
                    for link in links_df['Links']:

                        driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
                        time.sleep(10)
                        driver.get(link)


                        time.sleep(10)
                        driver.close()

                        zip_file = []
                        for files in os.listdir():
                            if files.endswith(".zip"):
                                zip_file.append(files)

                        with zipfile.ZipFile(zip_file[0]) as z:
                        # z = zipfile.ZipFile(zip_file[0])
                            z.extractall()
                            z.close()
                            file_name.append(str(z.filelist[0]).split("'")[1])
                            print(str(z.filelist[0]).split("'")[1])
                        for files in zip_file:
                            os.remove(files)

                    links_df['Links']=file_name

                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Aditya_Birla)"
                    error_msg = error_msg + " (Error in Aditya_Birla)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)


        def Edelweiss(url): # not being used currnetly
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Edelweiss MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:

                try:
                    # print(url)
                    # chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    # download_file_path = r"C:\Users\Administrator\AMC"
                    # os.chdir('C:/Users/Administrator/AMC')
                    # prefs = {
                    #     "download.default_directory": download_file_path,
                    #     "download.prompt_for_download": False,
                    #     "download.directory_upgrade": True
                    #     }
                    # options = webdriver.ChromeOptions()
                    # options.add_argument("--disable-infobars")
                    # options.add_argument("start-maximized")
                    # options.add_argument("--disable-extensions")
                    # options.add_argument("--disable-notifications")
                    # options.add_argument('--ignore-certificate-errors')
                    # options.add_argument('--no-sandbox')

                    # options.add_experimental_option('prefs', prefs)

                    # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

                    # no_of_ping += 1
                    # driver.get(url)
                    # robot.add_link(url)
                    # #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    # time.sleep(10)

                    # limit_2 = 0
                    # while True:
                    #     try:
                    #         no_of_ping += 1
                    #         driver.find_element_by_xpath("//*[@id='s9-tab']/span").click()
                    #         time.sleep(3)
                    #         break
                    #     except:
                    #         time.sleep(2)
                    #         limit_2 = limit_2 + 1
                    #         print('error')
                    #         if(limit_2<3):
                    #             no_of_ping += 1
                    #             driver.refresh()
                    #             time.sleep(3)
                    #         if(limit_2>8):
                    #             driver.quit()
                    #             raise Exception(str(sys.exc_info()[1]))

                    # limit_3 = 0
                    # while True:
                    #     try:
                    #         no_of_ping += 1
                    #         driver.find_element_by_xpath("//*[contains(text(),'Monthly disclosure of AAUM')]").click()
                    #         time.sleep(3)
                    #         break
                    #     except:
                    #         time.sleep(2)
                    #         limit_3 = limit_3 + 1
                    #         print('error')
                    #         if(limit_3<3):
                    #             no_of_ping += 1
                    #             driver.refresh()
                    #             time.sleep(3)
                    #         if(limit_3>8):
                    #             driver.quit()
                    #             raise Exception(str(sys.exc_info()[1]))




                    # time.sleep(2)
                    # soup = BeautifulSoup(driver.page_source, 'html.parser')

                    # time.sleep(2)

                    # driver.quit()

                    # links = []
                    # title = []
                    # for row in soup.findAll(href=re.compile(r'http(.*?)xlsb')):
                    #     links.append(row['href'])
                    #     title.append(row.text.replace("\n","").replace("  "," ").strip())


                    # links_df = pd.DataFrame({"Links":links,"Title":title})
                    # links_df = links_df[links_df["Links"].str.contains("Monthly disclosure of AAUM")]
                    # links_df = links_df[links_df["Title"]!=""]
                    # links_df["Title"] = links_df["Title"].apply(lambda x: x.split("on")[-1].strip().replace(",",""))
                    # links_df["Relevant_Date"] = links_df["Title"].apply(lambda x: datetime.datetime.strptime(x,"%B %d %Y").date())

                    # Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Edelweiss MF'",engine)
                    # Latest_Date = Latest_Date["Max"][0]
                    # print(links_df)
                    # links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    # links_df.reset_index(drop = True,inplace = True)

                    # chrome_driver = executable_path=chrome_driver
                    # download_file_path = r"C:\Users\Abdulmuizz\Desktop\ADQVest\AMC\AMC_MONTHLY_AAUM\Download path"
                    # os.chdir(r"C:\Users\Abdulmuizz\Desktop\ADQVest\AMC\AMC_MONTHLY_AAUM\Download path")
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')

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
                    # url = 'https://www.edelweissmf.com/statutory'
                    # no_of_ping += 1
                    driver.get(url)
                    #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    time.sleep(15)
                    robot.add_link(url)

                    element = driver.find_element(By.XPATH,"//*[contains(text(),'Other Disclosures')]")
                    # element = driver.find_element_by_xpath('/html/body/app-root/ion-app/ion-router-outlet/app-disclosure-documents/ion-content/ion-grid/ion-row/ion-col[2]/ion-row/ion-col/ion-row/ion-col[1]/ul/li[8]')

                    a = ActionChains(driver)
                    a.move_to_element(element).perform()

                    time.sleep(5)
                    element.click()
                    time.sleep(15)

                    driver.find_element(By.XPATH,"//*[contains(text(),' Monthly disclosure of AAUM ')]").click()


                    soup = BeautifulSoup(driver.page_source, 'lxml')

                    element = soup.findAll('li', class_ = 'item-center justify-content-between textUnderline ng-star-inserted')
                    print(element)
                    links = element[0].a
                    links = [links['href']]

                    date_text = element[0].p.text

                    # date_text=' Monthly AAUM as on January 31, 2024 '

                    date= []
                    operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"

                    month = re.search(operation, date_text.strip()).group(0)
                    year=re.findall(r'\d+',date_text.strip())[1]
                    day=re.findall(r'\d+',date_text.strip())[0]
                    try:
                        month_number = datetime.datetime.strptime(month, '%b').month
                    except:
                        month_number = datetime.datetime.strptime(month, '%B').month
                    date.append(year+'-'+str(month_number)+'-'+day)

                    links=["https://www.edelweissmf.com/Files/Other-Disclosure/Monthly%20disclosure%20of%20AAUM/2024/Jan/published/Monthly%20AAUM_January%202024_09022024_031438_PM.xlsx"]

                    links_df = pd.DataFrame({"Links":links,"Relevant_Date":date})
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())


                    print(links_df)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    links_df['Links'] = links_df['Links'].apply(lambda x: x.replace(' ','%20'))
                    links_df.reset_index(drop = True,inplace = True)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Edelweiss)"
                    error_msg = error_msg + " (Error in Edelweiss)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        # def Edelweiss_NEW(url):
        #     global no_of_ping
        #     try:
        #         r = requests.get(url)
        #         robot.add_link(url)
        #         data = json.loads(r.content)
        #         all_dates = []
        #         all_links = []
        #         for i in data['CommonDetails']:
        #             if i['SubMenuName'] == 'Monthly disclosure of AAUM':
        #                 month = i['Month']
        #                 year = i['Year']
        #                 date = month + " " + year
        #                 link = 'https://www.edelweissmf.com' + i['FilePath']
        #                 link = link.replace(' ','%20')
        #                 all_dates.append(date)
        #                 all_links.append(link)

        #         links_df = pd.DataFrame({'Links' : all_links, 'Relevant_Date' : all_dates})
        #         links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(date_value)
        #         Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Edelweiss MF' ",engine)
        #         Latest_Date = Latest_Date["Max"][0]
        #         print(Latest_Date)
        #         links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

        #         print(links_df)
        #         return links_df
        #     except:
        #         error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        #         error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        #         error_type = error_type + " (Error in Edelweiss_NEW)"
        #         error_msg = error_msg + " (Error in Edelweiss_NEW)"
        #         print(error_type)
        #         print(error_msg)
        #         if Adqvest_Bot == True:
        #             run = "'Adqvest_Bot'"
        #         else:
        #             run = 'null'
        #         query = f"Insert into TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA value ('AMC_MONTHLY_AAUM', 'A004_AMC_MONTHLY_AAUM', '10_AM_WINDOWS_SERVER_2_SCHEDULER', {run}, '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}', null,null,null,null,'{error_type}', '{error_msg}', null, null,'','','', '{str(today.strftime('%Y-%m-%d'))}', '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}')"
        #         connection.execute(sqlalchemy.text(query))
        #         connection.execute('commit')

        def HDFC(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'HDFC AMC'", engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    # limit_1 = 0
                    # while True:
                    #     try:
                    #         no_of_ping += 1
                    #         driver.get(url)
                    #         #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    #         time.sleep(5)
                    #         break
                    #     except:
                    #         time.sleep(2)
                    #         limit_1 = limit_1 + 1
                    #         print('error')
                    #         if(limit_1<3):
                    #             no_of_ping += 1
                    #             driver.refresh()
                    #             time.sleep(3)
                    #         if(limit_1>8):
                    #             driver.quit()
                    #             break
                    #             raise Exception(str(sys.exc_info()[1]))
                    robot.add_link(url)
                    # time.sleep(20)

                    # Commented | Pushkar | 23 Aug 2023
                    # limit_2 = 0
                    # while True:
                    #     try:
                    #         no_of_ping += 1
                    #         try:
                    #             driver.get(url)
                    #             time.sleep(10)
                    #             driver.find_element_by_xpath("//button[@class='rounded-pink-rect'][@data-attr='false']").click()
                    #         except:
                    #             pass
                    #         time.sleep(5)
                    #         break
                    #     except:
                    #         time.sleep(2)
                    #         limit_2 = limit_2 + 1
                    #         print('error')
                    #         if(limit_2<3):
                    #             no_of_ping += 1
                    #             driver.refresh()
                    #             time.sleep(3)
                    #         if(limit_2>8):
                    #             driver.quit()
                    #             break
                    #             raise str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
                    driver.get(url)
                    # Changes | Pushkar | 23 Aug 2023
                    time.sleep(2)
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    driver.quit()
                    # files_list = soup.findAll(class_="files")[0].findAll(class_=re.compile("container-930 list-content.*"))
                    files_list = soup.findAll('div', class_="row pt-4")[0].findAll('div', class_="col-md-12")

                    title_list = []
                    links_list = []
                    for i in range(len(files_list)):
                        links_list.append(files_list[i].find("a", href=True).get("href"))
                        title_list.append(files_list[i].text)
                    links_df = pd.DataFrame({"Links": links_list, "Title": title_list})

                    links_df["Year"] = links_df["Title"].apply(lambda x: re.findall(r'[0-9]{4}', x)[0])
                    links_df["Month"] = links_df["Links"].apply(lambda x: re.search(r'[0-9]{4}-[0-9]{2}', x)[0])
                    links_df["Relevant_Date"] = links_df["Month"].apply(lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), 1) - datetime.timedelta(1))
                    links_df["Relevant_Date"] = pd.to_datetime(links_df["Relevant_Date"]).dt.date
                    links_df.drop(columns=["Title"], inplace=True)

                    links_df = links_df.drop(columns=["Year", "Month"])
                    print(links_df)

                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"] > Latest_Date]

                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in HDFC)"
                    error_msg = error_msg + " (Error in HDFC)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def extract_json_objects(text, decoder=JSONDecoder()):
            """Find JSON objects in text, and yield the decoded JSON data

            Does not attempt to look for JSON arrays, text, or other JSON types outside
            of a parent JSON object.

            """
            pos = 0
            while True:
                match = text.find('{', pos)
                if match == -1:
                    break
                try:
                    result, index = decoder.raw_decode(text[match:])
                    yield result
                    pos = match + index
                except ValueError:
                    pos = match + 1

        # Commented | Pushkar | 28 Nov 2023 |

        # def LTMF(url):
        #     global no_of_ping
        #     global today
        #     Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'L&T MF'",engine)
        #     Latest_Date = Latest_Date["Max"][0]
        #     if today.month != (Latest_Date + relativedelta(months=1)).month:
        #         try:
        #             chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        #             download_file_path = r"C:\Users\Administrator\AMC"
        #             os.chdir('C:/Users/Administrator/AMC')
        #             prefs = {
        #                 "download.default_directory": download_file_path,
        #                 "download.prompt_for_download": False,
        #                 "download.directory_upgrade": True
        #                 }
        #             options = webdriver.ChromeOptions()
        #             options.add_argument("--disable-infobars")
        #             options.add_argument("start-maximized")
        #             options.add_argument("--disable-extensions")
        #             options.add_argument("--disable-notifications")
        #             options.add_argument('--ignore-certificate-errors')
        #             options.add_argument('--no-sandbox')

        #             options.add_experimental_option('prefs', prefs)

        #             driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
        #             driver.get(url='https://www.ltfs.com/companies/lnt-investment-management/statutory-disclosures.html')
        #             #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
        #             links=[]
        #             dates_list=[]
        #             for i in driver.find_elements(By.PARTIAL_LINK_TEXT,'Disclosure of AAUM'):
        #                 links.append(i.get_attribute('href'))
        #                 dates_list.append(i.text)

        #             driver.close()
        #             dates_list=dates_list[:3]
        #             links=links[:3]
        #             date=[]
        #             operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        #             for i in range(len(dates_list)):

        #                 month = re.search(operation, dates_list[i].strip()).group(0)
        #                 try:
        #                     year=re.findall(r'\d+',dates_list[i].strip())[1]
        #                 except:
        #                     year=re.findall(r'\d+',dates_list[i].strip())[0]
        #                 try:
        #                     month_number = datetime.datetime.strptime(month, '%b').month
        #                 except:
        #                     month_number = datetime.datetime.strptime(month, '%B').month
        #                 day=calendar.monthrange(int(year), month_number)[1]
        #                 date.append(year+'-'+str(month_number)+'-'+str(day))


        #             links_table=pd.DataFrame()
        #             links_table['Links']=links
        #             links_table['Relevant_Date']=date
        #             links_table['Text']=dates_list
        #             links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
        #             links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:x.date())

        #             print(Latest_Date)
        #             links_table = links_table[links_table["Relevant_Date"]>Latest_Date]

        #             print(links_table)


        #             return links_table
        #         except:
        #             error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        #             error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        #             error_type = error_type + " (Error in LTMF)"
        #             error_msg = error_msg + " (Error in LTMF)"
        #             print(error_type)
        #             print(error_msg)
        #             if Adqvest_Bot == True:
        #                 run = "'Adqvest_Bot'"
        #             else:
        #                 run = 'null'
        #             query = f"Insert into TRANSACTION_LOG_AND_ERROR_LOG_DAILY_DATA value ('AMC_MONTHLY_AAUM', 'A004_AMC_MONTHLY_AAUM', '10_AM_WINDOWS_SERVER_2_SCHEDULER', {run}, '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}', null,null,null,null,'{error_type}', '{error_msg}', null, null,'','','', '{str(today.strftime('%Y-%m-%d'))}', '{str(today.strftime('%Y-%m-%d %H:%M:%S'))}')"
        #             connection.execute(sqlalchemy.text(query))
        #             connection.execute('commit')



        def KOTAK(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Kotak Mahindra MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

                    # if today.month == 1:
                    #     year=str(today.year - 1)
                    # else:
                    #     year=str(today.year)
                    # url=url+year

                    # headers = {
                    #    "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
                    #            }

                    # r = requests.get(url , verify = False , headers = headers)
                    # robot.add_link(url)

                    # demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
                    # jsons = []
                    # for result in demo_text:
                    #     jsons.append(result)

                    # text=[]
                    # link=[]
                    # try:
                    #     for i in jsons[0]['subHeaderList']:
                    #         if 'AAUM as on' in i['subHeaderTitle']:
                    #             text.append(i['subHeaderTitle'])
                    #             link.append(i['content'])
                    # except:
                    #     print('No new data')
                    #     return pd.DataFrame() #returning an empty dataframe

                    # date=[]
                    # operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    # for i in range(len(text)):
                    #     month = re.search(operation, text[i].strip()).group(0)
                    #     try:
                    #         year=re.findall(r'\d+',text[i].strip())[1]
                    #     except:
                    #         year=re.findall(r'\d+',text[i].strip())[0]
                    #     try:
                    #         month_number = datetime.datetime.strptime(month, '%B').month
                    #     except:
                    #         month_number = datetime.datetime.strptime(month, '%b').month
                    #     date.append(year+'-'+str(month_number)+'-'+str(calendar.monthrange(int(year),int(month_number))[1]))

                    # links_df=pd.DataFrame()
                    # links_df['Relevant_Date']=date
                    # links_df['Links']=['https://vatseelabs.s3.ap-south-1.amazonaws.com/'+x for x in link]
                    # links_df['Text']=text
                    # links_df=links_df[~(links_df['Text'].str.contains('SEBI Specified'))]
                    # links_df.reset_index(drop=True,inplace=True)
                    # links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    # links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())

                    # links_df.drop('Text',axis=1,inplace=True)
                    from dateutil.parser import parse
                    def ldm(any_day):
                        # this will never fail
                        # get close to the end of the month for any day, and add 4 days 'over'
                        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
                        # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
                        return next_month - datetime.timedelta(days=next_month.day)

                    max_date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Kotak Mahindra MF'",engine)['Max'][0]
                    print(max_date)
                    start_year = max_date.year
                    present_year = today.year
                    all_data = []
                    pw = sync_playwright().start()
                    browser = pw.firefox.launch(headless = False)
                    context = browser.new_context(java_script_enabled = True,bypass_csp=True)
                    page = context.new_page()

                    page.goto(url)
                    time.sleep(5)
                    page.hover('//*[contains(text(),"Transmission of Units")]')
                    years = [i.strip() for i in page.locator('//div[contains(@class,"subHeaderTitle") and contains(text(),"Average AUM")]/parent::div//select').locator('option').all_text_contents()][1:]
                    robot.add_link(url)
                    time.sleep(20)
                    links = []
                    dates = []
                    print("Years :",years)
                    while start_year <= present_year:

                        if str(start_year) in years:
                            print("Year :", start_year)
                            page.select_option('//div[contains(@class,"subHeaderTitle") and contains(text(),"Average AUM")]/parent::div//select', value=str(start_year))
                            time.sleep(5)
                            page.hover('//*[contains(text(),"Transmission of Units")]')
                            search_df = pd.read_html(page.content())[0]
                            
                            search_df = search_df[search_df['Description'].str.contains('AAUM')]
                            search_df = search_df[~(search_df['Description'].str.contains('SEBI'))]
                            search_df['Relevant_Date'] = search_df['Description'].apply(lambda x: parse(x.replace('Apr 31','Apr 30'),fuzzy = True).date())
                            search_df['Relevant_Date'] = search_df['Relevant_Date'].apply(ldm)
                            search_df = search_df[search_df['Relevant_Date'] > max_date]
                            print(search_df)

                            if not search_df.empty:
                            
                                for _,search in search_df.iterrows():
                                    with page.expect_download() as download_info:
                                        page.locator(f'//td[@class="descwidth"]//a[contains(text(),"{search["Description"]}") and not(contains(text(),"SEBI"))]/ancestor::tr//span').click()
                                    download = download_info.value
                                    download.save_as(f"C:/Users/Administrator/AMC/{download.suggested_filename}")
                                    time.sleep(30)
                                Initial_path = 'C:/Users/Administrator/AMC'
                                print('-----', os.listdir(Initial_path))
                                if len(os.listdir(Initial_path)) != 0:
                                    filename = max([Initial_path + "/" + f for f in os.listdir(Initial_path)],key=os.path.getctime)
                                    newfilename = "Kotak-AAUM-" + str(search['Relevant_Date']) + '.' + filename.split('.')[-1]
                                    try:
                                        os.remove(newfilename)
                                    except:
                                        pass
                                    shutil.move(filename,os.path.join(Initial_path,newfilename))
                                    links.append(Initial_path + '/' + newfilename)
                                    dates.append(search['Relevant_Date'])

                        start_year += 1
                                    
                        
                    pw.stop()
                    links_df = pd.DataFrame({'Links': links, 'Relevant_Date' : dates})
                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    print("Final Links dataframe :",links_df)


                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in KOTAK)"
                    error_msg = error_msg + " (Error in KOTAK)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

                   



        def AXIS(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Axis AMC'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    # headers = {
                    #    "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
                    #            }

                    # r = requests.get(url , verify = False , headers = headers)
                    # robot.add_link(url)

                    # demo_text = extract_json_objects(r.text, decoder=JSONDecoder())
                    # jsons = []
                    # for result in demo_text:
                    #     jsons.append(result)

                    # text=[]
                    # link=[]
                    # for i in jsons:
                    #     if 'Average Assets Under Management' in i['field_pdf_name_statutory']:
                    #         text.append(i['field_pdf_name_statutory'].title())
                    #         link.append(i['field_related_file'])

                    # print(text)

                    # date=[]
                    # operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    # for i in range(len(text)):
                    #     month = re.search(operation, text[i].strip()).group(0)
                    #     try:
                    #         year=re.findall(r'\d+',text[i].strip())[1]
                    #     except:
                    #         year=re.findall(r'\d+',text[i].strip())[0]
                    #     try:
                    #         month_number = datetime.datetime.strptime(month, '%B').month
                    #     except:
                    #         month_number = datetime.datetime.strptime(month, '%b').month
                    #     date.append(year+'-'+str(month_number)+'-'+str(calendar.monthrange(int(year),int(month_number))[1]))

                    # links_df=pd.DataFrame()
                    # links_df['Relevant_Date']=date
                    # links_df['Links']=['https://www.axismf.com'+x for x in link]
                    # links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    # links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())
                    # links_df=links_df[links_df['Relevant_Date']>datetime.datetime.strptime('2019-03-31', '%Y-%m-%d').date()]
                    # links_df.reset_index(drop=True,inplace=True)
                    # links_df['File_Name']=['AXIS_AAUM'+str(x) for x in links_df['Relevant_Date']]
                    # print(links_df)

                    def ldm(any_day):
                        # this will never fail
                        # get close to the end of the month for any day, and add 4 days 'over'
                        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
                        # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
                        return next_month - datetime.timedelta(days=next_month.day)

                    max_date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Axis AMC'",engine)['Max'][0]

                    start_date = max_date + relativedelta(months=1)
                    start_date = ldm(start_date)

                    options = webdriver.ChromeOptions()
                    options.add_argument("--disable-infobars")
                    options.add_argument("start-maximized")
                    options.add_argument("--disable-extensions")
                    options.add_argument("--disable-notifications")
                    options.add_argument('--ignore-certificate-errors')
                    options.add_argument('--no-sandbox')


                    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)
                    # url = 'https://www.axismf.com/statutory-disclosures'
                    driver.get(url)
                    robot.add_link(url)

                    time.sleep(10)
                    from selenium.webdriver import ActionChains
                    # driver.find_element(By.TAG_NAME,"body").send_keys(Keys.HOME)

                    element = driver.find_element(By.XPATH, '//*[contains(text(),"Remuneration")]')
                    action = ActionChains(driver)
                    action.move_to_element(element).perform()

                    time.sleep(5)

                    driver.find_element(By.XPATH, '//*[contains(text(),"Monthly Average Assets under Management")]').click()

                    links = []
                    dates = []
                    while start_date < today.date():

                        print(start_date)
                        month_search = start_date.strftime('%B')
                        year = start_date.year
                        year_search = str(year)

                        time.sleep(5)

                        driver.find_element(By.XPATH, '//ion-text[contains(text(),"Select a Year")]/ancestor::ion-col//ion-input').click()

                        time.sleep(5)

                        available_years = [i.text.strip() for i in driver.find_elements(By.XPATH, '//div[@class="ps-content"]//span')]

                        if year_search in available_years:

                            driver.find_element(By.XPATH, f'//div[@class="ps-content"]//span[contains(text(),"{year_search}")]').click()

                            time.sleep(10)

                            driver.find_element(By.XPATH, '//ion-text[contains(text(),"Select a Month")]/ancestor::ion-col//ion-input').click()

                            time.sleep(10)

                            driver.find_element(By.XPATH, f'//div[@class="ps-content"]//span[contains(text(),"{month_search}")]').click()

                            time.sleep(30)

                            try:
                                doc = driver.find_element(By.XPATH, "//span[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'assets under management')]/ancestor::a[contains(@target,'blank')]")
                            except:
                                break

                            links.append(doc.get_attribute('href'))
                            dates.append(start_date)

                        start_date += relativedelta(months = 1)
                        start_date = ldm(start_date)

                    driver.quit()

                    links_df = pd.DataFrame({'Links': links, 'Relevant_Date' : dates})
                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    for i in range(len(links_df)):

                        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]


                        BUCKET_NAME = 'adqvests3bucket'
                        r =  requests.get(links_df['Links'][0],verify = False,headers={"User-Agent": "XY"})
                        time.sleep(3)
                        file_name = links_df['Links'][0].split('/')[-1]
                        with open(file_name,'wb') as f:
                            f.write(r.content)
                            f.close()


                        data =  open(file_name, 'rb')
                        s3 = boto3.resource(
                            's3',
                            aws_access_key_id=ACCESS_KEY_ID,
                            aws_secret_access_key=ACCESS_SECRET_KEY,
                            config=Config(signature_version='s3v4',region_name = 'ap-south-1')

                        )
                        #Uploading the file to S3 bucket
                        s3.Bucket(BUCKET_NAME).put_object(Key='AMC_Monthly_AAUM/'+file_name, Body=data)
                        data.close()

                        # Deleting the file from the local machine
                        if os.path.exists(file_name):
                            os.remove(file_name)

                        print('done')

                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in AXIS)"
                    error_msg = error_msg + " (Error in AXIS)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def DSP(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'DSP MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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


                    driver.get(url)
                    robot.add_link(url)
                    #driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS)
                    time.sleep(10)
                    no_of_ping += 1
                    driver.maximize_window()

                    links=[]
                    dates_list=[]

                    for i in driver.find_elements(By.PARTIAL_LINK_TEXT ,'Monthly AUM Disclosure for the Month ended ') + driver.find_elements(By.PARTIAL_LINK_TEXT, 'Monthly AUM Disclosure for the Month nded '):
                        links.append(i.get_attribute('href'))
                        dates_list.append(i.text)

                    date=[]
                    operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    for i in range(len(dates_list)):
                        month = re.search(operation, dates_list[i].strip()).group(0)
                        year=re.findall(r'\d+',dates_list[i].strip())[0]
                        month_number = datetime.datetime.strptime(month, '%B').month
                        day=calendar.monthrange(int(year), month_number)[1]
                        date.append(year+'-'+str(month_number)+'-'+str(day))
                    stop_index=date.index('2019-4-30')
                    date=date[:stop_index+1]
                    links=links[:stop_index+1]

                    links_table=pd.DataFrame()
                    links_table['Links']=links
                    links_table['Relevant_Date']=date
                    links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:x.date())
                    print(links_table)

                    print(Latest_Date)
                    links_table = links_table[links_table["Relevant_Date"]>Latest_Date]
                    driver.quit()

                    return links_table
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in DSP)"
                    error_msg = error_msg + " (Error in DSP)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)



        def FRANKLIN(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Franklin Templeton MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    # download_file_path = r"C:\Users\Administrator\AMC"
                    # os.chdir('C:/Users/Administrator/AMC')
                    # prefs = {
                    #     "download.default_directory": download_file_path,
                    #     "download.prompt_for_download": False,
                    #     "download.directory_upgrade": True
                    #     }
                    # options = webdriver.ChromeOptions()
                    # options.add_argument("--disable-infobars")
                    # options.add_argument("start-maximized")
                    # options.add_argument("--disable-extensions")
                    # options.add_argument("--disable-notifications")
                    # options.add_argument('--ignore-certificate-errors')
                    # options.add_argument('--no-sandbox')
                    #
                    # options.add_experimental_option('prefs', prefs)
                    #
                    # driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)
                    # driver.get(url='https://www.franklintempletonindia.com/investor/reports')
                    #
                    # time.sleep(10)
                    #
                    # soup=BeautifulSoup(driver.page_source)
                    # driver.close()
                    # links=[]
                    # dates=[]
                    #
                    # for row in soup.find_all('div',{'id':'MonthlyAverageAssetsunderManagement'})[0].find('div',{'class':'panel-body accordion-content'}):
                    #     for i in row:
                    #         try:
                    #             links.append(i.find('a').get('href'))
                    #             dates.append(i.text.strip('\n'))
                    #         except:
                    #             pass
                    # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
                    # r = requests.get(url, headers = headers)
                    # robot.add_link(url)
                    # data = json.loads(r.content)
                    # # print(data)
                    # for i in data['FirstDropDown']:
                    #     if i['id'] == 'MONTHLY-AVG-ASSETS-UNDER-MGMT':
                    #         for j in i['dataRecords']['linkdata']:
                    #             date = j['dctermsTitle'].strip('\n')
                    #             link = j['literatureHref']
                    #             links.append(link)
                    #             dates.append(date)

                    # links_table=pd.DataFrame()
                    # links_table['Links']=links
                    # links_table['Relevant_Date']=dates
                    # links_table['Relevant_Date'] = np.where(links_table['Relevant_Date'].str.contains('-') , links_table['Relevant_Date'].apply(lambda x: x.split('-')[-1] + ' ' + '20' + x.split('-')[0]), links_table['Relevant_Date'])
                    # # links_table['Month']=links_table['Relevant_Date'].apply(lambda x:x.split(' ')[0])
                    # # links_table['Year']=links_table['Relevant_Date'].apply(lambda x:x.split(' ')[1])
                    # links_table["Relevant_Date"] = links_table['Relevant_Date'].apply(date_value)

                    # links_table['Links']='https://www.franklintempletonindia.com/download'+links_table['Links']
                    # #links_table=links_table.iloc[14:27,]
                    # print(links_table)
                    options = webdriver.ChromeOptions()
                    options.add_argument("--disable-infobars")
                    options.add_argument("start-maximized")
                    options.add_argument("--disable-extensions")
                    options.add_argument("--disable-notifications")
                    options.add_argument('--ignore-certificate-errors')
                    options.add_argument('--no-sandbox')


                    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)


                    driver.get(url)
                    robot.add_link(url)

                    time.sleep(15)




                    docs = driver.find_elements(By.XPATH, '//div[@class="panel-body"]//li')
                    links = []
                    dates = []
                    for doc in docs:

                        link = doc.find_element(By.XPATH,'.//a').get_attribute('href')
                        date = doc.find_element(By.XPATH,'.//span[@class="doc-name"]').text.strip()


                        links.append(link)
                        dates.append(date)

                    driver.quit()

                    links_table = pd.DataFrame({'Links': links, 'Relevant_Date' : dates})
                    links_table['Relevant_Date'] = links_table['Relevant_Date'].apply(lambda x: x.title())
                    links_table['Relevant_Date'] = links_table['Relevant_Date'].apply(date_value)

                    print(Latest_Date)
                    links_table = links_table[links_table["Relevant_Date"]>Latest_Date]

                    return links_table
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in FRANKLIN)"
                    error_msg = error_msg + " (Error in FRANKLIN)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)


        def MIRAE(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Mirae AMC'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    driver.get(url)
                    robot.add_link(url)

                    time.sleep(10)
                    no_of_ping += 1
                    driver.maximize_window()

                    links=[]
                    dates_list=[]
                    for i in driver.find_elements(By.PARTIAL_LINK_TEXT,'Assets Under Management'):
                        links.append(i.get_attribute('href'))
                        dates_list.append(i.text)

                    date=[]
                    operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    for i in range(len(dates_list)):
                        month = re.search(operation, dates_list[i].strip()).group(0)
                        year=re.findall(r'\d+',dates_list[i].strip())[0]
                        month_number = datetime.datetime.strptime(month, '%B').month
                        day=calendar.monthrange(int(year), month_number)[1]
                        date.append(year+'-'+str(month_number)+'-'+str(day))
                    #stop_index=date.index('2019-4-30')
                    #date=date[:stop_index+1]
                    #links=links[:stop_index+1]

                    links_table=pd.DataFrame()
                    links_table['Links']=links
                    links_table['Relevant_Date']=date
                    links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    links_table['Relevant_Date']=links_table['Relevant_Date'].apply(lambda x:x.date())
                    links_table['File_Name']=['MIRAE_AAUM_'+str(x) for x in links_table['Relevant_Date']]


                    print(Latest_Date)

                    links_table = links_table[links_table["Relevant_Date"]>Latest_Date]

                    driver.quit()

                    for i in range(len(links_table)):

                        ACCESS_KEY_ID = adqvest_s3.s3_cred()[0]
                        ACCESS_SECRET_KEY = adqvest_s3.s3_cred()[1]


                        BUCKET_NAME = 'adqvests3bucket'
                        r =  requests.get(links_table.iloc[i,0],verify = False,headers={"User-Agent": "XY"})
                        time.sleep(3)
                        with open(links_table.iloc[i,2],'wb') as f:
                            f.write(r.content)
                            f.close()

                        file_name = links_table.iloc[i,2]
                        data =  open(file_name, 'rb')
                        s3 = boto3.resource(
                            's3',
                            aws_access_key_id=ACCESS_KEY_ID,
                            aws_secret_access_key=ACCESS_SECRET_KEY,
                            config=Config(signature_version='s3v4',region_name = 'ap-south-1')

                        )
                        #Uploading the file to S3 bucket
                        s3.Bucket(BUCKET_NAME).put_object(Key='AMC_Monthly_AAUM/'+file_name, Body=data)
                        data.close()

                        #Deleting the file from the local machine
                        if os.path.exists(file_name):
                            os.remove(file_name)

                        print('done')


                    return links_table
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in MIRAE)"
                    error_msg = error_msg + " (Error in MIRAE)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)


        def sundaram(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Sundaram MF' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    r = requests.get(url)
                    robot.add_link(url)
                    soup = BeautifulSoup(r.content, 'lxml')
                    src = soup.find('div', id = 'AjaxContainer').find_all('script')[1]['src']

                    link_url = f'https://www.sundarammutual.com{src}?_method=AumDisclosure&_session=no'

                    data= {"_method": "AumDisclosure",
                             "_session": "no"}
                    r = requests.post(link_url,data=data)
                    print(r)
                    soup = BeautifulSoup(r.text,'lxml')
                    data = soup.find_all('a')


                    links=[]
                    dates=[]
                    for vals in data:
                        x="https://www.sundarammutual.com"+vals['href'].split("\\")[1].split("'")[-1],vals.text
                        x=list(x)
                        #print(x[0])
                        links.append(x[0])
                        dates.append(" ".join(x[1].split()))
                        #links.append(links[0])

                    links_df=pd.DataFrame()
                    links_df['Links']=links
                    links_df['Relevant_Date']=dates
                    links_df["Relevant_Date"] = links_df['Relevant_Date'].apply(date_value)

                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                    # last_rel_date = Rel_date["Max"][0]
                    # Latest_Date=Rel_date['Max'][0]+datetime.timedelta(1)
                    # #Latest_Date=datetime.date(2022,3, 31)
                    #
                    # print("Last Relevant Date :",last_rel_date)
                    # print("Latest Date        :",Latest_Date)
                    #
                    # def last_day_of_month(date):
                    #     if date.month == 12:
                    #         return date.replace(day=31)
                    #     return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
                    #
                    # print("last_day_of_month  :",last_day_of_month(Latest_Date))
                    #
                    # Latest_Years=format(Latest_Date,'%Y')
                    # Latest_Month_Name=format(Latest_Date,'%B')
                    # Latest_Month_Name_s=format(Latest_Date,'%b')
                    # print(Latest_Years,":",Latest_Month_Name,":",Latest_Month_Name_s)
                    #
                    # links=links_df.loc[links_df['Dates'] == str(Latest_Month_Name+" "+Latest_Years), 'Links'].iloc[0]
                    #
                    # print(str(Latest_Month_Name+" "+Latest_Years),links)
                    #
                    #
                    # links_df=pd.DataFrame()
                    # links_df
                    #
                    # # append columns to an empty DataFrame
                    #
                    # links_df['Links']=[links]
                    # links_df['Relevant_Date']=[last_day_of_month(Latest_Date)]
                    #
                    # links_df

                    #Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Sundaram'",engine)
                    #Latest_Date = Latest_Date["Max"][0]
                    #print(Latest_Date)

                    print(links_df)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Sundaram)"
                    error_msg = error_msg + " (Error in Sundaram)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def TATA(url):
            global no_of_ping
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'TATA MF'",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:

                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\AMC"
                    os.chdir('C:/Users/Administrator/AMC')
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

                    driver.get(url)
                    robot.add_link(url)

                    time.sleep(20)

                    #element = driver.find_element_by_xpath('//*[@id="main-wrap"]/div[2]/section/div/div/div/div[7]')
                    element = driver.find_element(By.XPATH, "//*[contains(text(),'Disclosure for Monthly AAUM')]")


                    desired_y = (element.size['height'] / 2) + element.location['y']
                    current_y = (driver.execute_script('return window.innerHeight') / 2) + driver.execute_script('return window.pageYOffset')
                    scroll_y_by = desired_y - current_y
                    driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
                    time.sleep(10)
                    element = driver.find_element(By.XPATH, "/html/body/app-root/app-layout/div/div/app-statutory-disclosures/div/div[2]/div/span[4]/div[1]/button/img")
                    time.sleep(10)
                    element.click()
                    
                    time.sleep(10)
                    soup = BeautifulSoup(driver.page_source)
                    time.sleep(2)
                    # all_elements = soup.findAll('div', class_ = 'fadv-accord-Content')
                    # links = all_elements[3].findAll('li')

                    links=soup.findAll('a',attrs={'class':'hyperLink ng-tns-c184-1'})

                    dates_list = []
                    for i in links:
                        try:
                            a = i.text
                            a = parser.parse(a, fuzzy = True).strftime('%B %Y')
                            a = date_value(a)
                            a = a.strftime('%Y-%m-%d')
                            dates_list.append(a)
                        except:
                            pass    

                    links_list = []
                    for i in links:
                        try:

                            a = i['href']
                            if 'http' not in a:
                                a = 'https://www.tatamutualfund.com/' + a
                            else:
                                pass
                            links_list.append(a)
                        except:
                            pass    

                    driver.close()


                    links_df=pd.DataFrame()
                    links_df['Links']=links_list
                    links_df['Relevant_Date']=dates_list
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
                    links_df['Relevant_Date']=links_df['Relevant_Date'].apply(lambda x:x.date())
                    print(links_df)

                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in TATA)"
                    error_msg = error_msg + " (Error in TATA)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = 'null'
                    log_amc_wise_errors(error_type,error_msg,run)

        def Invesco(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Invesco' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"

                    # current_year = str(today.year)
                    # if today.month == 1:
                    #     current_year = str(today.year - 1)
                    # url = url + current_year

                    # r = requests.get(url)
                    # robot.add_link(url)
                    # if r.status_code != 200:
                    #     raise Exception('Invalid Response: Status code not 200')
                    # soup = BeautifulSoup(r.content, 'lxml')

                    # data = json.loads(soup.find('p').text)

                    # data = data[0]['DiscloserData']

                    # all_dates = []
                    # all_links = []
                    # operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
                    # for i in data:
                    #     for j in i['DisclosureList']:
                    #         if 'Average AUM' in j['Heading'] or 'AAUM Disclosure' in j['Heading']:
                    #             # date = j['Heading'].replace('&nbsp;','').split('-')[-1].strip()
                    #             date = re.search(operation + ".+(\d{2}|\d{4})", j['Heading']).group(0)
                    #             date = re.sub('[^\w]','',date).strip()
                    #             link = j['Url']
                    #             all_dates.append(date)
                    #             all_links.append(link)

                    # links_df = pd.DataFrame({'Links' : all_links, 'Relevant_Date' : all_dates})
                    # links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(date_value)
                    options = webdriver.ChromeOptions()
                    options.add_argument("--disable-infobars")
                    options.add_argument("start-maximized")
                    options.add_argument("--disable-extensions")
                    options.add_argument("--disable-notifications")
                    options.add_argument('--ignore-certificate-errors')
                    options.add_argument('--no-sandbox')

                    driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options = options)

                    time.sleep(15)

                    driver.get(url)
                    robot.add_link(url)

                    time.sleep(15)

                    driver.find_element(By.XPATH, '//div[contains(text(),"Monthly Disclosures")]').click()

                    max_date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name = 'Invesco'",engine)['Max'][0]
                    start_year = max_date.year
                    present_year = today.year


                    years = [i.get_attribute('value') for i in driver.find_elements(By.XPATH,'//ul[@class="yearly"]//li')]

                    links = []
                    dates = []
                    while start_year <= present_year:

                        if str(start_year) in years:

                            driver.find_element(By.XPATH, f'//ul[@class="yearly"]//li[@value="{str(start_year)}"]').click()

                            time.sleep(5)

                            soup = BeautifulSoup(driver.page_source, 'lxml')
                            links = []
                            dates = []
                            for link_box in soup.findAll('div', class_ = "downloadBoxMn"):

                                for link in link_box.findAll('a'):

                                    if 'Average AUM' in link.text:

                                        links.append(link['href'])
                                        dates.append(link.text.strip())
                        start_year += 1

                    links_df = pd.DataFrame({'Links' : links, 'Relevant_Date' : dates})
                    links_df.drop_duplicates(inplace = True)

                    links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(lambda x: re.sub('  +',' ',x.replace('Average AUM','').replace('-','')).strip())

                    links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(date_value)

                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    print(links_df)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Invesco)"
                    error_msg = error_msg + " (Error in Invesco)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)


        def Canara_Robeco(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Canara Robeco' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    if today.month > 4:
                        Year = str(today.year + 1)
                    else:
                        Year = str(today.year)
                    print(Year)

                    data = {
                    'InnerLibraryName': "Disclosure of AAUM",
                    'Year': Year,
                    'pagenumber': '1',
                    'pagesize': '10'
                    }

                    headers = {
                    'accept': 'application/json, text/javascript, */*; q=0.01',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-length': '84',
                    'content-type': 'application/json;charset=UTF-8',
                    'origin': 'https://www.canararobeco.com',
                    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
                    'dnt': '1',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest'
                    }
                    r = requests.post(url, json = data, headers = headers, verify = False)
                    robot.add_link(url)
                    if r.status_code != 200:
                        raise Exception('Invalid Response: Status code not 200')
                    print(r.status_code)
                    print(r.text)
                    data = json.loads(r.content)


                    data = data['GetResult']


                    all_dates = []
                    all_links = []
                    for i in data:
                        date = i['Title'].split('-')[1].strip()
                        link = i['MediaURL']
                        all_dates.append(date)
                        all_links.append(link)

                    links_df = pd.DataFrame({'Links' : all_links, 'Relevant_Date' : all_dates})
                    print(links_df)
                    links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(date_value)
                    Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Canara Robeco' ",engine)
                    Latest_Date = Latest_Date["Max"][0]
                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    print(links_df)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Canara Robeco)"
                    error_msg = error_msg + " (Error in Canara Robeco)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def PPFAS(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'PPFAS' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    r = requests.get(url)
                    robot.add_link(url)
                    if r.status_code != 200:
                        raise Exception('Invalid Response: Status code not 200')
                    soup = BeautifulSoup(r.content, 'lxml')

                    all_data = soup.find('div', id = 'menu1').find_all('div', class_ = 'card')

                    all_links = []
                    all_dates = []
                    for i in all_data:
                        j = i.find_all('a')
                        try:
                            date = j[0].text.split('for')[1].strip()
                            date_value(date)
                            link = 'https://amc.ppfas.com/schemes/assets-under-management/' + j[1]['href']
                            all_dates.append(date)
                            all_links.append(link)
                        except:
                            for k in i.find_all('li'):
                                date = k.text.strip()
                                link = 'https://amc.ppfas.com/schemes/assets-under-management/' + k.a['href']
                                all_dates.append(date)
                                all_links.append(link)



                    links_df = pd.DataFrame({'Links' : all_links, 'Relevant_Date' : all_dates})
                    links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(date_value)

                    print(Latest_Date)
                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    print(links_df)
                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in PPFAS)"
                    error_msg = error_msg + " (Error in PPFAS)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def HSBC(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'HSBC MF' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    r = requests.get(url)
                    robot.add_link(url)

                    soup = BeautifulSoup(r.content, 'lxml')

                    sections = soup.find('div').findAll('section')

                    table = [section.find('tbody').findAll('a') for section in sections if section.find('h2') != None if 'AUM' in section.find('h2').text][0]

                    links_df = pd.DataFrame({'Links' : ['https://www.assetmanagement.hsbc.co.in' + i['href'] for i in table], 'Relevant_Date' : [i.text for i in table]})
                    links_df = links_df[links_df['Relevant_Date'].str.contains('Monthly')]
                    links_df['Relevant_Date'] = links_df['Relevant_Date'].apply(lambda x: date_value(x.split('-')[1].strip()))


                    links_df = links_df[links_df["Relevant_Date"]>Latest_Date]

                    return links_df
                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in HSBC)"
                    error_msg = error_msg + " (Error in HSBC)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = ''
                    log_amc_wise_errors(error_type,error_msg,run)

        def Shriram(url):
            global today
            Latest_Date = pd.read_sql("select max(Relevant_Date) as Max from AMC_MONTHLY_AAUM where AMC_Name= 'Shriram MF' ",engine)
            Latest_Date = Latest_Date["Max"][0]
            # print(Latest_Date)
            if today.month != (Latest_Date + relativedelta(months=1)).month:
                try:
                    chrome_driver =r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
                    download_file_path = r"C:\Users\Administrator\Shriram AMC"
                    os.chdir('C:/Users/Administrator/Shriram AMC')

                    for file in os.listdir():
                        if file.endswith('.xls'):
                            os.remove(file)

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

                    driver.get(url)
                    time.sleep(2)
                    driver.find_element('xpath', "//h4[contains(text(),'Assets Under Management (AUM) Disclosure')]").click()
                    time.sleep(2)

                    soup = BeautifulSoup(driver.page_source)
                    time.sleep(2)

                    dt_text = [i.find('p').text for i in soup.findAll('div', class_='content') if 'AUM as on' in i.find('p').text]
                    temp = [i.find('a')['href'] for i in soup.findAll('div', class_='content') if 'AUM as on' in i.find('p').text]
                        

                    final_df=pd.DataFrame()
                    final_df['Text'] = dt_text
                    final_df['Temp'] = temp

                    final_df.columns=['Text','Temp']
                    final_df.reset_index(drop=True,inplace=True)

                    final_df.dropna(subset=['Text'],axis=0,inplace=True)

                    final_df['Relevant_Date']=final_df['Text'].apply(lambda x:x.split('on')[1].strip())
                    final_df['Relevant_Date']=final_df['Relevant_Date'].apply(lambda x:parser.parse(x.replace('3oth','30th')))
                    # final_df['Relevant_Date']=final_df['Relevant_Date'].apply(lambda x:parser.parse(x))

                    final_df['Relevant_Date']=final_df['Relevant_Date'].apply(lambda x:x.date())


                    final_df=final_df[final_df['Text'].str.lower().str.contains('disclosure of aum')==False]
                    del final_df['Temp']

                    final_df = final_df[final_df["Relevant_Date"]>Latest_Date]
                    final_df.reset_index(drop=True,inplace=True)
                    print(final_df)

                   

                    driver.execute_script("scrollBy(0,-2000);")
                    time.sleep(10)

                    for i in range(len(final_df)):
                        print(final_df['Text'][i])
                        x=final_df['Text'][i]
                        fy=get_financial_year(final_df['Relevant_Date'][i])
                        
                        e1 = driver.find_element(By.XPATH,"//span[@id='select2-select_121-container']") 
                        time.sleep(3)
                        e1.click()
                        
                        option_2023_2024 = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, f"//li[text()='{fy}']"))
                        )
                        option_2023_2024.click()
                        time.sleep(3)

                        element = driver.find_element(By.XPATH,f"//div[@class='content']//p[contains(text(), '{x}')]")
                        # Find the link within the same parent div
                        link = element.find_element(By.XPATH,"./following-sibling::div[@class='content-details']/a")
                        # Click the link
                        link.click()
                        time.sleep(3)

                        file_list=os.listdir()
                        files=[]
                        dates = []
                        for file in file_list:
                            if file.endswith('.xls'):
                                files.append(file)
                                dates.append(final_df['Relevant_Date'][i])

                        links_df=pd.DataFrame()


                        links_df['Links']=files
                        links_df['Relevant_Date']=dates
                        print(links_df)
                        links_df.reset_index(drop=True,inplace=True)

                        links_df = links_df[links_df["Relevant_Date"]>Latest_Date]
                        return links_df

                except:
                    error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                    error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
                    error_type = error_type + " (Error in Shriram)"
                    error_msg = error_msg + " (Error in Shriram)"
                    print(error_type)
                    print(error_msg)
                    if Adqvest_Bot == True:
                        run = "'Adqvest_Bot'"
                    else:
                        run = 'null'
                    log_amc_wise_errors(error_type,error_msg,run)
                    



        #main df
        global no_of_ping
        input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM'",engine)
        # input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM' and AMC_Name in ('Canara Robeco')",engine)
        # input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM' and AMC_Name IN ('Bandhan MF','HDFC AMC','UTI MF','SBI MF','L&T MF','Mirae AMC')",engine)
        # input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM' and AMC_Name IN ('Kotak Mahindra MF','Franklin Templeton MF','Axis AMC','DSP MF', 'Aditya Birla Sun Life MF')",engine)
        # input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM' and AMC_Name IN ('Aditya Birla Sun Life MF')",engine)
        # input_file_df = pd.read_sql("select * from AdqvestDB.AMC_MONTHLY_AAUM_LINKS_TABLE_STATIC where Type = 'Monthly AAUM'",engine)
        print(input_file_df)

        final_df = pd.DataFrame()
        amc_df = pd.DataFrame()
        for j in range(len(input_file_df)):
            print(input_file_df["AMC_Name"][j])
            if(input_file_df["AMC_Name"][j] == "Nippon MF"):
                try:
                    amc_df = Reliance_Nippon(input_file_df["Link"][j])
                    AMC_Error_Check = "Nippon MF"
                except:
                    continue

            elif(input_file_df["AMC_Name"][j] == "HDFC AMC"):
                try:
                    amc_df = HDFC(input_file_df["Link"][j])
                    AMC_Error_Check = "HDFC AMC"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Bandhan MF"):
                try:
                    amc_df = Bandhan(input_file_df["Link"][j])
                    AMC_Error_Check = "Bandhan MF"
                except:
                    continue

            elif(input_file_df["AMC_Name"][j] == "UTI MF"):
                try:
                    amc_df = UTI(input_file_df["Link"][j])
                    AMC_Error_Check = "UTI MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "ICICI Prudential MF"):
                try:
                    amc_df = ICICI(input_file_df["Link"][j])
                    AMC_Error_Check = "ICICI Prudential MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Edelweiss MF"):
                try:
                    amc_df = Edelweiss(input_file_df["Link"][j])
                    AMC_Error_Check = "Edelweiss MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "SBI MF"):
                try:
                    amc_df = SBI_NEW(input_file_df["Link"][j])
                    AMC_Error_Check = "SBI MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Axis AMC"):
                try:
                    amc_df = AXIS(input_file_df["Link"][j])
                    AMC_Error_Check = "Axis AMC"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Mirae AMC"):
                try:
                    amc_df = MIRAE(input_file_df["Link"][j])
                    AMC_Error_Check = "Mirae AMC"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Aditya Birla Sun Life MF"):
                try:
                    amc_df = Aditya_Birla(input_file_df["Link"][j])
                    AMC_Error_Check = "Aditya Birla Sun Life MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Kotak Mahindra MF"):
                try:
                    amc_df = KOTAK(input_file_df["Link"][j])
                    AMC_Error_Check = "Kotak Mahindra MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Franklin Templeton MF"):
                try:
                    amc_df = FRANKLIN(input_file_df["Link"][j])
                    AMC_Error_Check = "Franklin Templeton MF"
                except:
                    continue
            # elif(input_file_df["AMC_Name"][j] == "L&T MF"):
            #     try:
            #         amc_df = LTMF(input_file_df["Link"][j])
            #         AMC_Error_Check = "L&T MF"
            #     except:
            #         continue
            elif(input_file_df["AMC_Name"][j] == "DSP MF"):
                try:
                    amc_df = DSP(input_file_df["Link"][j])
                    AMC_Error_Check = "DSP MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "TATA MF"):
                try:
                    amc_df = TATA(input_file_df["Link"][j])
                    AMC_Error_Check = "TATA MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Sundaram MF"):
                try:
                    amc_df = sundaram(input_file_df["Link"][j])
                    AMC_Error_Check = "Sundaram MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Invesco"):
                try:
                    amc_df = Invesco(input_file_df["Link"][j])
                    AMC_Error_Check = "Invesco"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "PPFAS"):
                try:
                    amc_df = PPFAS(input_file_df["Link"][j])
                    AMC_Error_Check = "PPFAS"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Canara Robeco"):
                try:
                    amc_df = Canara_Robeco(input_file_df["Link"][j])
                    AMC_Error_Check = "Canara Robeco"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "HSBC MF"):
                try:
                    amc_df = HSBC(input_file_df["Link"][j])
                    AMC_Error_Check = "HSBC MF"
                except:
                    continue
            elif(input_file_df["AMC_Name"][j] == "Shriram MF"):
                try:
                    amc_df = Shriram(input_file_df["Link"][j])
                    AMC_Error_Check = "Shriram MF"
                    print(amc_df)
                except:
                    continue
            #amc_df=amc_df.iloc[[1]]
            #amc_df.reset_index(drop=True,inplace=True)
            try:
                if(amc_df.empty == True):
                    print("skipped")
                    continue
            except:
                try:
                    if amc_df==None:
                        print("skipped")
                        continue
                except:
                    raise Exception("Error in Reading DataFrame")
            else:
                print(amc_df.shape)


            try:
                for row in range(len(amc_df)):
                    ssl._create_default_https_context = ssl._create_unverified_context
                    url = amc_df["Links"].iloc[row]
                    Relevant_Date = amc_df['Relevant_Date'].iloc[row]
                    print(url)
                    try:

                        df = pd.read_excel(url)
                    except:
                        try:

                            os.chdir('C:/Users/Administrator/AMC/')

                            try:
                                os.remove("monthly_mf.xlsb")
                            except:
                                pass
                            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
                            #no_of_ping += 1
                            r = requests.get(url,headers = headers)
                            with open('monthly_mf.xlsb', 'wb') as f:
                                    f.write(r.content)
                                    f.close()
                            df = []

                            with open_xlsb("monthly_mf.xlsb") as wb:
                                with wb.get_sheet(1) as sheet:
                                    for row in sheet.rows():
                                        df.append([item.v for item in row])

                            df = pd.DataFrame(df[1:], columns=df[0])
                            os.remove("monthly_mf.xlsb")
                        except:
                            try:
                                os.chdir('C:/Users/Administrator/AMC/')
                                try:
                                    os.remove("monthly_mf.xls")
                                except:
                                    pass
                                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
                                #no_of_ping += 1
                                r = requests.get(url,headers = headers)
                                with open('monthly_mf.xls', 'wb') as f:
                                        f.write(r.content)
                                        f.close()

                                file_name = r'C:\Users\Administrator\AMC\monthly_mf.xls'

                                df = pd.read_excel(file_name)
                                os.remove("monthly_mf.xls")
                            except:
                                try:
                                    os.chdir('C:/Users/Administrator/AMC/')
                                    # os.chdir(r"C:\Users\Abdulmuizz\Desktop\ADQVest\AMC\AMC_MONTHLY_AAUM\Download path")
                                    try:
                                        os.remove("monthly_mf.xlsx")
                                    except:
                                        pass
                                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'}
                                    #no_of_ping += 1
                                    r = requests.get(url,headers = headers)
                                    with open('monthly_mf.xlsx', 'wb') as f:
                                            f.write(r.content)
                                            f.close()

                                    file_name = 'C:/Users/Administrator/AMC/monthly_mf.xlsx'

                                    df = pd.read_excel(file_name)
                                    os.remove("monthly_mf.xlsx")
                                except:

                                    try:

                                        file_name = "C:/Users/Administrator/AMC/monthly_mf.xlsx"
                                        wingsbook = xlwings.Book(file_name)
                                        wingsapp = xlwings.apps.active
                                        wingsbook.save(file_name)
                                        wingsapp.quit()

                                        df = pd.read_excel(file_name)
                                        os.remove("monthly_mf.xlsx")

                                    except:

                                        with open("C:/Users/Administrator/AMC/monthly_mf.xlsx","rb") as f:
                                            xlsx = XLSX(f)
                                            df = xlsx.read(0)
                                        os.remove("monthly_mf.xlsx")




                    df.columns = list(range(df.shape[1]))
                    df.dropna(how='all',axis=1,inplace=True)
                    df1=df[(df[0].isnull())&(df[1].isnull())]
                    df1.dropna(how='all',axis=0,inplace=True)
                    df1.reset_index(drop=True,inplace=True)
                    if df1.iloc[: , -1:].iloc[0].isna().bool():
                        col_index = df1.iloc[: , -1:].iloc[0].index[0]
                        df1.at[0, col_index] = 'GRAND TOTAL'
                    else:
                        pass
                    df1=df1.ffill(axis=1)
                    df1=df1.dropna(how='all',axis=1)
                    df1.reset_index(drop=True,inplace=True)
                    df1.dropna(how='any',axis=0,inplace=True)
                    df1.reset_index(drop=True,inplace=True)
                    df1 = df1.loc[~(df1==0).all(axis=1)]
                    if(df1.shape[0]==4 or df1.shape[0]==5):

                        for i in df1.columns:
                            df1[i] = np.where((df1[i].map(str).str.lower().str.contains('through direct plan')), ' DP ' , df1[i])
                            df1[i] = np.where((df1[i].map(str).str.lower().str.contains('through non - associate')), ' RP_Non_Associate ' , df1[i])
                            df1[i] = np.where((df1[i].map(str).str.lower().str.contains('through associate')), ' RP_Associate ' , df1[i])
                            if input_file_df["AMC_Name"][j] == "Invesco":
                                df1[i] = np.where((df1[i].map(str).str.lower().str.contains('through non   associate')), ' RP_Non_Associate ' , df1[i])
                            else:
                                pass

                            df1[i] = np.where((df1[i]=='I'), ' Sponsor ' , df1[i])
                            df1[i] = np.where((df1[i]=='II'), ' Non_Sponsor ' , df1[i])


                            df1[i] = np.where(((df1[i]==1) | (df1[i]=='1')), ' RI ' , df1[i])
                            df1[i] = np.where(((df1[i]==2) | (df1[i]=='2')), ' COR ' , df1[i])
                            df1[i] = np.where(((df1[i]==3) | (df1[i]=='3')), ' B_FI ' , df1[i])
                            df1[i] = np.where(((df1[i]==4) | (df1[i]=='4')), ' FII_FPI ' , df1[i])
                            df1[i] = np.where(((df1[i]==5) | (df1[i]=='5')), ' HNI ' , df1[i])
                    else:
                        raise Exception('format changed')
                    columns = (df1.iloc[0] + df1.iloc[1] + df1.iloc[2] + df1.iloc[3]).to_dict()
                    df.drop(labels=0,inplace=True)
                    df=df.rename(columns=columns)
                    try:
                        df['Check'] = np.where((df[0].isin([chr(x) for x in list(range(65,65 + 25))])), df[1],np.nan)
                        df['Check'] = df['Check'].ffill()
                        df['Check'] = np.where((df['Check'].str.lower().str.contains('debt')), 'Debt',df['Check'])
                        df['Check'] = np.where((df['Check'].str.lower().str.contains('equity')), 'Equity',df['Check'])
                    except:
                        df['Check'] = np.where((df[0].isin([f'{chr(x)})' for x in list(range(65,65 + 25))])), df[1],np.nan)
                        df['Check'] = df['Check'].ffill()
                        df['Check'] = np.where((df['Check'].str.lower().str.contains('debt')), 'Debt',df['Check'])
                        df['Check'] = np.where((df['Check'].str.lower().str.contains('equity')), 'Equity',df['Check'])

                    df['Category'] = None
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('other')),df['Check'],df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('gilt')),'Gilt',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('liquid')),'Liquid',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('fmp')),'FMP',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('debt')),df['Check'],df['Category'])

                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('elss')),'ELSS',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('balanced scheme')),'Balanced',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('etf')),'ETF',df['Category'])
                    df['Category'] = np.where((df[0].notnull()) & (df[0].isin([chr(x) for x in list(range(65,65 + 25))])==False) & (df[1].str.lower().str.contains('fund of fund')),'FOF',df['Category'])
                    df['Category'] = df['Category'].ffill()

                    df = df[(df[0].isnull())]
                    df=df[df[1].str.lower().str.contains('total')==False]
                    df.insert(2,'Scheme_Type',value=df['Category'])
                    df.drop(['Category','Check'],axis=1,inplace=True)
                    df.drop([0],axis=1,inplace=True)
                    df = df.rename(columns={1:'Scheme_Name'})
                    df.columns = df.columns.map(clean_col)
                    df.drop(df.columns[df.columns.str.lower().str.contains('grand_total')],axis=1,inplace=True)
                    df = df[df['Scheme_Name'].str.lower().str.contains('scheme name')==False]

                    df.reset_index(drop=True,inplace=True)
                    try:
                        df = df.iloc[:df[df['Scheme_Name'].str.lower().str.contains('top 30 cities as identified by')].index[0]]
                    except:
                        pass
                    # df = df.apply(pd.to_numeric,errors = 'ignore')
                    for i in df.columns[df.columns.str.contains('DP_')]:
                        df[i] = pd.to_numeric(df[i], errors='coerce')
                    for i in df.columns[df.columns.str.contains('RP_')]:
                        df[i] = pd.to_numeric(df[i], errors='coerce')
                    df['Total_DP_AAUM_Cr'] = df[df.columns[df.columns.str.contains('DP_')]].sum(axis=1)
                    df['Total_RP_AAUM_Cr'] = df[df.columns[df.columns.str.contains('RP_')]].sum(axis=1)
                    df['Total_AAUM_Cr'] = df[['Total_DP_AAUM_Cr', 'Total_RP_AAUM_Cr']].sum(axis=1)
                    df.reset_index(drop=True,inplace=True)
                    df["AMC_Name"] = input_file_df["AMC_Name"][j]

                    columns_before = df.columns.to_list()
                    df.columns = [x.replace("15","30") for x in df.columns.to_list()]
                    columns_after = df.columns.to_list()
                    if(columns_before == columns_after):
                        df["Comments"] = ""
                    else:
                        df["Comments"] = "Top 15 and Bottom 15 cities instead of Top 30 and Bottom 30"

                    df = df[df["DP_T30_Sponsor_RI"].notnull()]    #works for uti mf

                    df['Relevant_Date'] = Relevant_Date

                    df['Runtime']=pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    column_list=df.columns.tolist()
                    for column in column_list:
                        #print(column)
                        try:
                            df[column]=df[column].apply(lambda x: x.lstrip())
                            df[column]=df[column].apply(lambda x: x.rstrip())
                        except:
                            continue
                    print(input_file_df["AMC_Name"][j])
                    print(df.head())
                    print(df.shape)
                    df = df[df['Scheme_Name'] != 'None']

                    final_df = pd.concat([final_df,df])
                    final_df = final_df.dropna(axis = 0)
            except:
                error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno) + " when running (" + AMC_Error_Check + ") for table extraction"
                error_type = cleaner.full_clean(error_type)
                error_msg = cleaner.full_clean(error_msg)
                print(error_type)
                print(error_msg)
                if Adqvest_Bot == True:
                    run = "'Adqvest_Bot'"
                else:
                    run = ''
                log_amc_wise_errors(error_type,error_msg,run)
                continue



        if(final_df.shape[0]!=0):
            print("Data available")
            final_df["AMC_Name"]=final_df["AMC_Name"].replace({'Sundaram':'Sundaram MF'})
            final_df=drop_duplicates(final_df)
            final_df.to_sql(name = "AMC_MONTHLY_AAUM" ,if_exists = "append",con = engine,index = False)
            #print(final_df)
            log.job_end_log(table_name,job_start_time,no_of_ping)
        else:
            print("Data not available")
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        try:
             driver.quit()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
