import requests
import sys
import warnings
from selenium import webdriver
import os
import time
from bs4 import BeautifulSoup
import numpy as np
import re
from pytz import timezone
from pandas.io import sql
from calendar import monthrange
import datetime as datetime
import pandas as pd
import sqlalchemy
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log


def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'HEALTH_CARE'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        chrome_driver_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = r"C:\Users\Administrator\Junk_One_Time folder"
        options=webdriver.ChromeOptions()
        prefs = {
                    "download.default_directory": download_file_path,
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True
                    }
        options.add_experimental_option('prefs', prefs)
        options.add_argument('--ignore-certificate-errors')

        page1=[str(i).zfill(2) for i in np.arange(2,50)]
        page2=[str(i).zfill(2) for i in np.arange(2,100)]
        page3=[str(i).zfill(2) for i in np.arange(2,12)]
        page4=[str(i).zfill(2) for i in np.arange(2,4)]

        def month_number(month_name):
            try:
                month_num=datetime.datetime.strptime(month_name,'%B').month
            except:
                month_num=datetime.datetime.strptime(month_name,'%b').month
            return month_num

        def end_date(year,month):
            end_dt = datetime.datetime(year, month, monthrange(year, month)[1]).date()
            return end_dt

        years=[]
        operation = "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        driver = webdriver.Chrome(executable_path=chrome_driver_path, chrome_options=options)
        driver.get(url='https://nrhm-mis.nic.in/hmisreports/frmstandard_reports.aspx')
        driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl06_imgDir").click()
        time.sleep(1)
        driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl05_imgDir").click()
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source)
        for a in soup.find_all('span', id=re.compile(r'lblName')):
            years.append(a.get_text())
        for i in range(14,len(years)+2):
            i=str(i)
            var = driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl" + i + "_lblName").text
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl" + i + "_imgDir").click()
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl02_imgDir").click()
            try:
                for j in page1:
                    var1 = driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl" + j + "_lblName").text
                    driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl" + j + "_imgDir").click()
                    for k in page2:
                        try:
                            var2 = driver.find_element_by_id(
                                "ctl00_ContentPlaceHolder1_gridDirList_ctl" + k + "_lblName").text
                            driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridDirList_ctl" + k + "_imgDir").click()
                            links = []
                            state = []
                            year = []
                            district = []
                            file_name = []
                            try:
                                for l in page3:
                                    var3 = driver.find_element_by_xpath(
                                        "//*[@id='ctl00_ContentPlaceHolder1_gridFileList']/tbody/tr[" + l + "]/td[2]").text
                                    file_name.append(var3)
                                    driver.find_element_by_id(
                                        "ctl00_ContentPlaceHolder1_gridFileList_ctl" + l + "_imgFrd").click()
                                    driver.switch_to.window(driver.window_handles[-1])
                                    element = driver.find_element_by_id('form1')
                                    var4 = element.get_attribute('action')
                                    links.append(var4)
                                    year.append(var)
                                    state.append(var1)
                                    district.append(var2)
                                    driver.close()
                                    driver.switch_to.window(driver.window_handles[-1])
                                driver.find_element_by_xpath("//*[@id='ctl00_ContentPlaceHolder1_gridFileList']/tbody/tr[12]/td/table/tbody/tr/td[2]/a").click()
                                for a in page4:
                                    var5 = driver.find_element_by_xpath("//*[@id='ctl00_ContentPlaceHolder1_gridFileList']/tbody/tr[" + a + "]/td[2]").text
                                    file_name.append(var5)
                                    driver.find_element_by_id("ctl00_ContentPlaceHolder1_gridFileList_ctl" + a + "_imgFrd").click()
                                    driver.switch_to.window(driver.window_handles[-1])
                                    element = driver.find_element_by_id('form1')
                                    var6 = element.get_attribute('action')
                                    links.append(var6)
                                    year.append(var)
                                    state.append(var1)
                                    district.append(var2)
                                    driver.close()
                                    driver.switch_to.window(driver.window_handles[-1])
                            except:
                                pass
                            driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnBack").click()
                            years = [int(i.split('-')[0]) for i in year]
                            date_df = pd.DataFrame(file_name)
                            date_df.columns = ['File_Name']
                            date_df['Month'] = date_df['File_Name'].apply(lambda x: re.search(operation, x).group(0))
                            date_df['Month_number'] = date_df['Month'].apply(lambda x: month_number(x))
                            date_df["Year"] = years
                            for i in range(date_df['Month'].shape[0]):
                                if date_df['Month'].iloc[i] == 'January':
                                    date_df['Year'].iloc[i] += 1
                                elif date_df['Month'].iloc[i] == 'February':
                                    date_df['Year'].iloc[i] += 1
                                elif date_df['Month'].iloc[i] == 'March':
                                    date_df['Year'].iloc[i] += 1
                            date_df['Relevant_Date'] = date_df.apply(lambda x: end_date(x.Year, x.Month_number), axis=1)
                            date_df['Link'] = links
                            date_df['State'] = state
                            date_df['District'] = district
                            date_df['File_Name_Ref'] = np.nan
                            date_df['Status'] = np.nan
                            date_df['Download_Status'] = np.nan
                            date_df['Comments'] = np.nan
                            date_df['Runtime'] = datetime.datetime.now()
                            date_df['Last_Updated'] = datetime.datetime.now()
                            date_df['State'] = state
                            date_df['District'] = district
                            for i in range(len(date_df)):
                                date_df['File_Name_Ref'].iloc[i] = str(date_df['Year'].iloc[i]) + '_' + date_df['Month'].iloc[i] + '_' + \
                                                                   date_df['State'].iloc[i] + '_' + date_df['District'].iloc[i]
                            date_df_final = date_df.copy()
                            date_df_final.drop(columns=['Year', 'Month', 'Month_number'], axis=1, inplace=True)
                            date_df_final = date_df_final[
                                ['File_Name', 'File_Name_Ref', 'Link', 'Status', 'Download_Status', 'Comments', 'Relevant_Date',
                                 'Runtime', 'Last_Updated']]
                            max_rel_date=pd.read_sql("select max(Relevant_Date) as Max from HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS",engine)['Max'][0]
                            date_df_final=date_df_final[date_df_final['Relevant_Date']>max_rel_date]
                            if date_df_final.empty is False:
                                date_df_final.to_sql("HEALTHCARE_MONTHLY_RAW_DATA_FILES_LINKS", index=False, if_exists='append', con=engine)
                        except:
                            pass
                    driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnBack").click()
            except:
                pass
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnBack").click()
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnBack").click()

        log.job_end_log(table_name, job_start_time, no_of_ping)
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)
        a=input()
        print(a)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)


if(__name__=='__main__'):
    run_program(run_by='manual')
