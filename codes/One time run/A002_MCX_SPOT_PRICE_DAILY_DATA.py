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

def find_ext(dr, ext):
    return glob(path.join(dr,"*.{}".format(ext)))

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MCX_SPOT_PRICE_DAILY_DATA'
    if (py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
        chrome_driver_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe"
        download_file_path = "C:\\Users\\Administrator\\Junk folder\\"
        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
        }
        options.add_experimental_option('prefs', prefs)
        os.chdir('C:\\Users\\Administrator\\AdQvestDir\\Junk folder\\')
        driver = webdriver.Chrome(executable_path=chrome_driver_path, chrome_options=options)
        driver.get("https://www.mcxindia.com/market-data/spot-market-price")
        driver.find_element_by_xpath("//*[@class='today']").click()
        r = requests.get("https://www.mcxindia.com/market-data/spot-market-price")
        soup = BeautifulSoup(r.content)
        items = soup.find_all('li', attrs={'class': 'rcbItem'})
        items_list = []
        for i in items:
            items_list.append(i.get_text())
        start_index = items_list.index('ALMOND')
        end_index = items_list.index('ZINCMINI')
        items_list_new = items_list[start_index:end_index + 1]
        for i in items_list_new:
            max_rel_date = pd.read_sql('select max(Relevant_Date) as Max from AdqvestDB.MCX_SPOT_PRICE_DAILY_DATA where Commodity="' + i + '"',engine).Max[0]
            max_rel_date += datetime.timedelta(days=1)
            driver.find_element_by_xpath('//*[@class="hasDatepick"]').click()
            select_from_year = driver.find_element_by_xpath('//*[@title="Change the year"]')
            for option in select_from_year.find_elements_by_tag_name("option"):
                if option.text == str(max_rel_date.year):
                    option.click()
                    break
            select_from_month = driver.find_element_by_xpath('//*[@title="Change the month"]')
            for option in select_from_month.find_elements_by_tag_name("option"):
                if option.text == datetime.datetime.strftime(max_rel_date, '%B'):
                    option.click()
                    break
            driver.find_element_by_xpath('//a[text()="' + str(max_rel_date.day) + '"]').click()
            time.sleep(1)
            driver.find_element_by_xpath('//*[@id="txtToDate"]').click()
            cur_year = datetime.datetime.today().year
            cur_month = datetime.datetime.today().strftime('%B')
            cur_day = datetime.datetime.today().day
            select_to_year = driver.find_element_by_xpath('//*[@title="Change the year"]')
            for option in select_to_year.find_elements_by_tag_name("option"):
                if option.text == str(cur_year):
                    time.sleep(1)
                    option.click()
                    break
            select_to_month = driver.find_element_by_xpath('//*[@title="Change the month"]')
            for option in select_to_month.find_elements_by_tag_name("option"):
                if option.text == cur_month:
                    option.click()
                    time.sleep(2)
                    break
            select_to_date = driver.find_element_by_xpath('//a[text()="' + str(cur_day) + '"]').click()
            time.sleep(5)
            driver.find_element_by_xpath('//*[@id="ctl00_cph_InnerContainerRight_C004_ddlSymbols_Input"]').clear()
            driver.find_element_by_xpath('//*[@id="ctl00_cph_InnerContainerRight_C004_ddlSymbols_Input"]').send_keys(i)
            driver.find_element_by_xpath('//*[@id="btnShowArchive"]').click()
            time.sleep(5)
            try:
                driver.find_element_by_xpath('//*[@id="cph_InnerContainerRight_C004_lnkExportToExcel"]/img').click()
                time.sleep(10)
                files = find_ext(".", "xls")
                files = [x.partition("\\")[2] for x in files]
                files = files[0]
                df = pd.read_html(files)[0]
                os.remove(files)
                df.drop(columns=['Up/Down'], inplace=True)
                df.rename(columns={'Spot Price(Rs.)': 'Spot_Price_INR', 'Date': 'Relevant_Date'}, inplace=True)
                df['Relevant_Date'] = df['Relevant_Date'].apply(
                    lambda x: datetime.datetime.strptime(x, '%d %b %Y').date())
                df["Runtime"] = datetime.datetime.now()
                df.to_sql("MCX_SPOT_PRICE_DAILY_DATA", index=False, if_exists='append', con=engine)
                time.sleep(5)
                print(df)
            except:
                pass
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
