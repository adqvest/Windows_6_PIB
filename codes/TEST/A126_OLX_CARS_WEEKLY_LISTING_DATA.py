from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import datetime as datetime
from pytz import timezone
import re
import csv
import time
import io
import os
import sqlalchemy
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


#%%
def splchar(x):
    try:
        return re.sub('[^A-Za-z0-9]+', '', x)
    except:
        return None

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'USED_CARS_LISTING_WEEKLY_DATA_2_TABLES'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
        Last_Relevant_Date = pd.read_sql("Select max(Relevant_Date) as Max from USED_CARS_LISTING_OVERALL_WEEKLY_DATA where Source = 'OLX' ",engine)
        Last_Relevant_Date = Last_Relevant_Date["Max"][0]
        if(today.date() - Last_Relevant_Date >= datetime.timedelta(7)):

            ######## OVERALL ########
            driver_path =r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
            driver = webdriver.Chrome(driver_path)
            driver.maximize_window()
            driver.implicitly_wait(5)
            driver.get("https://www.olx.in/cars_c84")
            time.sleep(5)
            soup=BeautifulSoup(driver.page_source,'lxml')
            overall_list = soup.findAll("li",attrs = {"class":"_2uB4V"})
            overall_list = overall_list[0].get_text()
            overall_list = overall_list.split()
            overall_count = overall_list[1]
            overall_list = overall_list[0]
            overall_count = splchar(overall_count)
            print(overall_list)
            print(overall_count)

            overall_df = pd.DataFrame({"Count":[overall_count]})
            overall_df["Source"] = "OLX"
            overall_df["Relevant_Date"] = today.date()
            overall_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            overall_df.to_sql(name = "USED_CARS_LISTING_OVERALL_WEEKLY_DATA",if_exists = 'append',con = engine,index = False)
            print(overall_df.head())
            print('\nUSED_CARS_LISTING_OVERALL_WEEKLY_DATA To SQL rows: ', len(overall_df))
        else:
            print('\nUSED_CARS_LISTING_OVERALL_WEEKLY_DATA already present')

        ######## City Wise Counts #########
        max_rel_date_city = pd.read_sql("SELECT max(Relevant_Date) as max from USED_CARS_LISTING_CITYWISE_WEEKLY_DATA WHERE Source = 'OLX'", con=engine)['max'][0]
        
        if today.date() - max_rel_date_city >= datetime.timedelta(7):
            st_ut_list = ['Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chhattisgarh','Goa','Gujarat','Haryana','Himachal Pradesh','Jharkhand','Karnataka','Kerala','Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttaranchal','West Bengal', 'Andaman and Nicobar Islands', 'Chandigarh', 'Dadra & Nagar Haveli', 'Daman & Diu', 'Delhi', 'Jammu & Kashmir', 'Lakshadweep', 'Puducherry']
            city_wise_count_df = pd.DataFrame()
            makewise_count_df = pd.DataFrame()

            driver_path =r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
            options = webdriver.ChromeOptions()
            options.add_argument("--disable-infobars")
            options.add_argument("--start-maximized")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-notifications")
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('--allow-insecure-localhost')
            driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
            driver.implicitly_wait(5)
            driver.get("https://www.olx.in/cars_c84")
            time.sleep(5)

            for st in st_ut_list:
                limit = 0
                while True:
                    try:
                        time.sleep(2)
                        driver.find_element(By.XPATH, '//input[@placeholder="Search city, area or locality"]').clear()
                        time.sleep(5)
                        driver.find_element(By.XPATH, '//input[@placeholder="Search city, area or locality"]').send_keys(st)
                        time.sleep(5)
                        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//div[@data-aut-id="locationItem"]'))).click()
                        time.sleep(5)
                        soup = BeautifulSoup(driver.page_source, 'lxml')

                        city_list = soup.findAll("a", attrs={"class": "_1HnJ6 _2OYDZ _2L7t8"})
                        city_list = [x for x in city_list if 'India' not in x["data-aut-id"]]
                        city_name_list = [x.text for x in city_list]
                        print(city_name_list)

                        city_df = pd.DataFrame({"City": city_name_list, "Counts": [re.findall(r"\((.*)\)", x) for x in city_name_list], 'State':st})
                        city_wise_count_df = pd.concat([city_wise_count_df, city_df])
                        driver.find_element(By.XPATH, '//input[@placeholder="Search city, area or locality"]').clear()
                        time.sleep(5)
                        break
                    except:
                        limit += 1
                        driver.refresh()
                        time.sleep(5)
                        driver.find_element(By.XPATH, '//input[@placeholder="Search city, area or locality"]').clear()
                        if limit > 3:
                            print('\nWebsite Loading issue: Rerun\n')

            city_wise_count_df["City"] = city_wise_count_df["City"].apply(lambda x : re.sub(pattern = r"\((.*)\)",repl = "",string = x))
            city_wise_count_df["City"] = city_wise_count_df["City"].apply(lambda x: x.strip())

            city_wise_count_df["Counts"] = city_wise_count_df["Counts"].apply(lambda x: str(x))
            city_wise_count_df["Counts"] = np.where(city_wise_count_df["Counts"] == '[]','[0]',city_wise_count_df["Counts"])
            city_wise_count_df["Counts"] = city_wise_count_df["Counts"].apply(lambda x : re.sub(pattern = r"\['|\']",repl = "",string = x))
            city_wise_count_df["Counts"] = city_wise_count_df["Counts"].apply(lambda x: re.findall(r"[0-9]+",x)[0])
            city_wise_count_df["Counts"] = city_wise_count_df["Counts"].apply(lambda x : int(x))

            city_wise_count_df = city_wise_count_df[city_wise_count_df["Counts"] != 0]
            city_wise_count_df["Source"] = "OLX"
            city_wise_count_df["Relevant_Date"] = today.date()
            city_wise_count_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            city_wise_count_df = city_wise_count_df.rename(columns={"Counts": "Count"})
            city_wise_count_df.to_sql(name = "USED_CARS_LISTING_CITYWISE_WEEKLY_DATA",if_exists = 'append',con = engine,index = False)
            print(city_wise_count_df.head())
            print('\nUSED_CARS_LISTING_CITYWISE_WEEKLY_DATA To SQL rows: ', len(city_wise_count_df))
        else:
            print("\nUSED_CARS_LISTING_CITYWISE_WEEKLY_DATA already present")

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + " line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)
if(__name__=='__main__'):
    run_program(run_by='manual')
