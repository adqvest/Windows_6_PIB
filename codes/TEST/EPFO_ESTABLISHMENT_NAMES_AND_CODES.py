from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as datetime
import re
import os
import xlwings as xw
import easyocr
import cv2
import requests
import warnings
from calendar import monthrange
warnings.filterwarnings('ignore')
from dateutil import relativedelta, parser
import sqlalchemy
from selenium import webdriver
from pytz import timezone
import time
import shutil
import sys

from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver.common.by import By
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import ClickHouse_db
import JobLogNew as log
from adqvest_robotstxt import Robots
robot = Robots(__file__)


def captcha_solver():
    image = cv2.imread("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    _, black_and_white_image = cv2.threshold(gray_image, 127, 255, cv2.THRESH_BINARY)

    cv2.imwrite("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png", black_and_white_image)
    image = cv2.imread("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
    reader = easyocr.Reader(['en'])
    result = reader.readtext(image, detail = 0)
    combined_text = ' '.join(result).replace(' ','').strip().upper()
    return combined_text

def fetchResuslts(co_name,driver):
    os.chdir("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA")
    element = driver.find_element(By.XPATH, "//input[@id = 'estName']")
    element.clear()
    time.sleep(5)
    element.send_keys(co_name)
    time.sleep(2)
    img =  driver.find_element(By.XPATH,"//img[@id = 'capImg']")
    time.sleep(3)
    img.screenshot("C:/Users/Administrator/AdQvestDir/EPFO_CAPTCHA/captcha.png")
    text = captcha_solver()
    if text=='':
        element = driver.find_element(By.CSS_SELECTOR, "input[value='Reset']")
        element.click()
    else:
        element = driver.find_element(By.XPATH, "//input[@id = 'captcha']")
        element.clear()
        element.send_keys(text)
        element = driver.find_element(By.XPATH, "//*[@id='searchEmployer']")
        element.click()
    return driver

def download_file(driver):
    os.chdir("C:/Users/Administrator/Temp Folder")
    limit = 0
    while True:
        try:
            driver.find_element(By.XPATH,"//a[@class='dt-button buttons-excel buttons-html5']").click()
            break
        except:
            limit = limit + 1
            print('error')
            if(limit>8):
                break

            time.sleep(3)
    time.sleep(5)
    return driver

def file_transfer(co_name):
    os.chdir("C:/Users/Administrator/Temp Folder")

    filename=os.listdir()[0]
    co_name=co_name.replace(" ","_")
    os.rename(filename,f"{co_name}.xlsx")
    print("File Name : ",filename)
    filename=os.listdir()[0]
    source_directory =r"C:/Users/Administrator/Temp Folder"
    destination_directory = r"C:/Users/Administrator/Establishment Files"


    source_file_path = source_directory + '/' + filename
    destination_file_path = destination_directory + '/' + filename
    shutil.move(source_file_path, destination_file_path)

    os.chdir("C:/Users/Administrator/Temp Folder")
    for file in os.listdir():
        print(file)
        os.remove(file)



def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    # job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = ''
    if(py_file_name is None):
       py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        download_file_path = r"C:\Users\Administrator\Temp Folder"
        chrome_driver = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'

        prefs = {
            "download.default_directory": download_file_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True
            }
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', prefs)

        query = "select Company_Name from EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA where Sl_No>0 and Establishment_Download is Null"
        company_names = pd.read_sql(query,con=engine)
        for co in company_names['Company_Name']:

           
            # co_name = co.replace(".",'').strip()
            co_name=co
            print("Company: ",co_name)
            driver = webdriver.Chrome(executable_path=chrome_driver, chrome_options=options)

            driver.maximize_window()

            driver.get("https://unifiedportal-epfo.epfindia.gov.in/publicPortal/no-auth/misReport/home/loadEstSearchHome")

            soup=''
            driver=fetchResuslts(co_name,driver)

            soup=BeautifulSoup(driver.page_source)
            while ('Please enter valid captcha' in soup.find('div',{'id':'tablecontainer'}).text or soup.find('div',{'id':'tablecontainer'}).text == '' or soup == ''):
               driver=fetchResuslts(co_name,driver)
               time.sleep(5)
               soup=BeautifulSoup(driver.page_source)

            if('No details found for this criteria. Please enter valid Establishment name or code number .' in soup.text):
                engine.execute(f'UPDATE EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA SET Establishment_Download = "Not Available" where Company_Name = "{co}";')
                continue
            try:
                total_pages = int(soup.find('div',{'id':'example_info'}).text.split('of')[-1].strip())
            except:
                total_pages = 1

            # for x in range(1,total_pages+1):
            #     print("Check Details: ",x)
            #     if(total_pages > 1):
            #         sub_count1=0
            #         while True:

            #             try:
            #                 driver.find_element(By.XPATH, f"//a[@data-dt-idx='{x}']").click()
            #                 time.sleep(5)
            #                 break
            #             except:
            #                 sub_count1=sub_count1+1
            #                 if sub_count1>10:
            #                     break
            '''Excel file downloaidng'''
            driver=download_file(driver)
            time.sleep(5)
            os.chdir("C:/Users/Administrator/Temp Folder")
            file_transfer(co_name)
            engine.execute(f'UPDATE EPFO_ESTABLISHMENTS_PAYMENT_STATUS_DATA SET Establishment_Download = "Done" where Company_Name = "{co}";')
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)
        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')
