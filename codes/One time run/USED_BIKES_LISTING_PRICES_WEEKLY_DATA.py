import sqlalchemy
import pandas as pd
import calendar
import os
import requests
from bs4 import BeautifulSoup
import re
import datetime as datetime
from pytz import timezone
import numpy as np
import time
import sys
from string import digits
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import statistics

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options


# In[ ]:






#################################################################################
no_of_ping = 0
scheduler = ''
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
connection = engine.connect()

#################################################################################

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'USED_BIKES_LISTING_PRICES_WEEKLY_DATA'


    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]



    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        option = Options()
        option.add_argument("--disable-infobars")
        option.add_argument("start-maximized")
        # option.add_argument("--disable-extensions")
        # option.add_argument("--disable-notifications")
        # option.add_argument('--ignore-certificate-errors')


        driver = webdriver.Chrome(executable_path= "chromedriver.exe",chrome_options = option)

        driver.get("https://www.olx.in/motorcycles_c81")

        driver.maximize_window()

        time.sleep(1)

        limit = 0
        while True:
            try:
                driver.get("https://www.olx.in/motorcycles_c81")
                #driver.refresh()

                time.sleep(2)

                soup = BeautifulSoup(driver.page_source)

                pattern = re.compile("(.*)make(.*)")

                make_list = soup.findAll("label",attrs = {'for':pattern})
                if(make_list != []):
                    break
                else:
                    raise Exception
            except:
                limit += 1
                if(limit < 20):
                    continue
                else:
                    break


        #make_list = make_list[1:]

        year_list = ['2015','2016','2017','2018','2019','2020']

        make_list = [x.text for x in make_list]
        make_list = [re.sub(r'\((.*)\)','',x) for x in make_list]
        make_list = [x.strip() for x in make_list ]
        #make_list = [x for x in make_list if ((x!= "Yamaha")&(x!= "Bajaj")&(x!= "Royal Enfield"))]
        print(make_list)
        # make_list = [x for x in make_list if x == 'Hero']

        for make in make_list:
            driver.find_element_by_xpath("//*[@title='"+make+"']").click()
            time.sleep(1.5)
            print(make)

            soup = BeautifulSoup(driver.page_source)

            pattern = re.compile("(.*)model(.*)")

            model_list = soup.findAll("label",attrs = {'for':pattern})

            #make_list = make_list[1:]

            model_list = [x.text for x in model_list]
            model_list = [re.sub(r'\((.*)\)','',x) for x in model_list]
            model_list = [x.strip() for x in model_list]

            for model in model_list: #change this later
                time.sleep(1)
                try:
                    driver.find_element_by_xpath("//*[@title='"+model+"']").click()
                    time.sleep(1.5)
                except:
                    continue
                print(model)


                for year in year_list:
                    print(year)
                    driver.find_elements_by_xpath("//*[@type = 'number']")[0].clear()
                    time.sleep(1)
                    driver.find_elements_by_xpath("//*[@type = 'number']")[0].send_keys(year)
                    time.sleep(1)
                    driver.find_elements_by_xpath("//*[@type = 'number']")[1].clear()
                    time.sleep(1)
                    driver.find_elements_by_xpath("//*[@type = 'number']")[1].send_keys(year)
                    time.sleep(1)
                    driver.find_element_by_xpath("//*[@data-aut-id='apply-filter-btn']").click()

                    scroll_limit = 0
                    while scroll_limit <= 5:
                        try:
                            driver.find_element_by_xpath("//*[@data-aut-id='btnLoadMore']").click()
                            scroll_limit += 1
                            time.sleep(2)
                        except:
                            scroll_limit += 1
                            pass


                    soup = BeautifulSoup(driver.page_source)

                    price_list = soup.findAll("span",attrs = {'data-aut-id':'itemPrice'})
                    price_list = [x.text.replace(",","").replace("₹","").strip() for x in price_list]
                    price_list = [int(x) for x in price_list]

                    try:
                        no_of_listings = soup.findAll("p",attrs = {'id':'adsResultsIn'})[0].text
                        no_of_listings = no_of_listings.replace(",","")
                        no_of_listings = int(re.findall(r'\d+',no_of_listings)[0])
                    except:
                        driver.back()
                        time.sleep(0.5)
                        continue


                    mean = statistics.mean(price_list)
                    median = statistics.median(price_list)
                    sample_size = len(price_list)

                    olx_df = pd.DataFrame({"Mean":[mean],"Median":[median],"Sample_Size":[sample_size],"No_Of_Listings":[no_of_listings]})
                    olx_df["Make"] = make
                    olx_df["Model"] = model
                    olx_df["Year"] = year
                    olx_df["Source"] = "OLX"
                    olx_df['Relevant_Date'] = pd.to_datetime(today.strftime("%Y-%m-%d"))
                    olx_df['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    olx_df.to_sql(name = "USED_BIKES_LISTING_PRICES_WEEKLY_DATA",con = engine,index = False,if_exists = 'append')
                    print("uploaded")



                    driver.back()
                driver.back()
            driver.get("https://www.olx.in/motorcycles_c81")
            time.sleep(1)

        #driver.quit()









        # connection.close()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)



        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
