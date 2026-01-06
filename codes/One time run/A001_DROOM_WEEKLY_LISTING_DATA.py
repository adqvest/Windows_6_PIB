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
#os.chdir(r'D:\Adqvest\ncdex')
import sqlalchemy
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import JobLogNew as log
import adqvest_db

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

#%%


#%%

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
    table_name = 'USED_CARS_LISTING_WEEKLY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)
#%%
        # Last_Relevant_Date = pd.read_sql('Select max(Relevant_Date) as Max from NINETY_NINE_ACRES_PROPERTIES_COUNT_WEEKLY_DATA',engine)
        # Last_Relevant_Date = Last_Relevant_Date["Max"][0]
        # if(today.date() - Last_Relevant_Date <= datetime.timedelta(7)):

        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        url_list = ["https://droom.in/bikes/used?search=true","https://droom.in/scooters/used?search=true"]
        overall_complete_df = pd.DataFrame()
        citywise_complete_df = pd.DataFrame()
        makewise_complete_df = pd.DataFrame()
        for url in url_list:
            print(url)
            r = requests.get(url,headers = headers)
            no_of_ping += 1
            soup = BeautifulSoup(r.content,"lxml")
            try:
                count = soup.findAll(text = re.compile("Find(.*)Used Bikes"))[0]
                count = re.match(r"Find(.*)Used Bikes available",count)[0]
            except:
                try:
                    count = soup.findAll(text = re.compile("Find(.*)Used Scooters"))[0]
                    count = re.match(r"Find(.*)Used Scooters available",count)[0]
                except:
                    print("skipped")
                    continue

            count = re.findall(r"\d+",count)[0]

            overall_df = pd.DataFrame({"Count":[count]})
            overall_df["Source"] = "Droom"
            overall_df["Relevant_Date"] = today.date()
            overall_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            overall_complete_df = pd.concat([overall_complete_df,overall_df])



            options = Options()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            city_list = soup.findAll("input",attrs = {"class":"location-filter"})





            for city in city_list:
                try:
                    print(city["data-city"])

                    #driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                    if('bike' in url):
                        r = requests.get("https://droom.in/bikes/used?page=1&tab=grid&bucket=bike&location="+city["data-city"]+"&condition=used",headers = headers)
                        # driver.get("https://droom.in/bikes/used?page=1&tab=grid&bucket=bike&location="+city["data-city"]+"&condition=used")
                        # driver.maximize_window()
                        # time.sleep(1)
                    else:
                        r = requests.get("https://droom.in/scooters/used?page=1&tab=grid&bucket=scooter&location="+city["data-city"]+"&condition=used",headers = headers)
                        # driver.get("https://droom.in/scooters/used?page=1&tab=grid&bucket=scooter&location="+city["data-city"]+"&condition=used")
                        # driver.maximize_window()
                        # time.sleep(1)
                    no_of_ping += 1
                    soup = BeautifulSoup(r.content,"lxml")
                    # soup = BeautifulSoup(driver.page_source,"lxml")
                    # driver.quit()
                    try:
                        count = soup.findAll(text = re.compile("Find(.*)Used Bikes"))[0]
                        count = re.match(r"Find(.*)Used Bikes available",count)[0]
                    except:
                        try:
                            count = soup.findAll(text = re.compile("Find(.*)Used Scooters"))[0]
                            count = re.match(r"Find(.*)Used Scooters available",count)[0]
                        except:
                            print("skipped")
                            continue
                    count = re.findall(r"\d+",count)[0]

                    citywise_df = pd.DataFrame({"Count":[count]})
                    citywise_df["City"] = city["data-city"]
                    citywise_df["Source"] = "Droom"
                    citywise_df["Relevant_Date"] = today.date()
                    citywise_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    citywise_complete_df = pd.concat([citywise_complete_df,citywise_df])
                except:
                    print("skipped")
                    continue

                make_list = soup.findAll("input",attrs = {"class":re.compile("(.*)make-filter")})

                for make in make_list:
                    print(make["data-make"])

                    try:
                        #driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                        if('bike' in url):
                            r = requests.get("https://droom.in/bikes/used?page=1&tab=grid&bucket=bike&location="+city["data-city"]+"&condition=used&make="+make["data-make"],headers = headers)
                            # driver.get("https://droom.in/bikes/used?page=1&tab=grid&bucket=bike&location="+city["data-city"]+"&condition=used&make="+make["data-make"])
                            # driver.maximize_window()
                            # time.sleep(1)
                        else:
                            r = requests.get("https://droom.in/scooters/used?page=1&tab=grid&bucket=scooter&location="+city["data-city"]+"&condition=used&make="+make["data-make"],headers = headers)
                            # driver.get("https://droom.in/scooters/used?page=1&tab=grid&bucket=scooter&location="+city["data-city"]+"&condition=used&make="+make["data-make"])
                            # driver.maximize_window()
                            # time.sleep(1)
                        no_of_ping += 1
                        soup = BeautifulSoup(r.content,"lxml")
                        # soup = BeautifulSoup(driver.page_source,"lxml")
                        # driver.quit()

                        try:
                            count = soup.findAll(text = re.compile("Find(.*)Used Bikes"))[0]
                            count = re.match(r"Find(.*)Used Bikes available",count)[0]
                        except:
                            count = soup.findAll(text = re.compile("Find(.*)Used Scooters"))[0]
                            count = re.match(r"Find(.*)Used Scooters available",count)[0]


                            count = re.findall(r"\d+",count)[0]


                        makewise_df = pd.DataFrame({"Count":[count]})
                        makewise_df["City"] = city["data-city"]
                        makewise_df["Make"] = make["data-make"]
                        makewise_df["Source"] = "Droom"
                        makewise_df["Relevant_Date"] = today.date()
                        makewise_df["Runtime"] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                        makewise_complete_df = pd.concat([makewise_complete_df,makewise_df])
                    except:
                        print("skipped")
                        continue






        overall_complete_df["Count"] = overall_complete_df["Count"].apply(lambda x: int(x))
        overall_complete_df = overall_complete_df.groupby(by = ["Source","Relevant_Date","Runtime"])["Count"].sum().reset_index()


        citywise_complete_df["City"] = citywise_complete_df["City"].apply(lambda x: x.title())
        citywise_complete_df["Count"] = citywise_complete_df["Count"].apply(lambda x: int(x))
        citywise_complete_df = citywise_complete_df.groupby(by = ["Source","Relevant_Date","City","Runtime"])["Count"].sum().reset_index()

        makewise_complete_df["City"] = makewise_complete_df["City"].apply(lambda x: x.title())
        makewise_complete_df["Count"] = makewise_complete_df["Count"].apply(lambda x: int(x))
        makewise_complete_df = makewise_complete_df.groupby(by = ["Source","Relevant_Date","City","Make","Runtime"])["Count"].sum().reset_index()

        overall_complete_df.to_sql(name = "USED_BIKES_LISTING_OVERALL_WEEKLY_DATA",index = False,con = engine,if_exists = 'append')
        citywise_complete_df.to_sql(name = "USED_BIKES_LISTING_CITYWISE_WEEKLY_DATA",index = False,con = engine,if_exists = 'append')
        makewise_complete_df.to_sql(name = "USED_BIKES_LISTING_MAKEWISE_WEEKLY_DATA",index = False,con = engine,if_exists = 'append')



        log.job_end_log(table_name,job_start_time, no_of_ping)



#%%
    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)




        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
