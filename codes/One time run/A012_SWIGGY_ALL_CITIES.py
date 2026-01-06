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


# In[ ]:




def restaurant_details(req, city):
    remove_digits = str.maketrans('', '', digits)
    name = req.findAll('div', class_ = 'nA6kb')[0].text
    title = req.findAll('div', class_ = '_1gURR')[0].text
    avg_cost = req.find('div', class_ = 'nVWSi').text

    try:
        ratings = req.find('div', class_ = ['_9uwBC']).text
        ratings = float(ratings)
    except:
        ratings = np.nan

    try:
        offer = req.find('div', class_='Zlfdx').text
    except:
        offer = None


    return [name, title, avg_cost, ratings, offer, city]

def get_restaurant_data(url, city):
    global no_of_ping
    data = []
    no_of_ping += 1

    driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
    driver.get(url)
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    driver.quit()



    area = soup.findAll('div', class_='_1c_iw')[0].findAll('a', class_='_15mJL')
    links = []
    for req in area:

        area_url = "http://www.swiggy.com" + req['href']
        area_name = req.text
        links.append([area_url, area_name])

    for link in links:
        limit = 0
        page = 1
        count = 0
        while True:
            try:

                url = link[0] + "?page=" + str(page)
                print(url)

                driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                driver.get(url)
                time.sleep(1)

                driver.quit()
                restaurants = soup.findAll('div',class_='_3XX_A')
                if (restaurants == []):
                    raise Exception("breaked get_restaurant_data")




            except:
                break
            soup = BeautifulSoup(driver.page_source, 'lxml')
            restaurants = soup.findAll('div',class_='_3XX_A')
            if (restaurants == []):
                count = count + 1
                print(count)
            for req in restaurants:
                ls = restaurant_details(req, city)
                ls.append(link[1])
                data.append(ls)

            page += 1
            if(count>5):
                break
            time.sleep(0.7)

        return data

#################################################################################
no_of_ping = 0
scheduler = ''
os.chdir('C:/Users/Administrator/AdQvestDir/')
engine = adqvest_db.db_conn()
connection = engine.connect()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

## job log details
job_start_time = datetime.datetime.now(india_time)
table_name = 'SWIGGY_ALL_CITIES'
#################################################################################

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    global no_of_ping


    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]



    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        page_url = "https://www.swiggy.com/"

        driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
        driver.get(page_url)
        time.sleep(1.5)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        driver.quit()



        cities_list = soup.findAll(class_ = "_3TjLz b-Hy9")
        print(cities_list)
        if(cities_list == []):
            raise Exception("SWIGGY_WEEKLY_UPDATION_STATUS not updated")
        swiggy_cities_df = pd.DataFrame({"Cities":cities_list})
        swiggy_cities_df["Cities"] = swiggy_cities_df["Cities"].apply(lambda x: x.text)
        swiggy_cities_df["Status"] = np.nan
        swiggy_cities_df["Relevant_Date"] = np.nan
        swiggy_cities_df["Runtime"] = np.nan
        connection.execute("delete from SWIGGY_WEEKLY_UPDATION_STATUS")
        connection.execute("commit")
        swiggy_cities_df.to_sql(name = "SWIGGY_WEEKLY_UPDATION_STATUS",if_exists="append",con = engine,index = False)
        print("SWIGGY_WEEKLY_UPDATION_STATUS updated")
        time.sleep(3)






        swiggy_cities_df = pd.read_sql("select * from SWIGGY_WEEKLY_UPDATION_STATUS order by Cities",engine)

        cities_df = swiggy_cities_df[swiggy_cities_df["Status"].isnull()]

        cities_df.reset_index(drop = True,inplace = True)



        for i in range(len(cities_df)):

            url = 'http://www.swiggy.com/'+cities_df["Cities"][i]



            city = cities_df["Cities"][i].title()
            time.sleep(1)
            try:
                try:
                    data = get_restaurant_data(url, city)
                except:

                    url_no_localities = url
                    data = []
                    links = []

                    driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                    driver.get(url)
                    time.sleep(1)
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    driver.quit()

                    limit = 0
                    page = 1
                    count = 0
                    while True:
                        try:
                            url = url_no_localities+"?page=" + str(page)
                            print(url)
                            no_of_ping += 1
                            driver = webdriver.Chrome(executable_path = r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
                            driver.get(url)
                            time.sleep(1)
                            soup = BeautifulSoup(driver.page_source, 'lxml')
                            driver.quit()
                            restaurants = soup.findAll('div',class_='_3XX_A')
                            if (restaurants == []):
                                raise Exception("no more pages")

                        except:
                            break
                        #soup = BeautifulSoup(r.content, 'lxml')
                        restaurants = soup.findAll('div',class_='_3XX_A')
                        if (restaurants == []):
                            count = count + 1
                            print(count)
                        for req in restaurants:
                            ls = restaurant_details(req, city)
                            ls.append(None)
                            data.append(ls)

                        page += 1
                        if(count>5):
                            break
                        time.sleep(0.7)
            except:
                print("skipped")
                print(city)
                continue


            SWIGGY = pd.DataFrame(data, columns=['Restaurant_Name', 'Restaurant_Type', 'Average_Cost', 'Ratings', 'Offer', 'City', 'Locality'])
            SWIGGY['Restaurant_Name'] = SWIGGY['Restaurant_Name'].str.title()
            SWIGGY['Restaurant_Type'] = SWIGGY['Restaurant_Type'].str.title()
            SWIGGY['Average_Cost'] = SWIGGY['Average_Cost'].str.replace('₹','Rs ')
            SWIGGY['Relevant_Date'] = yesterday.date()
            SWIGGY["Relevant_Week"] = yesterday.date().strftime("%V")
            SWIGGY["Relevant_Week"] = SWIGGY["Relevant_Week"].apply(lambda x:"Week "+ x +"-"+yesterday.date().strftime("%Y"))
            SWIGGY['Runtime'] = pd.to_datetime(yesterday.strftime('%Y-%m-%d %H:%M:%S'))
            SWIGGY.drop('Offer', axis=1,inplace=True)

            SWIGGY["City"] = SWIGGY["City"].apply(lambda x:x.title())
            SWIGGY["Locality"] = np.where((SWIGGY["Locality"].isnull()),"",SWIGGY["Locality"])
            limit = 0
            while True:
                try:
                    SWIGGY.to_sql(name='SWIGGY_ALL_CITIES', if_exists='append', con=engine, index=False)
                    time.sleep(0.5)
                    connection.execute("update SWIGGY_WEEKLY_UPDATION_STATUS set Relevant_Date ='"+today.strftime("%Y-%m-%d")+"',Status = 'Completed',Runtime='"+today.strftime('%Y-%m-%d %H:%M:%S')+"' where Cities='"+cities_df["Cities"][i] +"'")
                    connection.execute("commit")

                    print("uploaded")
                    break
                except:
                    limit = limit + 1
                    time.sleep(5)
                    if(limit < 8):
                        continue
                    else:
                        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
                        error_msg = str(sys.exc_info()[1])
                        raise Exception(error_msg)
                        #raise Exception("SQL Connection Error")
            time.sleep(5)


            print(SWIGGY.shape)

        count =  pd.read_sql("select count(*) as Count from SWIGGY_WEEKLY_UPDATION_STATUS where Status is null",engine)
        count = count["Count"][0]
        if(count != 0):
            raise Exception("Not Completed for all cities")
        connection.close()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
