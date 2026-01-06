# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:24:30 2020

@author: abhis
"""

import scrapy
import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import os
#os.chdir(r"C:\Adqvest\Mercadolibre")
import sqlalchemy
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import requests
import pandas as pd
import numpy as np
import datetime
import sys
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from dateutil import parser
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from random import randint
from time import sleep
import time
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



def make_link(x):

    if len(x.split())>=2:
        x = x.split()
        link = x[0].lower()+'-'+ x[2].lower()

    else:
        link = x.lower()

    return link

def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'NINETY_NINE_ACRES_PROPERTIES_COUNT_WEEKLY_DATA'
    scheduler = ''
    global no_of_ping
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        Last_Relevant_Date = pd.read_sql('Select max(Relevant_Date) as Max from NINETY_NINE_ACRES_PROPERTIES_COUNT_WEEKLY_DATA',engine)
        Last_Relevant_Date = Last_Relevant_Date["Max"][0]
        if(today.date() - Last_Relevant_Date >= datetime.timedelta(0)):



            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
            driver=webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")

            start_url = 'https://www.99acres.com/'

            driver.get(start_url)

            element = driver.find_element_by_id("selectcityheader").click()
            cities = driver.find_elements_by_xpath("//a[@class='cityLinkInHeader']")
            cities = [a.get_attribute("text") for a in cities]

            cities = [make_link(x) for x in cities]

            driver.close()

            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

            cities = ['Delhi-NCR','Mumbai','Bangalore','Chennai','Hyderabad','Pune','Kolkata','Ahmedabad','Chandigarh','Agra','Allahabad','Amritsar','Anand','Aurangabad',
                    'Belgaum','Bharuch','Bhopal','Bhubaneswar','Coimbatore','Daman-Diu','Dehradun','Dharwad','Durgapur',
                    'Goa','Guntur','Guwahati','Gwalior','Haridwar','Hubli','Indore','Jabalpur','Jaipur',
                    'Jalandhar','Jamshedpur','Jodhpur','Kanpur','Karnal','Kerala','Kolhapur','Lucknow','Ludhiana','Madurai','Mangalore','Mathura',
                    'Meerut','Mysore','Nadiad','Nagpur','Nasik','Panipat','Patna','Pondicherry','Raipur',
                    'Rajkot','Ranchi','Ratnagiri','Salem','Sonipat', 'Surat','Trichy','Udaipur', 'Vadodara','Valsad','Vapi','Varanasi','Vijayawada','Visakhapatnam']

            cities = [x.lower() for x in cities]

            '''

            Residental

            '''

            for city in cities:

                url_buy = 'https://www.99acres.com/property-for-buy-in-'+city+'-ffid'
                url_rent = 'https://www.99acres.com/property-for-rent-in-'+city+'-ffid'
                url_pg = 'https://www.99acres.com/property-for-pg-in-'+city+'-ffid'
                url_project = 'https://www.99acres.com/property-for-project-in-'+city+'-ffid'

                urls = [url_buy,url_rent,url_pg,url_project]

                values = []
                for url in urls:

                    sleep(randint(0,10))

                    r = requests.get(url,headers = headers,verify = False)

                    soup = BeautifulSoup(r.text,'html.parser')

                    try:
                       listings = soup.find_all('div',{'class':'title_semiBold r_srp__spacer20'})[0].text.split()[0]
                    except:
                        raise Exception('ERROR CANNOT FIND LISTINGS')

                    values.append(listings)

                data = {
                        'Property_Type' : 'Residential',
                        'Category': ['Buy Listings','Rent Listings','PG Listings','Project Listings'],
                        'Counts' : values,
                        'City': city,
                        'Relevant_Date': today.date(),
                        'Runtime' : pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

                        }

                data = pd.DataFrame(data)

                print(data)



            '''

            Commercial

            '''


            for city in cities:

                url_all_commercial = 'https://www.99acres.com/commercial-property-in-'+city+'-ffid'
                url_lease_1 = 'https://www.99acres.com/rent-ready-to-move-office-space-in-'+city+'-ffid'
                url_lease_2 = 'https://www.99acres.com/commercial-property-for-rent-in-'+city+'-ffid?property_type=6,82,9,16,20,10,83,12,15,17,18,19,81'
                url_commercial_project = 'https://www.99acres.com/new-commercial-projects-in-'+city+'-ffid'

                urls = [url_all_commercial,url_lease_1,url_lease_2,url_commercial_project]

                values = []

                for url in urls:

                    sleep(randint(0,10))

                    r = requests.get(url,headers = headers,verify = False)

                    soup = BeautifulSoup(r.text,'html.parser')

                    try:
                        listings = soup.find_all('div',{'class':'title_semiBold r_srp__spacer20'})[0].text.split()[0]
                    except:
                        try:
                            listings = soup.find_all('b',{'id':'resultCount'})[0].text.split()[0]
                        except:
                            raise Exception('ERROR CANNOT FIND LISTINGS')

                    values.append(listings)

                data = {
                        'Property_Type' : 'Residential',
                        'Category': ['Buy Listings','Lease Listings','Project Listings'],
                        'Counts' : [values[0],values[1]+values[2],values[3]],
                        'City': city.title(),
                        'Relevant_Date': today.date(),
                        'Runtime' : pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))

                        }

                data = pd.DataFrame(data)

                print(data)
        else:
            print("No new data")


        log.job_end_log(table_name,job_start_time, no_of_ping)




    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])




        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
