# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 17:21:28 2021

@author: abhis
"""
import json
import scrapy
import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import sys
import os
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
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
sys.path.insert(0, r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
import adqvest_db
import JobLogNew as log


def get_json(soup):
    scripts = soup.find_all('script')
    script = []
    for a in scripts:
        try:
            print(a) # See what you try to load
            data = json.loads(a.text)
            print("Success")
            script.append(data)
        except Exception as e:
            print("Not Successful because {}".format(e)) # Print additional information

    script1 = []
    for a in scripts:
        try:
            print(a) # See what you try to load
            data = a.text
            print("Success")
            script1.append(data)
        except Exception as e:
            print("Not Successful because {}".format(e))

    test = scripts[22].get_text()
    "window.__myx" in test

    test = test.split("window.__myx = ")[1]
    # convert to dictionary
    data = json.loads(test)

    return data



def run_program(run_by='Adqvest_Bot', py_file_name=None):
#    os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MYNTRA_FASHION_FOOTWEAR_CATEGORY'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try:
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

        download_file_path = r'C:\Users\Administrator\AdQvestDir'
        prefs = {
                    "download.default_directory": download_file_path,
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    'profile.default_content_setting_values.automatic_downloads': 1
                    }
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs', prefs)

        #options.add_experimental_option('prefs', prefs)
        #options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",options = options)

#
#        con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
#        engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')
        url1 = 'https://www.myntra.com/girls-footwear?p=1&plaEnabled=false&sort=popularity'
        url2 = 'https://www.myntra.com/boys-footwear?p=1&plaEnabled=false&sort=popularity'
        url3 = 'https://www.myntra.com/men-footwear?plaEnabled=false&sort=popularity'
        url4 = 'https://www.myntra.com/women-footwear?plaEnabled=false&sort=popularity'

        list_urls = [url1,url2,url3,url4]

        main = pd.DataFrame()
        for url in list_urls:
            print(url)
            driver.get(url)

            time.sleep(4)

            soup = driver.page_source

            soup = BeautifulSoup(soup)

            data = get_json(soup)

            total = data['searchData']['results']['totalCount']
            nos = 50

            pages = round(total/nos)

            for i in range(2,pages+1):
                print(i)
                url = 'https://www.myntra.com/girls-footwear?p='+str(i)+'&plaEnabled=false&sort=popularity'

                print(url)
                time.sleep(2)
                #r = requests.get(url, verify = False , headers = headers)
                driver.get(url)
                soup = driver.page_source

                soup = BeautifulSoup(soup)

                data = get_json(soup)


#                soup = BeautifulSoup(r.text,'lxml')

                data = get_json(soup)

                data = data['searchData']['results']['products']

                for prds in data:
                    try:
                        del prds['attributeTagsPriorityList']
                    except:
                        pass
                    try:
                        del prds['images']
                    except:
                            pass
                    try:
                        del prds['inventoryInfo']
                    except:
                        pass
                    try:
                        del prds['productVideos']
                    except:
                        pass
                    try:
                        del prds['systemAttributes']
                    except:
                        pass

                    df = pd.DataFrame(prds,index=[0])
                    df = df.dropna(axis=1)
                    main = pd.concat([main,df])


                    df.columns = ['Landing pageurl', 'Loyalty points enabled', 'Adid', 'Ispla', 'Product id',
                       'Product', 'Productname', 'Rating', 'Ratingcount', 'Isfastfashion',
                       'Future discounted price', 'Future discount startdate', 'Discount', 'Brand',
                       'Searchimage', 'Effective discount percentage after tax',
                       'Effective discount amount after tax', 'Buy button winner sku id',
                       'Buy button winner seller partner id', 'Related styles count',
                       'Related styles type', 'Sizes', 'Gender', 'Primary colour',
                       'Discount label', 'Discount display label', 'Additional info', 'Category',
                       'Mrp', 'Price', 'Advance order tag', 'Color variant available',
                       'Product image tag', 'List views', 'Discount type', 'Tdb xgy text',
                       'Catalog date', 'Season', 'Year', 'Is personalised', 'Eor spic kstag',
                       'Personalized coupon', 'Personalized coupon value', 'Product meta']

                    df.columns = [x.title() for x in list(df.columns)]

                    df.columns = [x.replace(" ","_") for x in list(df.columns)]

                    df = df[['Landing_Pageurl', 'Loyalty_Points_Enabled', 'Product', 'Productname', 'Rating', 'Ratingcount',
                       'Isfastfashion', 'Future_Discounted_Price', 'Future_Discount_Startdate',
                       'Discount', 'Brand',
                       'Effective_Discount_Percentage_After_Tax',
                       'Effective_Discount_Amount_After_Tax', 'Related_Styles_Count',
                       'Related_Styles_Type', 'Sizes', 'Gender', 'Primary_Colour',
                       'Discount_Label', 'Discount_Display_Label', 'Additional_Info',
                       'Category', 'Mrp', 'Price',
                       'Color_Variant_Available', 'Product_Image_Tag',
                       'Discount_Type', 'Catalog_Date', 'Season', 'Year',
                       'Is_Personalised']]

                    df['Relevant_Date'] = datetime.date(2020,3,31)
                    df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))


                    df.to_sql(name= 'MYNTRA_FASHION_FOOTWEAR_CATEGORY',con=engine,if_exists='append',index=False)

                    print("UPLOADED")
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
