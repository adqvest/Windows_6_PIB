# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 13:03:48 2021

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
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import random
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
#sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log
engine = adqvest_db.db_conn()
#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days

def get_json(soup):
    scripts = soup.find_all('script')
    script = []
    for a in scripts:
        try:
            #print(a) # See what you try to load
            data = json.loads(a.text)
            print("Success")
            script.append(data)
        except:
            print("Not Successful because") # Print additional information

    script1 = []
    for a in scripts:
        try:
            #print(a) # See what you try to load
            data = a.text
            if "window.__myx" in data:
                print("Success")
                script1.append(data)
        except:
            print("Not Successful because") # Print additional information
    try:
        test = script[22].get_text()
        "window.__myx" in test

        test = test.split("window.__myx = ")[1]
        # convert to dictionary
        data = json.loads(test)

    except:
        test = script1[1]#.get_text()
        "window.__myx" in test

        test = test.split("window.__myx = ")[1]
        # convert to dictionary
        data = json.loads(test)

    return data

def get_brands(x):
    data = x
    df = pd.DataFrame()
    reqd_data = data['searchData']['results']['filters']['primaryFilters']
    catg_name = data['searchData']['pageTitle']
    depth = data['searchData']['results']['appliedParams']['filters']
    sub_catgs = []
    for ranges in depth:
        sub_catgs.append(ranges['enrichedValues'][0]['displayTitle'])
#        sub_catgs.append(ranges['values'][-1])
    for vals in reqd_data:
        if vals['id']=='Brand':
            values = vals['filterValues']
            values = pd.DataFrame(values)
            df = pd.concat([df,values])

    df = df.iloc[:,[0,2]]
    cols = ['Brand','Count']
    df.columns = cols
    df['Category'] = catg_name
    try:
        sub_catgs = ["Women" if x == 'men women' else x for x in sub_catgs]
        sub_catgs = ["Girl" if x == 'boys girls' else x for x in sub_catgs]
    except:
        pass

    if sub_catgs == []:
        df['Sub_Category_1'] = None
        df['Sub_Category_2'] = None
    elif len(sub_catgs) == 1:
        df['Sub_Category_1'] = sub_catgs[0].title()
        df['Sub_Category_2'] = None
    elif len(sub_catgs) == 2:
        df['Sub_Category_1'] = sub_catgs[1].title()
        df['Sub_Category_2'] = sub_catgs[0].title()
    else:
        for i in range(len(sub_catgs)):
    #        if sub_catgs[i] in ["women","men","boys","girls",'men women']:
    #        if i > 0:
    #        if len(sub_catgs)>1:
            try:
                df['Sub_Category_'+str(i+1)] = sub_catgs[i].title()
            except:
                df['Sub_Category_'+str(i+1)] = sub_catgs[i]
#        else:
#            df['Sub_Category_'+str(i+1)] = sub_catgs[i]

    df.to_sql(name= 'MYNTRA_DAILY_BRAND_DATA_1',con=engine,if_exists='append',index=False)
    print("Brands Updated")
    return df


def get_categories(x):
    data = x
    df = pd.DataFrame()
    reqd_data = data['searchData']['results']['filters']['primaryFilters']
    catg_name = data['searchData']['pageTitle']
    depth = data['searchData']['results']['appliedParams']['filters']
    sub_catgs = []
    for ranges in depth:
        sub_catgs.append(ranges['enrichedValues'][0]['displayTitle'])
#        sub_catgs.append(ranges['values'][-1])
    for vals in reqd_data:
        if vals['id']=='Categories':
            values = vals['filterValues']
            values = pd.DataFrame(values)
            df = pd.concat([df,values])
    df['Category'] = catg_name
    df = df.iloc[:,[0,2]]
    cols = ['Category_Name','Count']
    df.columns = cols
    try:
        sub_catgs = ["Women" if x == 'men women' else x for x in sub_catgs]
        sub_catgs = ["Girl" if x == 'boys girls' else x for x in sub_catgs]
    except:
        pass

    if sub_catgs == []:
        df['Sub_Category_1'] = None
        df['Sub_Category_2'] = None
    elif len(sub_catgs) == 1:
        df['Sub_Category_1'] = sub_catgs[0].title()
        df['Sub_Category_2'] = None
    elif len(sub_catgs) == 2:
        df['Sub_Category_1'] = sub_catgs[1].title()
        df['Sub_Category_2'] = sub_catgs[0].title()
    else:
        for i in range(len(sub_catgs)):
    #        if sub_catgs[i] in ["women","men","boys","girls",'men women']:
    #        if i > 0:
    #        if len(sub_catgs)>1:
            try:
                df['Sub_Category_'+str(i+1)] = sub_catgs[i].title()
            except:
                df['Sub_Category_'+str(i+1)] = sub_catgs[i]
#        else:
#            df['Sub_Category_'+str(i+1)] = sub_catgs[i]

    df.to_sql(name= 'MYNTRA_DAILY_CATEGORIES_DATA_1',con=engine,if_exists='append',index=False)
    print("Categories Updated")
    return df

def get_sub_catgs(data,url):
    catgs = data.sort_values("Count",ascending=False)

    dive = catgs.head(8)
    dive_catgs = list(dive.iloc[:,0])

    dive = []
    url = url#'https://www.myntra.com/clothing?f=Gender%3Amen%20women%2Cwomen&plaEnabled=false'
    for vals in dive_catgs:
       a = url.split("=")
       if len(a)==3:
           dive.append(a[0]+"=Categories%3A"+vals+"%3A%3A"+a[1]+"="+a[2])
       elif len(a)==4:
           dive.append(a[0]+"=Categories%3A"+vals+"%3A%3A"+a[1]+"="+a[2]+"="+a[3])
       elif len(a)==5:
           dive.append(a[0]+"=Categories%3A"+vals+"%3A%3A"+a[1]+a[2]+a[3]+a[4])

    return dive

def get_driver():
    download_file_path = r'C:\Users\Administrator\AdQvestDir\chromedriver.exe'
    prefs = {
                "download.default_directory": download_file_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                'profile.default_content_setting_values.automatic_downloads': 1
                }
    option = webdriver.ChromeOptions()

    option.add_experimental_option('prefs', prefs)

    #option = Options()
    option.add_argument("--disable-infobars")
    option.add_argument("start-maximized")
    option.add_argument("--disable-extensions")
    option.add_argument("--disable-notifications")
    option.add_argument('--ignore-certificate-errors')
    option.add_argument('--no-sandbox')


    driver = webdriver.Chrome(executable_path=r"C:\Users\Administrator\AdQvestDir\chromedriver.exe",chrome_options=option)

    return driver


def get_data(x):
    data = x
    main = pd.DataFrame()
    #data = get_json(soup)
    df = pd.DataFrame()
    reqd_data = data['searchData']['results']['filters']['primaryFilters']
    catg_name = data['searchData']['pageTitle']
    depth = data['searchData']['results']['appliedParams']['filters']
    sub_catgs = []
    for ranges in depth:
        sub_catgs.append(ranges['enrichedValues'][0]['displayTitle'])
#        sub_catgs.append(ranges['values'][-1])

    try:
        data = data['searchData']['results']['products']
    except:
        pass
    try:
        count = data['searchData']['results']['totalCount']
    except:
        count = None

    for l in range(len(data)):# in data:
        prds = data[l]
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
        try:
            sub_catgs = ["Women" if x == 'men women' else x for x in sub_catgs]
            sub_catgs = ["Girl" if x == 'boys girls' else x for x in sub_catgs]
        except:
            pass
        if sub_catgs == []:
            df['Sub_Category_1'] = None
            df['Sub_Category_2'] = None
        elif len(sub_catgs) == 1:
            df['Sub_Category_1'] = sub_catgs[0]
            df['Sub_Category_2'] = None
        elif len(sub_catgs) == 2:
            df['Sub_Category_1'] = sub_catgs[1]
            df['Sub_Category_2'] = sub_catgs[0]
        else:
            for i in range(len(sub_catgs)):
        #        if sub_catgs[i] in ["women","men","boys","girls",'men women']:
        #        if i > 0:
        #        if len(sub_catgs)>1:
                df['Sub_Category_'+str(i+1)] = sub_catgs[i]
    #        else:
    #            df['Sub_Category_'+str(i+1)] = sub_catgs[i]


        try:
            df['Category_Total_Pds'] = count
        except:
            df['Category_Total_Pds'] = None

        if "Sub_Category_2" not in list(df.columns):
            df['Sub_Category_2'] = None

        if "Sub_Category_1" not in list(df.columns):
            df['Sub_Category_1'] = None

        df['Category'] = catg_name

        df.columns = ['Landing pageurl', 'Loyalty points enabled', 'Adid', 'Ispla', 'Product id',
           'Product', 'Productname', 'Rating', 'Ratingcount', 'Isfastfashion',
           'Future discounted price', 'Future discount startdate', 'Discount', 'Brand',
           'Searchimage', 'Effective discount percentage after tax',
           'Effective discount amount after tax', 'Buy button winner sku id',
           'Buy button winner seller partner id', 'Related styles count',
           'Related styles type', 'Sizes', 'Gender', 'Primary colour',
           'Discount label', 'Discount display label', 'Additional info', 'Category_Site',
           'Mrp', 'Price', 'Advance order tag', 'Color variant available',
           'Product image tag', 'List views', 'Discount type', 'Tdb xgy text',
           'Catalog date', 'Season', 'Year', 'Is personalised', 'Eor spic kstag',
           'Personalized coupon', 'Personalized coupon value', 'Product meta','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category']

        df.columns = [x.title() for x in list(df.columns)]

        #print(df['Rank'][0])
        df.columns = [x.replace(" ","_") for x in list(df.columns)]

        df = df[['Landing_Pageurl', 'Product', 'Productname', 'Rating', 'Ratingcount',
           'Isfastfashion', 'Future_Discounted_Price', 'Future_Discount_Startdate',
           'Discount', 'Brand',
           'Effective_Discount_Percentage_After_Tax',
           'Effective_Discount_Amount_After_Tax', 'Related_Styles_Count',
           'Related_Styles_Type', 'Sizes', 'Gender', 'Primary_Colour',
           'Discount_Label', 'Discount_Display_Label', 'Additional_Info',
           'Category_Site', 'Mrp', 'Price',
           'Color_Variant_Available',
           'Discount_Type', 'Catalog_Date', 'Season', 'Year',
           'Is_Personalised','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category']]

        df.columns = ['Landing_Page_Url', 'Product', 'Product_Name', 'Rating', 'Rating_Count',
           'Is_Fast_Fashion', 'Future_Discounted_Price', 'Future_Discount_Startdate',
           'Discount', 'Brand',
           'Effective_Discount_Percentage_After_Tax',
           'Effective_Discount_Amount_After_Tax', 'Related_Styles_Count',
           'Related_Styles_Type', 'Sizes', 'Gender', 'Primary_Colour',
           'Discount_Label', 'Discount_Display_Label', 'Additional_Info',
           'Category_Site', 'MRP', 'Retail_Price',
           'Color_Variant_Available',
           'Discount_Type', 'Catalog_Date', 'Season', 'Year',
           'Is_Personalised','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category']

        df['Relevant_Date'] = today.date()
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        #df['Rank'] = None
        main = pd.concat([main,df])
        for vals in ['Sub_Category_1','Sub_Category_2','Sub_Category_3','Category']:
            try:
                main[vals] = main[vals].str.title()
            except:
                continue
    print("DATA ACQUIRED")
    return main

def get_all_pages_urls(url,nos_of_pages,level):
    other_pages = []
    if level == 1:
        for i in range(2,nos_of_pages+1):
            a = url.split("?")
            other_pages.append(a[0]+"?p="+str(i)+"&"+a[-1])
    elif level == 2:
        for i in range(2,nos_of_pages+1):
            a = url.split("&")
            other_pages.append(a[0]+"&p="+str(i)+"&"+a[-1])
    elif level == 3:
        for i in range(2,nos_of_pages+1):
            a = url.split("&")
            other_pages.append(a[0]+"&p="+str(i)+"&"+a[-1])
    return other_pages
#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MYNTRA_ONE_TIME_RUN'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)

        import time
        start_time = time.time()

        check = []


        url = "https://www.myntra.com/clothing?plaEnabled=false&sort=popularity"

        driver = get_driver()


        '''

        Level1

        '''
        driver.get(url)

        soup = driver.page_source

        soup = BeautifulSoup(soup)

        data = get_json(soup)
        try:
            brands = get_brands(data)
        except:
            pass
        try:
            catgs = get_categories(data)
        except:
            pass
        try:
            page1 = len(data['searchData']['results']['products'])
        except:
            pass
        nos_of_pages = int(round(500/page1,0))

        other_pages = get_all_pages_urls(url,nos_of_pages,level=1)

        output = pd.DataFrame()
        try:
            output = pd.concat([output,get_data(data)])
            for links in other_pages:
                time.sleep(2)
                try:
                    driver.delete_all_cookies()
                except:
                    pass
                driver.get(links)
                soup = driver.page_source

                soup = BeautifulSoup(soup)

                data = get_json(soup)

                processed = get_data(data)

                output = pd.concat([output,processed])

            output1 = output.reset_index().drop("index",axis=1)
            output1['Rank'] = [i for i in range(1,len(output1)+1)]
            try:
                output1.to_sql(name= 'MYNTRA_DAILY_PRODUCT_DATA_4',con=engine,if_exists='append',index=False)
            except:
                pass
            check.append(output1)
        except:
            pass
        #%%
        '''

        Level2

        '''
        level2 = ['https://www.myntra.com/clothing?f=Gender%3Amen%2Cmen%20women&plaEnabled=false&sort=popularity',
         'https://www.myntra.com/clothing?f=Gender%3Amen%20women%2Cwomen&plaEnabled=false&sort=popularity',
         'https://www.myntra.com/clothing?f=Gender%3Aboys%2Cboys%20girls&plaEnabled=false&sort=popularity',
         'https://www.myntra.com/clothing?f=Gender%3Aboys%20girls%2Cgirls&plaEnabled=false&sort=popularity']


        for level2_links in level2:
            print(level2_links)
            driver.get(level2_links)

            soup = driver.page_source

            soup = BeautifulSoup(soup)

            data = get_json(soup)
            try:
                brands = get_brands(data)
            except:
                pass
            try:
                catgs = get_categories(data)
            except:
                pass
            try:
                page1 = len(data['searchData']['results']['products'])
            except:
                page1  = 50
            nos_of_pages = int(round(500/page1,0))

            other_pages = get_all_pages_urls(level2_links,nos_of_pages,level=2)

            try:
                output = pd.DataFrame()
                output = pd.concat([output,get_data(data)])

                for links in other_pages:
                    time.sleep(2)
                    try:
                        driver.delete_all_cookies()
                    except:
                        pass
                    driver.get(links)
                    soup = driver.page_source

                    soup = BeautifulSoup(soup)

                    data = get_json(soup)

                    processed = get_data(data)

                    output = pd.concat([output,processed])

                output1 = output.reset_index().drop("index",axis=1)
                output1['Rank'] = [i for i in range(1,len(output1)+1)]

                if len(output1)>500:
                    output1 = output1.head(500)
                try:
                    output1.to_sql(name= 'MYNTRA_DAILY_PRODUCT_DATA_4',con=engine,if_exists='append',index=False)
                except:
                    pass
                check.append(output1)
            except:
                pass

            '''

            Level3

            '''


            sub_urls = get_sub_catgs(catgs,level2_links)

            final_urls = []



            for links_2 in sub_urls:

                print("####"+links_2+"####")

                driver.get(links_2)

                soup = driver.page_source

                soup = BeautifulSoup(soup)

                data = get_json(soup)

                brands = get_brands(data)

                #catgs = get_categories(data)

                try:
                    page1 = len(data['searchData']['results']['products'])
                except:
                    page1  = 50

                nos_of_pages = int(round(500/page1,0))

                final_urls = get_all_pages_urls(links_2,nos_of_pages,level=3)

                output = pd.DataFrame()
                try:
                    output = pd.concat([output,get_data(data)])

                    for links_3 in final_urls:
                        time.sleep(2)
                        try:
                            driver.delete_all_cookies()
                        except:
                            pass
                        driver.get(links_3)
                        soup = driver.page_source

                        soup = BeautifulSoup(soup)

                        data = get_json(soup)

                        processed = get_data(data)

                        output = pd.concat([output,processed])

                    output1 = output.reset_index().drop("index",axis=1)
                    output1['Rank'] = [i for i in range(1,len(output1)+1)]



                    if len(output1)>500:
                        output1 = output1.head(500)
                    try:
                        output1.to_sql(name= 'MYNTRA_DAILY_PRODUCT_DATA_4',con=engine,if_exists='append',index=False)
                    except:
                        pass
                    check.append(output1)

                except:
                    continue


        print("--- %s seconds ---" % (time.time() - start_time))
#$$
        connection.close()
        log.job_end_log(table_name,job_start_time,no_of_ping)

    except:

        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
