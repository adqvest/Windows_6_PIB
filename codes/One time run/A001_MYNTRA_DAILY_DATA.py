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
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import random
import time
import numpy as np
import math
import requests, json
from bs4 import BeautifulSoup
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, 'C:\Adqvest')
import adqvest_db
import JobLogNew as log

#%%
def get_json(soup):
    scripts = soup.find_all('script')
    script = []
    for a in scripts:
        try:
            #print(a) # See what you try to load
            data = json.loads(a.text)
            print("Success")
            script.append(data)
        except Exception as e:
            print("Not Successful because {}".format(e)) # Print additional information

    script1 = []
    for a in scripts:
        try:
            #print(a) # See what you try to load
            data = a.text
            if "window.__myx" in data:
                print("Success")
                script1.append(data)
        except Exception as e:
            print("Not Successful because {}".format(e))

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

def data_acq(driver,row,sub_catg3,engine):
    soup = driver.page_source
    soup = BeautifulSoup(soup)
    data = get_json(soup)
    url = driver.current_url
#                        count = data['searchData']['results']['totalCount']

#                soup = BeautifulSoup(r.text,'lxml')

#                        data = get_json(soup)

    try:
        data = data['searchData']['results']['products']
    except:
        pass
    try:
        count = get_json(soup)['searchData']['results']['totalCount']
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

        try:
            sub_catg3 = sub_catg3.split("(")[0].strip()
        except:
            sub_catg3 = None
        df = pd.DataFrame(prds,index=[0])
        df = df.dropna(axis=1)
        df['Category'] = row['Category']
        df['Sub_Category_1'] = row['Sub_Category_1']
        df['Sub_Category_2'] = row['Sub_Category_2']
        if sub_catg3 != None:
            df['Sub_Category_3'] = sub_catg3
        else:
            df['Sub_Category_3'] = None
        try:
            df['Category_Total_Pds'] = count
        except:
            df['Category_Total_Pds'] = None



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
           'Personalized coupon', 'Personalized coupon value', 'Product meta','Category','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category_Total_Pds']

        df.columns = [x.title() for x in list(df.columns)]

        #print(df['Rank'][0])
        df.columns = [x.replace(" ","_") for x in list(df.columns)]
        if '2' in url:
            l = l +50
            df['Rank'] = l
        else:
            df['Rank'] = l


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
           'Is_Personalised','Rank','Category','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category_Total_Pds']]

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
           'Is_Personalised','Rank','Category','Sub_Category_1','Sub_Category_2','Sub_Category_3','Category_Total_Pds']

        df['Relevant_Date'] = today.date()
        df['Runtime'] = pd.to_datetime(today.strftime('%Y-%m-%d %H:%M:%S'))
        print(df)
        df.to_sql(name= 'MYNTRA_DAILY_PRODUCTS_DATA_3',con=engine,if_exists='append',index=False)

#%%
def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = 'MYNTRA_DAILY_PRODUCTS_DATA_TRIAL'
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

#%%
        url = 'https://www.myntra.com/'

        driver = get_driver()

        driver.get(url)

        soup = driver.page_source

        soup = driver.page_source

        soup = BeautifulSoup(soup)

        site_structure = pd.DataFrame()

        for vals in ["men","women","kids","home-&-living"]:
            men = soup.find_all("div",{"data-group":vals})
            sub_catg_1 = []
            sub_catg_2  = []
            men_data = pd.DataFrame()

            catgs = men[0].findAll("div",{"class":'desktop-hrLine'})
            sub_catgs = catgs[0].find_next_siblings("")
            overall = soup.find_all("a",{"data-group":vals})
            overall_text = [x.text for x in  overall]
            overall_link = [x['href'] for x in overall]

#            site_structure = pd.concat([site_structure,pd.DataFrame({"Category":overall_text,"Sub_Category_1":None,"Sub_Category_2":None,"Link":overall_link},index=[0])])

            for elem in men:
        #            sub_catg_1.append(elem.find_all("a",{"class":"desktop-categoryName"}))
        #            text1 = [x.text for x in elem.find_all("a",{"class":"desktop-categoryName"})]
        #            link1 = [x['href'] for x in elem.find_all("a",{"class":"desktop-categoryName"})]
        #            data_reactid = [x['data-reactid'] for x in elem.find_all("a",{"class":"desktop-categoryName"})]
                for index,x1 in enumerate(elem.find_all("a",{"class":"desktop-categoryName"})):
        #                x1 = elem.find_all("a",{"class":"desktop-categoryName"})[0]
                    text1 = x1.text
                    link1 = x1['href']
                    sub_data = pd.DataFrame({"Category":overall_text,"Sub_Category_1":text1,"Sub_Category_2":None,"Link":link1},index=[0])
                    men_data = pd.concat([men_data,sub_data])
                    try:
                        data_reactid = [elem.find_all("a",{"class":"desktop-categoryName"})[index]['data-reactid'],elem.find_all("a",{"class":"desktop-categoryName"})[index+1]['data-reactid']]
                    except:
                        try:
                           data_reactid = [elem.find_all("a",{"class":"desktop-categoryName"})[index]['data-reactid']]
                        except:
                            continue
                    for i in range(len(data_reactid)):
                        try:
                            if i/2 == 0:
                                a = [x for x in range(int(data_reactid[i])+1,int(data_reactid[i+1])-1)]
                                a = [y for x,y in enumerate(a) if x%2 == 0]
                            else:
                                a = [x for x in range(int(data_reactid[i])+1,int(data_reactid[i+1])-1)]
                                a = [y for x,y in enumerate(a) if x%2 == 0]
                            print(a)
                            for x in a:
                                sub_catg_2.append(elem.find_all("li",{'data-reactid':str(x)}))
                                text2= [x.text for x in elem.find_all("li",{'data-reactid':str(x)})]
                                try:
                                    link2 = [x['href'] for x in [elem.find_all("li",{'data-reactid':str(x)})[0].find("a")]]
                                except:
                                    pass
                                for a, b in zip(text2,link2):
                                    sub_data1 =  pd.DataFrame({"Category":overall_text,"Sub_Category_1":text1,"Sub_Category_2":a,"Link":b},index=[0])
                                    men_data = pd.concat([men_data,sub_data1])
                        except:
                            continue

                    site_structure = pd.concat([men_data,site_structure])

#        site_structure['Sub_Category_2'] = np.where(site_structure['Sub_Category_2'].isna(),site_structure['Sub_Category_1'],site_structure['Sub_Category_2'])
#        site_structure['Sub_Category_1'] = np.where(site_structure['Sub_Category_1'].isna(),site_structure['Category'],site_structure['Sub_Category_2'])

        site_structure = site_structure.drop_duplicates(["Sub_Category_1","Sub_Category_2","Link"])
        #site_structure = site_structure.drop_duplicates(["Link"])
        #site_structure = site_structure[site_structure.Link.duplicated()]
#        site_structure = site_structure.groupby(["Sub_Category_1","Sub_Category_2","Link"], as_index=False).nth(1)
#        site_structure = site_structure.reset_index().drop("index",axis=1)
        #        site = ["https://www.myntra.com"+x for x in site]
        na = []
        na1 = []
        for i in range(len(site_structure)):
            print(site_structure.iloc[i])
            try:
                if site_structure.iloc[i]['Link']==site_structure.iloc[i+1]['Link']:
                    na.append(site_structure.iloc[i]['Sub_Category_2'])
                    na1.append(i)
            except:
                continue

        site_structure = site_structure[site_structure['Sub_Category_2'].isin(na)==False]
        site_structure['Link'] = "https://www.myntra.com"+site_structure['Link']

#%%


        for ix,row in site_structure.iterrows():
            url1 = row['Link']
            print(url1)
            urls = [url1,url1+"?p=2&plaEnabled=false"]
#            driver.get(url)https://www.myntra.com/men-bags-backpacks?p=1&plaEnabled=false
            time.sleep(8)
            #r = requests.get(url, verify = False , headers = headers)
            #main_limit=0
            for url in urls:
                main = pd.DataFrame()
                try:
                    print(url)
                    driver.get(url)
                    driver.set_page_load_timeout(60)
                    if '2' in url:
                        data_acq(driver,row,None,engine)
                    else:
                        data_acq(driver,row,None,engine)
                        try:
                            xpath = '//ul[@class="categories-list"]/li'
                            eles = driver.find_elements_by_xpath(xpath)
                            for ele in eles:
                                print(ele.text)
                                soup = driver.page_source
                                soup = BeautifulSoup(soup)
                                data_acq(driver,row,str(ele.text),engine)
                                try:
                                    xpath = '//li[@class="pagination-next"]/a'
                                    eles = driver.find_elements_by_xpath(xpath)[0]
                                    eles.click()
                                    data_acq(driver,row,str(ele.text),engine)

                                except:
                                    print("ERROR")
                                    continue
                        except:
                            print("ERROR")
                            continue

                    #main.to_sql(name= 'MYNTRA_DAILY_PRODUCTS_DATA_1',con=engine,if_exists='append',index=False)

                    #break
                except:
                        print("ERROR")
                    try:
                        driver.get(url)
                    except WebDriverException:
                        print("page down")
                            #raise Exception("Error in code")
                    #driver.save_screenshot(r'C:\Users\Administrator\AdQvestDir\codes\One time run\myntra\filename'+str(len(url))+'.png')
#%%
        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
