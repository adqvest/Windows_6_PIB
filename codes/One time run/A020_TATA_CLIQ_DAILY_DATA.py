# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 10:07:09 2021

@author: abhis
"""


import unicodedata
import sqlalchemy
from dateutil import parser
import pandas as pd
from pandas.io import sql
import os
import requests
from bs4 import BeautifulSoup
import json
import re
from dateutil import parser
import datetime as datetime
from pytz import timezone
import sys
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import numpy as np
import adqvest_db
import JobLogNew as log
os.chdir('C:/Users/Administrator/AdQvestDir/')

engine = adqvest_db.db_conn()

#****   Date Time *****
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday = today - days


def total_products(data):

    try:
        cat = data["facetdatacategory"]['filters'][0]['categoryName']
    except:
        cat = None

    try:
        sub_cat_1 = data["facetdatacategory"]['filters'][0]['childFilters'][0]['categoryName']
    except:
        sub_cat_2 = None
    try:
        sub_cat_2 = data["facetdatacategory"]['filters'][0]['childFilters'][0]['childFilters'][0]['categoryName']
    except:
        sub_cat_2 = None

    try:
        sub_cat_3 = data["facetdatacategory"]['filters'][0]['childFilters'][0]['childFilters'][0]['childFilters'][0]['categoryName']
    except:
        sub_cat_3 = None

    try:
        qty = data['pagination']['totalResults']
    except:
        qty = None


    total_products = {
                    "Total_Products":qty,
                    "Category":cat,
                    "Sub_Category_1":sub_cat_1,
                    "Sub_Category_2":sub_cat_2,
                    "Sub_Category_3":sub_cat_3,
                    "Relevant_Date":today.date(),
                    "Runtime":pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                    }
    total_products = pd.DataFrame(total_products,index=[0])

    return total_products

def product_data(data,additional):

    output = pd.DataFrame()
    catg = additional['Category']
    catg1 = additional['Sub_Category_1']
    catg2 = additional['Sub_Category_2']
    catg3 = additional['Sub_Category_3']
    for stuff in data['searchresult']:
        i = 1
        price = stuff['price']
        mrp = price['mrpPrice']['doubleValue']
        rp = price['sellingPrice']['doubleValue']
        try:
            exchange = stuff['exchangeAvailable']
        except:
            exchange = None
        try:
            name = stuff['productTitle']
        except:
            name = stuff['productname']

        df = {
                "Product":name,
                "Main_Category":stuff['productCategoryType'],
                "Category" : catg,
                "Sub_Category_1":catg1,
                "Sub_Category_2":catg2,
                "Sub_Category_3":catg3,
                "Brand":stuff['brandname'],
                "MRP":mrp,
                "Discount_Percent":stuff['discountPercent'],
                "Retail Price":rp,
                "ProductId":stuff['productId'],
                "Nos_Ratings":stuff['ratingCount'],
                "Exchange_Possible":exchange,
                "Total_Reviews":stuff['totalNoOfReviews'],
                "Rank":i,
                "Link":stuff['webURL'],
                "Relevant_Date":today.date(),
                "Runtime":pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                }
        df = pd.DataFrame(df,index=[0])
        print(df)

        output = pd.concat([output,df])

    return output

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
    table_name = 'TATA_CLIQ_DAILY_DATA'
    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]
    scheduler = ''
    no_of_ping = 0

    try :
        if(run_by=='Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)


        headers = {
           "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
                   }

        codes = pd.read_sql("Select * from AdqvestDB.TATA_CLIQ_API_CODES",con=engine)
        #reqd_codes = codes[codes['Codes2'].isin(list(codes['Codes2'].unique()))]
        #reqd_codes = codes.drop_duplicates(['Codes2'])


        for i,row in reqd_codes.iterrows():
            try:
	            code = row['Codes2']
	            url1 = 'https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/searchProducts/?searchText=%3Arelevance%3Acategory%3AMSH'+str(code)+'%3AinStockFlag%3Atrue&isKeywordRedirect=false&isKeywordRedirectEnabled=true&channel=WEB&isMDE=true&isTextSearch=false&isFilter=false&qc=false&page=1&isPwa=true&pageSize=40&typeID=all'
	            url2 = 'https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/searchProducts/?searchText=%3Arelevance%3Acategory%3AMSH'+str(code)+'%3AinStockFlag%3Atrue&isKeywordRedirect=false&isKeywordRedirectEnabled=true&channel=WEB&isMDE=true&isTextSearch=false&isFilter=false&qc=false&page=2&isPwa=true&pageSize=40&typeID=all'
	            url3 = 'https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/searchProducts/?searchText=%3Arelevance%3Acategory%3AMSH'+str(code)+'%3AinStockFlag%3Atrue&isKeywordRedirect=false&isKeywordRedirectEnabled=true&channel=WEB&isMDE=true&isTextSearch=false&isFilter=false&qc=false&page=3&isPwa=true&pageSize=40&typeID=all'

	            #url = 'https://www.tatacliq.com/marketplacewebservices/v2/mpl/cms/defaultpage?pageId=defaulthomepage&channel=desktop'
	            #url = 'https://www.tatacliq.com/marketplacewebservices/v2/mpl/cms/desktopservice/header'
	            overall = []
	            for link in [url1,url2,url3]:
	                time.sleep(2)
	                r = requests.get(link, headers = headers ,verify = False)
	                data = json.loads(r.text)
	                overall.append(data)
	            try:
		            total = total_products(overall[0])
                    total['Code'] = code
                    total.to_sql(name = "TATA_CLIQ_TOTAL_PRODUCTS_DAILY_DATA_2",index = False,if_exists = 'append',con = engine)
                except:
	                pass
	            output = pd.DataFrame()
	            for data in overall:
	                try:
	                    df = product_data(data,total)
	                except:
	                    continue
	                output = pd.concat([output,df])

	            output = output.head(100)
	            output['Rank'] = [i for i in range(1,len(output)+1)]
                output['Code'] = code
	            output.to_sql(name = "TATA_CLIQ_PRODUCT_LEVEL_DATA_2",index = False,if_exists = 'append',con = engine)
#
            except:
                continue

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
