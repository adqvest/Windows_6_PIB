# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 16:24:52 2021

@author: abhis
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlalchemy
import unidecode
import os
import datetime
import re
import unidecode
import re
import time
import datetime
from pytz import timezone
import re
import sys
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db
import JobLogNew as log



def run_program(run_by='Adqvest_Bot', py_file_name=None):
    os.chdir(r'C:/Users/Administrator/AdQvestDir')
    engine = adqvest_db.db_conn()
    #****   Date Time *****
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday = today - days

    ## job log details
    job_start_time = datetime.datetime.now(india_time)
    table_name = '1MG'
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
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
#        data = pd.read_sql("Select * from AdqvestDB.1MG_MANUFACTURERS_PRODUCTS",con=engine)
        data1 = pd.DataFrame()
        r = requests.get("https://www.1mg.com/manufacturer/omni-wellness-and-nutraceutical-private-limited-81738" , verify = False,headers=headers)
        soup = BeautifulSoup(r.text,'lxml')
        omni = soup.find_all("div",{"class":"col-sm-3 col-xs-6"})
        omni = [x.find_all("a") for x in omni]
        omni = sum(omni,[])
        text = [x.text for x in omni]
        link = [x['href'] for x in omni]
        for product,url in zip(text,link):

        #for i,row in data.iterrows():
#            print(i)
            print(product)
            time.sleep(10)

            try:
                r = requests.get("https://www.1mg.com"+url, verify = False , headers = headers)


                views = soup.find_all("span",{"class":'SocialSkus__views-text___251zk'})
                if views == []:
                    try:
                        views = soup.find_all("span",{"class":"SocialCue__views-text___1CTJI"})
                        views = views[0].text.split()[0]
                    except:
                        views = None
                else:
                    views = views[0].text.split()

                discounts = soup.find_all("span",{"class":"DrugPriceBox__bestprice-slashed-price___2ANwD"})
                if discounts == []:
                    discounts = None
                else:
                    discounts = discounts[0].text.strip()


                price = soup.find_all("div",{"class":"DrugPriceBox__best-price___32JXw"})
                if price == []:
                    price = None
                else:
                    price = price[0].text


                alt_brands_pds = soup.find_all("div",{"class":"SubstituteItem__name___PH8Al"})
                alt_brands = soup.find_all("div",{"class":"SubstituteItem__manufacturer-name___2X-vB"})

                if ((alt_brands_pds == []) or (alt_brands == [])):
                    alt_brands = None
                    alt_products = None
                else:
                    alt_brands = [x.text for x in alt_brands]
                    alt_brands_pds = [x.text for x in alt_brands_pds]

                alt_brands  = str(alt_brands)
                alt_brands_pds = str(alt_brands_pds)
                views = str(views)

                try:

                    tp_class = soup.find_all("div",{"class":"DrugFactBox__flex___1bp8c DrugFactBox__fact-row___38FQC DrugFactBox__marginTop8___359g7"})
                    tp_class = [x.text for x in tp_class]
                    tp_class = [x for x in tp_class if "Therapeutic" in x][0]
                    tp_class = tp_class.split("Class")[1]

                except:
                    tp_class = None


                try:
                    uses = soup.find_all("ul",{'class':'DrugOverview__list___1HjxR DrugOverview__uses___1jmC3'})
                    uses = [x.text for x in uses]
                    if len(uses) > 1:
                        uses = str(uses)
                    else:
                        uses = uses[0]
                except:
                    uses = None

                df = pd.DataFrame({"Product":product,#row['Product'],
                                   "Link":url,#row['Link'],
                                   "Company":"Omni Wellness And Nutraceutical Private Limited",#row['Company'],
                                   "Views":views,
                                   "Discounts":discounts,
                                   "Price":price,
                                   "Alternate Brands": alt_brands,
                                   "Alternative Brand Products":alt_brands_pds,
                                   "Therapeutic_Class": tp_class,
                                   "Uses": uses
                                   },index=[0])
                data1 = pd.concat([data1,df])


                df.to_sql(name='1MG_MANUFACTURES_PRODUCT_DATA_1',con=engine,if_exists='append',index=False)


            except:
                print('Not Found')
                continue
            #%%

        log.job_end_log(table_name,job_start_time,no_of_ping)
    except:

        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_msg = str(sys.exc_info()[1]) + " line " + str(exc_tb.tb_lineno)
        print(error_type)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg,no_of_ping)

if(__name__=='__main__'):
    run_program(run_by='manual')
