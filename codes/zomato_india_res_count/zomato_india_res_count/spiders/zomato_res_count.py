# -*- coding: utf-8 -*-
import scrapy
# from ..items import ZomatoItem
import re
import datetime as datetime
from pytz import timezone
from bs4 import BeautifulSoup
import json
import requests
import os
import sqlalchemy
import pandas as pd
import json
import random
import time
import numpy as np
import math
import pandas
import sys
import unidecode
import boto3
import botocore
#%%
# sys.path.insert(0, 'D:/AdQvestDir/Adqvest_Function')
# #sys.path.insert(0, 'C:/Adqvest')
# import adqvest_db
# import unidecode
# import ClickHouse_db

# engine = adqvest_db.db_conn()
# connection = engine.connect()
# client = ClickHouse_db.db_conn()
#%%
india_time = timezone('Asia/Kolkata')
today = datetime.datetime.now(india_time)
yesterday = datetime.datetime.now(india_time) - datetime.timedelta(1)


class FirstSpider(scrapy.Spider):
    name = 'zomato_india_res_count'
    headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

    def start_requests(self):
        url = 'https://www.zomato.com/delivery-cities'
        yield scrapy.Request(url=url, callback=self.parse,headers = self.headers)


    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        b = soup.find_all("script")
        b = [x for x in b if "window.__PRELOADED_STATE__" in x.text]
      
#%%
        b = response.text.split(" = JSON.parse(")[-1].split(");")[0]
        # print(b)
        print(type(b))
        data = json.loads(eval(b))
        print(type(data))
        # print(data)
        
        cities = data['pages']['deliverycities']['allO2Cities']
        city_urls = [x['url'] for x in cities]
        city_name = [x['name'] for x in cities]


        city_name = [x.replace("\n","") for x in city_name]
        # print(city_name)

        for url,city in zip(city_urls,city_name):
            yield scrapy.Request(url = url, callback = self.parse1, meta = {'City':city},headers = self.headers)




    def parse1(self, response):

            city = response.meta['City']
            soup = BeautifulSoup(response.body, "lxml")
            b = soup.find_all("script")
            b = [x for x in b if "window.__PRELOADED_STATE__" in x.text]
            b = response.text.split(" = JSON.parse(")[-1].split(");")[0]
            data = json.loads(eval(b))
            
            # print(data)
            city_key = list(data['pages']['search'].keys())[0]#
            act_data = data['pages']['search'][city_key]['sections']['SECTION_POPULAR_LOCATIONS']['locations']
            # print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n',act_data)
            area_list = []
            count_list = []
            for i in act_data:
                print(i['name'], '------>', i['count'])
                area_list.append(i['name'].lower().replace('locality', '').strip().title())
                count_list.append(i['count'].split()[0])



            for area,count in zip(area_list,count_list):
                item = {}
                item['Country'] = "India"
                item['City'] = city
                item["Area"] = area
                try:
                    item["Area_Clean"] = item["Area"].apply(lambda x:x.split("(")[0].strip())
                except:
                    item["Area_Clean"] =item["Area"]
                item["Count"] = count
                item['Relevant_Date'] = today.date()
                item['Runtime'] = today.strftime("%Y-%m-%d %H:%M:%S")
                item["Relevant_Week"] ="Week "+str(today.date().strftime("%V"))+"-"+today.date().strftime("%Y")
                item={"Country":item['Country'],"City":item['City'],"Area":item["Area"],"Area_Clean":item["Area_Clean"],
                      "Count":item["Count"],"Relevant_Week":item["Relevant_Week"],"Relevant_Date":item['Relevant_Date'],"Runtime":item['Runtime']}
                
                print("uploaded")
                yield item
