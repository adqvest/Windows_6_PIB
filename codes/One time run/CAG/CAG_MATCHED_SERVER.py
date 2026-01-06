# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 19:19:23 2021

@author: abhis
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 17:59:57 2021

@author: abhis
"""


import unidecode
import os
import sys
import datetime
import re
from quantities import units
import unidecode
import re
from quantulum3 import parser
from pytz import timezone
from bs4 import BeautifulSoup
from scrapy import Selector
import json
import requests
import os
import sqlalchemy
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import re
import os
import sqlalchemy
import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import requests
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
from selenium import webdriver
import random
import time
import numpy as np
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import math
import requests, json
from bs4 import BeautifulSoup

from fuzzywuzzy import process
from itertools import product
os.chdir(r'C:\Users\Administrator\AdQvestDir\codes\One time run\CAG')

con_string = 'mysql+pymysql://abhishek:@bhI$_02@ohiotomumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine     = sqlalchemy.create_engine(con_string,encoding='utf-8')


option = Options()
option.add_argument("--disable-infobars")
option.add_argument("start-maximized")
option.add_argument("--disable-extensions")
option.add_argument("--disable-notifications")
option.add_argument('--ignore-certificate-errors')
option.add_argument('--no-sandbox')

headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
driver=webdriver.Chrome(executable_path= r"C:\Users\Administrator\AdQvestDir\chromedriver.exe")
unable = pd.DataFrame()
data = pd.DataFrame()
overall_villages = pd.read_sql("Select * from AdqvestDB.MATCHED_VILLAGES",con=engine)
overall_villages = overall_villages[overall_villages['Links'].isnull()==False]
cols = list(overall_villages)
matched = pd.read_sql("select * from AdqvestDB.VILLAGE_LAT_LONG_CAG",con=engine)
unmatched = matched[matched['Geocode']=='en_US']['Village'].to_list()
overall_villages = pd.read_sql("Select * from AdqvestDB.MATCHED_VILLAGES",con=engine)
overall_villages = overall_villages[overall_villages['Links'].isnull()==False]
unmatched = ['Pataria',
'Dandasora',
'Vishun Pur Airhaniya'
]
overall_villages =  overall_villages[overall_villages['Village'].isin(unmatched)]
for i,row in overall_villages.iterrows():
    print(row['Links'])

#            response = requests.get(row['Links'])
    #    soup = BeautifulSoup(response.text, 'html.parser')
    #    soup.find_all("a",{'target':'_blank'})[1]['href']
    driver.get(row['Links'])
    time.sleep(5)
    driver.execute_script("window.scrollTo(0, window.scrollY + 400)")
    driver.implicitly_wait(30)
    driver.switch_to.default_content()
    driver.switch_to.frame("gmap")
    driver.implicitly_wait(30)
    time.sleep(15)
    elems = driver.find_elements_by_xpath("//a[@href]")
    geocode = []
    for elem in elems:
        print(elem.get_attribute("href"))
        geocode.append(elem.get_attribute("href"))
    try:
        val = geocode[2].split("/")[4]
    except:
        try:
            val =  geocode[2].split("/")[3]
        except:
            pass
    try:
        if len(geocode) != 0:

            df = {
                    cols[1]:row[cols[1]],
                    cols[2]:row[cols[2]],
                    cols[3]:row[cols[3]],
                    cols[4]:row[cols[4]],
                    cols[5]:row[cols[5]],
                    'Geocode': str(geocode)
                    }

            df = pd.DataFrame(df,index=[0])
            print(df)
            data = pd.concat([data,df])
            #df.to_csv(name = "VILLAGE_LAT_LONG_CAG",index = False,if_exists = 'append',con = engine)

            print("uploaded")
        else:

            print('Not Found')

            df = {

            cols[0]:row[cols[0]],
            cols[1]:row[cols[1]],
            cols[2]:row[cols[2]],
            cols[3]:row[cols[3]],
            cols[4]:row[cols[4]],
            cols[5]:row[cols[5]]

            }
            df = pd.DataFrame(df,index=[0])

            unable = pd.concat([unable,df])

    except:
            print('Not Found')

            df = {

            cols[0]:row[cols[0]],
            cols[1]:row[cols[1]],
            cols[2]:row[cols[2]],
            cols[3]:row[cols[3]],
            cols[4]:row[cols[4]],
            cols[5]:row[cols[5]]

            }
            df = pd.DataFrame(df,index=[0])

            unable = pd.concat([unable,df])

unable.to_csv("NOT FOUND.csv",index=False)
data.to_csv("FOUND.csv",index=False)
