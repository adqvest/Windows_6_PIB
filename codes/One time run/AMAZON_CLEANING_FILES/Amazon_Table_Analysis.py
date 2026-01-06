# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 16:14:29 2020

@author: abhis
"""

from pytz import timezone
import en_core_web_sm
import pandas as pd
import numpy as np
import sqlalchemy
import unidecode
import os
import datetime
import re
from quantities import units
import unidecode
import re
from quantulum3 import parser
import sys
#import adqvest_db
#import JobLogNew as log
os.chdir(r"C:\Users\Administrator\AdQvestDir\codes\One time run\AMAZON_CLEANING_FILES")


con_string = 'mysql+pymysql://AdqServiceAct:AdServ!ce%2@amazondbmumbai.cnfgszrwissj.ap-south-1.rds.amazonaws.com:3306/AdqvestDB?charset=utf8'
engine1     = sqlalchemy.create_engine(con_string,encoding='utf-8')

main_table = pd.read_sql("Select * from AdqvestDB.AMAZON_SCRAPY_CONF_TABLE",con=engine1)
tables  = main_table['Table_Name'].tolist()
reqd = ['BEAUTY',
'GROCERY_AND_GOURMET_FOODS',
'HEALTH_AND_PERSONAL_CARE',
'LAUNCHPAD',
'SHOES_AND_HANDBAGS',
'HOME_AND_KITCHEN']
tables1 = []
for x in tables:
    for y in reqd:
        if y in x:
            tables1.append(x)
tables = tables1
#tables = ["AMAZON_BEAUTY"]#Hard Coded
tables = [x for x in tables if "LAUNCHPAD" not in x]
df_new = pd.DataFrame(columns=['Table_Name','Count of Unique Products','Total_Products'])

for t in tables:
    print(t)
    d1 = pd.read_sql("Select count(Distinct(Name)) as Count_Unique_Products , count(*) as Total_Products from AdqvestDB."+t+";",con=engine1)
    d1['Table_Name'] = t

    print(d1)

    df_new = pd.concat([df_new,d1],sort=False)


df_new = df_new[['Table_Name','Count_Unique_Products','Total_Products']]
df_new.to_csv("AMAZON_TABLES_ANALYSIS.csv",index=False)
