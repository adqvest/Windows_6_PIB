import sqlalchemy
import pandas as pd

import calendar
import os
import requests
import json
from bs4 import BeautifulSoup

import re
import ast
import datetime as datetime
from pytz import timezone
import requests

import numpy as np
import time

import sys
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import shutil
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver

import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
import adqvest_db

engine = adqvest_db.db_conn()
connection = engine.connect()

table_names = ["VAHAN_MAKER_VS_FUEL_RTO_LEVEL_DATA"]

for table in table_names:
    data_df = pd.read_sql("Select sum(Total) as Total,RTO_Office_Raw,count(distinct(Runtime)) as Runtime_Count,Relevant_Date from "+table+"   group by RTO_Office_Raw,Relevant_Date",engine)
    data_df = data_df[data_df["Runtime_Count"] > 1]
    data_df.reset_index(drop = True,inplace = True)

    from tqdm import tqdm
    for index,i in tqdm(data_df.iterrows()):
        print(table)
        runtimes_df = pd.read_sql("select distinct(Runtime) from "+table+" where RTO_Office_Raw = '"+i["RTO_Office_Raw"]+"' and Relevant_Date = '" +i["Relevant_Date"].strftime("%Y-%m-%d")+"'",engine)


        connection.execute("delete from "+table+" where Runtime = '" +runtimes_df["Runtime"].min().strftime("%Y-%m-%d %H:%M:%S")+"' and RTO_Office_Raw = '"+i["RTO_Office_Raw"]+"' and Relevant_Date = '"+i["Relevant_Date"].strftime("%Y-%m-%d")+"'")
        connection.execute('commit')
