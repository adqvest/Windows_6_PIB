# -*- coding: utf-8 -*-
"""
Created on Fri Jul 30 14:21:41 2021

@author: Abhishek Shankar
"""
import sys
sys.path.insert(0, r'C:\Users\Administrator\AdQvestDir\Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
#import adqvest_db
import JobLogNew as log
import adqvest_db

from selenium import webdriver
from selenium.webdriver.support.select import Select
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.support.select import Select
import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
import time
import numpy as np
import re
import itertools
import requests
import sqlalchemy
from pandas.io import sql
import os
from bs4 import BeautifulSoup
from dateutil import parser
import sys
import warnings
warnings.filterwarnings('ignore')
import csv
import calendar
import pdb
import json
from dateutil import parser
warnings.filterwarnings('ignore')


#start_time = timeit.default_timer()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

engine = adqvest_db.db_conn()
connection = engine.connect()

def get_pages(soup,r,r2):
  ''' Pages Iteration '''
  headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
  url = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'


  pages = soup.find_all("table",{"class":"table Grid1"}==False)
  pages = [x.find("table") for x in pages]
  pages = [x for x in pages if x!=None][-1]
  pages = pages.find_all("a")
  pages = [x['href'] for x in pages]
  pages_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in pages]
  pages_nos_fns = [eval(x.split("javascript:__doPostBack")[-1])[1] for x in pages]
  final_page = pages_nos_fns[-1]
  #pages_nos_fns = [eval(x.split("javascript:__doPostBack")[-2])[0] for x in pages]
  #GETTING ALL JAVASCRIPT FUNCTIONS FROM PAGE
  view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
  event_target        = soup.select('input[name=__EVENTTARGET]')[0]['value']
  #last_focus          = soup.select('input[name=__LASTFOCUS]')[0]['value']
  event_argument      = soup.select('input[name=__EVENTARGUMENT]')[0]['value']
  view_stategenerator = soup.select('input[name=__VIEWSTATEGENERATOR]')[0]['value']
  view_statencrypted  = soup.select('input[name=__VIEWSTATEENCRYPTED]')[0]['value']
  view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
  event_validation    = soup.select('input[name=__EVENTVALIDATION]')[0]['value']

  #fns = eval(all_fns[45].split("javascript:__doPostBack")[-1])[0]

  data = {
          "__EVENTTARGET":pages_fns[0],
          "__EVENTARGUMENT":final_page,
          "__VIEWSTATE":view_state,
          "__VIEWSTATEGENERATOR":view_stategenerator,
          "__EVENTVALIDATION":event_validation,
          "__VIEWSTATEENCRYPTED":view_statencrypted,
          "ctl00$ContentPlaceHolder1$textbox2": ""}

  url2 = "http://environmentclearance.nic.in/offlineproposal_status.aspx"
  headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
  headers['content-type'] = 'application/x-www-form-urlencoded'
  headers['Host']=  'environmentclearance.nic.in'
  headers['Origin']=  'http://environmentclearance.nic.in'
  headers['Referer'] = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'

  r_2 = r.post(url, headers=headers, data=data)
  print(r_2.status_code)

  soup = BeautifulSoup(r_2.content,"html")
  table = soup.find("table",{"class":"table Grid1"})
  df = pd.read_html(str(table))
  #print(soup)
  total_pages = list(df[-1].iloc[0])[-1] # EXCLUDING LAST PAGE
  page_nos_function = []
  page_fns = []
  for i in range(2,total_pages):
    page_nos_function.append("Page$"+str(i))
    page_fns.append(list(set([eval(x.split("javascript:__doPostBack")[-1])[0] for x in pages]))[0])

  page_df = pd.DataFrame({"Page_Function":page_fns,"Page_Nos":page_nos_function})
  page_df.loc[len(page_df)+1,"Page_Nos"] = final_page
  page_df.loc[len(page_df),"Page_Function"] = page_fns[0]


  return page_df


def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    start_time = timeit.default_timer()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days
    end_date = yesterday.date()

    job_start_time = datetime.datetime.now(india_time)
    table_name = "MEFCC_PROPOSAL_YEARLY_DATA_PARIVESH_FUNCTIONS"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        #df = pd.read_excel("C:\Adqvest\Adam")

        ''' Level 1 '''



        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

        url = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'
        r       = requests.Session()
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36'}
        r1      = r.get(url, headers=headers)

        soup                = BeautifulSoup(r1.content,'lxml')
        view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
        event_target        = soup.select('input[name=__EVENTTARGET]')[0]['value']
        #last_focus          = soup.select('input[name=__LASTFOCUS]')[0]['value']
        event_argument      = soup.select('input[name=__EVENTARGUMENT]')[0]['value']
        view_stategenerator = soup.select('input[name=__VIEWSTATEGENERATOR]')[0]['value']
        view_statencrypted  = soup.select('input[name=__VIEWSTATEENCRYPTED]')[0]['value']
        view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
        event_validation    = soup.select('input[name=__EVENTVALIDATION]')[0]['value']


        data = {
                "__EVENTTARGET":event_target,
                "__EVENTARGUMENT":event_argument,
                "__VIEWSTATE":view_state,
                "__VIEWSTATEGENERATOR":view_stategenerator,
                "__EVENTVALIDATION":event_validation,
                "__VIEWSTATEENCRYPTED":view_statencrypted,
                "ctl00$ContentPlaceHolder1$textbox2": "",
                "ctl00$ContentPlaceHolder1$btn": "Search"}


        headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
        headers['content-type'] = 'application/x-www-form-urlencoded'
        headers['Host']=  'environmentclearance.nic.in'
        headers['Origin']=  'http://environmentclearance.nic.in'
        headers['Referer'] = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'

        r2 = r.post(url, headers=headers, data=data)
        print(r2.status_code)


        soup = BeautifulSoup(r2.content,"html")
        #print(soup)

        table = soup.find("table",{"class":"table Grid1"})
        df = pd.read_html(str(table))
        nos_pages = soup.find_all("")
        #df = pd.read_html(soup)
        output = df[0].copy()
        first_index = output[output.iloc[:,0].str.lower().str.contains("sno")].index[0]
        final_index = len(output)-1
        output = output[first_index:final_index]
        output.columns = output.iloc[0]
        output = output.iloc[1:len(output)]
        output.columns = [x.replace(".","") for x in output.columns]
        output.columns = [re.sub(' +',' ',x) for x in output.columns]
        output.columns = [x.title() for x in output.columns]
        output.columns = [x.replace(" ","_") for x in output.columns]
        output = output.dropna(how='all',axis=1)
        output['Page'] = 1
        ''' PAGE 1 '''
        output['Relevant_Date'] = today.date()
        output['Runtime']=datetime.datetime.now()
        output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FUNCTIONS_PIT",con = engine,if_exists = 'append',index = False)
        print("Page 1 Sent")

        page_df = get_pages(soup,r,r2)
        #page_df = page_df.head(1)

        for i,row in page_df.iterrows():
        #  print(row['Page_Nos'])
          time.sleep(1)
          ''' Pages Iteration '''
          pages = soup.find_all("table",{"class":"table Grid1"}==False)
          pages = [x.find("table") for x in pages]
          pages = [x for x in pages if x!=None][-1]
          pages = pages.find_all("a")
          pages = [x['href'] for x in pages]
          pages_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in pages]
          pages_nos_fns = [eval(x.split("javascript:__doPostBack")[-1])[1] for x in pages]
          final_page = pages_nos_fns[-1]
          #pages_nos_fns = [eval(x.split("javascript:__doPostBack")[-2])[0] for x in pages]
          #GETTING ALL JAVASCRIPT FUNCTIONS FROM PAGE
          view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
          event_target        = soup.select('input[name=__EVENTTARGET]')[0]['value']
          #last_focus          = soup.select('input[name=__LASTFOCUS]')[0]['value']
          event_argument      = soup.select('input[name=__EVENTARGUMENT]')[0]['value']
          view_stategenerator = soup.select('input[name=__VIEWSTATEGENERATOR]')[0]['value']
          view_statencrypted  = soup.select('input[name=__VIEWSTATEENCRYPTED]')[0]['value']
          view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
          event_validation    = soup.select('input[name=__EVENTVALIDATION]')[0]['value']

          #fns = eval(all_fns[45].split("javascript:__doPostBack")[-1])[0]

          data = {
                  "__EVENTTARGET":row['Page_Function'],
                  "__EVENTARGUMENT":row['Page_Nos'],
                  "__VIEWSTATE":view_state,
                  "__VIEWSTATEGENERATOR":view_stategenerator,
                  "__EVENTVALIDATION":event_validation,
                  "__VIEWSTATEENCRYPTED":view_statencrypted,
                  "ctl00$ContentPlaceHolder1$textbox2": ""}

          url2 = "http://environmentclearance.nic.in/offlineproposal_status.aspx"
          headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
          headers['content-type'] = 'application/x-www-form-urlencoded'
          headers['Host']=  'environmentclearance.nic.in'
          headers['Origin']=  'http://environmentclearance.nic.in'
          headers['Referer'] = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'

          r_2 = r.post(url, headers=headers, data=data)
          print(r_2.status_code)

          soup = BeautifulSoup(r_2.content,"html")
          table = soup.find("table",{"class":"table Grid1"})
          df = pd.read_html(str(table))
          #df = pd.read_html(soup)
          output = df[0].copy()
          first_index = output[output.iloc[:,0].str.lower().str.contains("sno")].index[0]
          final_index = len(output)-1
          output = output[first_index:final_index]
          output.columns = output.iloc[0]
          output = output.iloc[1:len(output)]
          output.columns = [x.replace(".","") for x in output.columns]
          output.columns = [re.sub(' +',' ',x) for x in output.columns]
          output.columns = [x.title() for x in output.columns]
          output.columns = [x.replace(" ","_") for x in output.columns]
          try:
              output['Page'] = int(row['Page_Nos'].split("Page$")[-1])
          except:
              output['Page'] =  int(page_df.iloc[i-2]['Page_Nos'].split("Page$")[-1])+1
          output = output.dropna(how='all',axis=1)
          output['Relevant_Date'] = today.date()
          output['Runtime']=datetime.datetime.now()
          print("############", str(row['Page_Nos']) ,"############")
          output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FUNCTIONS_PIT",con = engine,if_exists = 'append',index = False)

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1])
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')
