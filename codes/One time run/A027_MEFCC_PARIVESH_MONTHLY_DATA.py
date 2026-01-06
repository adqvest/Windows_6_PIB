# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 10:25:55 2022

@author: Abhishek Shankar
"""

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
sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
#sys.path.insert(0, '/home/ubuntu/AdQvestDir/Adqvest_Function')
#import adqvest_db
import JobLogNew as log
import adqvest_db


#start_time = timeit.default_timer()
india_time = timezone('Asia/Kolkata')
today      = datetime.datetime.now(india_time)
days       = datetime.timedelta(1)
yesterday =  today - days

engine = adqvest_db.db_conn()
connection = engine.connect()

#%%

def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
#    os.chdir(r'C:\Adqvest')
    #os.chdir('/home/ubuntu/AdQvestDir')
    engine = adqvest_db.db_conn()
    connection = engine.connect()

    start_time = timeit.default_timer()
    india_time = timezone('Asia/Kolkata')
    today      = datetime.datetime.now(india_time)
    days       = datetime.timedelta(1)
    yesterday =  today - days
    end_date = yesterday.date()

    job_start_time = datetime.datetime.now(india_time)
    table_name = "MEFCC_PROPOSAL_YEARLY_DATA_PARIVESH"
    scheduler = ''
    no_of_ping = 0

    if(py_file_name is None):
        py_file_name = sys.argv[0].split('.')[0]


    try :
        if(run_by == 'Adqvest_Bot'):
            log.job_start_log_by_bot(table_name,py_file_name,job_start_time)
        else:
            log.job_start_log(table_name,py_file_name,job_start_time,scheduler)



        def tableDataText(table):
            """Parses a html segment started with tag <table> followed
            by multiple <tr> (table rows) and inner <td> (table data) tags.
            It returns a list of rows with inner columns.
            Accepts only one <th> (table header/data) in the first row.
            """
            def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
                return [td.get_text(strip=True) for td in tr.find_all(coltag)]
            rows = []
            trs = table.find_all('tr')
            headerow = rowgetDataText(trs[0], 'th')
            if headerow: # if there is a header row include first
                rows.append(headerow)
                trs = trs[1:]
            for tr in trs: # for every table row
                rows.append(rowgetDataText(tr, 'td') ) # data row
            return rows

        def get_pages(soup,r,r2):
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

            ''' Pages Iteration '''
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
            url = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'


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

            data = {"__EVENTTARGET":pages_fns[0],
                    "__EVENTARGUMENT":final_page,
                    "__VIEWSTATE":view_state,
                    "__VIEWSTATEGENERATOR":view_stategenerator,
                    "__EVENTVALIDATION":event_validation,
                    "__VIEWSTATEENCRYPTED":view_statencrypted,
                    "ctl00$ContentPlaceHolder1$textbox2": ""}

            url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
            headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
            headers['content-type'] = 'application/x-www-form-urlencoded'
            headers['Host']=  'environmentclearance.nic.in'
            headers['Origin']=  'http://environmentclearance.nic.in'
            headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

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

        def level2data(output,soup,sesh,headers):
            r = sesh
            sub_data = pd.DataFrame()

            # ''' Level 2 '''
            #GETTING ALL JAVASCRIPT FUNCTIONS FROM PAGE
            view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
            event_target        = soup.select('input[name=__EVENTTARGET]')[0]['value']
            #last_focus          = soup.select('input[name=__LASTFOCUS]')[0]['value']
            event_argument      = soup.select('input[name=__EVENTARGUMENT]')[0]['value']
            view_stategenerator = soup.select('input[name=__VIEWSTATEGENERATOR]')[0]['value']
            view_statencrypted  = soup.select('input[name=__VIEWSTATEENCRYPTED]')[0]['value']
            view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
            event_validation    = soup.select('input[name=__EVENTVALIDATION]')[0]['value']

            for i,row in output.copy().iterrows():
                time.sleep(2)
                try:
                    fns = row['Level_2_Functions']

                    data = {
                            "__EVENTTARGET":fns,
                            "__EVENTARGUMENT":event_argument,
                            "__VIEWSTATE":view_state,
                            "__VIEWSTATEGENERATOR":view_stategenerator,
                            "__VIEWSTATEENCRYPTED":view_statencrypted,
                            "__EVENTVALIDATION":event_validation,
                            "ctl00$ContentPlaceHolder1$textbox2": ""}

                    url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
                    headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
                    headers['content-type'] = 'application/x-www-form-urlencoded'
                    headers['Host']=  'environmentclearance.nic.in'
                    headers['Origin']=  'http://environmentclearance.nic.in'
                    headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

                    r3 = r.post(url2, headers=headers, data=data)
                    print(r3.status_code)
                    soup3 = BeautifulSoup(r3.text,'html')
                  #  table = soup3.find("table",{"class":"table Grid1"})
                  #  details1 = pd.read_html(str(table))
                    #soup = BeautifulSoup(r3.content,"html")

                  # '''CLEANING THE DATA'''

                  #AFTER GETTING THE PAGE DATA INTO TABULAR FORMAT
                    df2 = tableDataText(soup3)
                    df2 = [x for x in df2 if len(x)==3]
                    df2 = [" ".join(x) for x in df2]
                    df2 = [x for x in df2 if ":" in x]
                    df2 = [re.sub(' +',' ',x) for x in df2]
                    df2 = [x.replace(" : ",":") for x in df2]
                    df2 = [x for x in df2 if "enter" not in x.lower()]
                 #  df2 = ["{"+x+"}" for x in df2]
                 #  df2 = [x.replace("[","") for x in df2]
                 #  import yaml

                    def list_to_dict(rlist):
                        return dict(map(lambda s : s.split(':'), rlist))
                 #  df2 = [yaml.load(x) for x in df2]
                    details2 = pd.DataFrame(list_to_dict(df2),index=[0])
                 #  del df2
                    a = pd.DataFrame(row).T.reset_index(drop=True)
                    b = details2.reset_index(drop=True)

                    data1 = pd.concat([a,b],axis=1)
                    data1['Function'] = fns
                    data1['Timestamp'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                 #  del details2
                    pt = fns.split("ContentPlaceHolder1")[-1]
                    sub_data = pd.concat([sub_data,data1])
                    print("####### Done : ",pt , "#######" )
                except:
                    print("NA ",row['Level_2_Functions'])
                    continue


                # print(sub_data.columns)
                sub_data['Data_Check'] = np.where(sub_data.Proposal_No==sub_data['Proposal No'],0,1)
                return sub_data
        #df = pd.read_excel("C:\Adqvest\Adam")
        '''
        Total of 3 tables

        1. Tables Run in the first Run go to MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT
        2. Tables Run in the 2nd Run go to MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT_NF
        3. Cleaning Data from the Above 2 tables and storing in MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FINAL_PIT_CLEAN_DATA

        '''


        query = "Select max(Relevant_Date) as RD from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FINAL_PIT_CLEAN_DATA"
        max_date = pd.read_sql(query,con=engine)['RD'][0]

        if (today.date()-max_date).days >= 100:

            ''' Level 1 '''


            query1 = "Drop Table IF EXISTS AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT"
            query2 = "Drop Table IF EXISTS AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT_NF"

            connection.execute(query1)
            connection.execute(query2)

            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

            url = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'
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

        #>>>>>>.. changed by Rahul . Added the new payload for the post req <<<<<<<<<<<
                    "ctl00$ContentPlaceHolder1$textbox2": "Search",
        #>>>>>>>>>>>>>>>>end of changes <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                    "ctl00$ContentPlaceHolder1$btn": "Search"}


            headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
            headers['content-type'] = 'application/x-www-form-urlencoded'
            headers['Host']=  'environmentclearance.nic.in'
            headers['Origin']=  'http://environmentclearance.nic.in'
            headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

            r2 = r.post(url, headers=headers, data=data)
            print(r2.status_code)


            soup = BeautifulSoup(r2.content,"html")
            #print(soup)

            table = soup.find("table",{"class":"table Grid1"})
            df = pd.read_html(str(table))
            #df = pd.read_html(soup)
            output = df[0].copy()
            print(output)
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
    #        output.columns = [x.replace("Moefcc")]
            all_fns = soup.find_all("a",{"title":"Click To View"})
            all_fns = [x['href'] for x in all_fns]
            all_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in all_fns]
            output['Level_2_Functions'] = all_fns
            output['Page'] = 1
            output['Relevant_Date'] = today.date()
            output['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            ''' PAGE 1
            '''
            output = level2data(output,soup,r,headers)
            try:
                output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT",con = engine,if_exists = 'append',index = False)
            except Exception as e:
                print(e.__traceback__.tb_lineno)


            page_df = get_pages(soup,r,r2)
            lp = page_df.copy()
    #        page_df = page_df.tail(1)

            for i,row in page_df.iterrows():
            #  print(row['Page_Nos'])
              time.sleep(1)
              ''' Pages Iteration '''
              pg_nos = row['Page_Nos'].split("$")[-1]
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

              url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
              headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
              headers['content-type'] = 'application/x-www-form-urlencoded'
              headers['Host']=  'environmentclearance.nic.in'
              headers['Origin']=  'http://environmentclearance.nic.in'
              headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

              r_2 = r.post(url, headers=headers, data=data)
              print(r_2.status_code)

              soup = BeautifulSoup(r_2.content,"html")
              table = soup.find("table",{"class":"table Grid1"})
              #>>>>>>>>>>>>>>..... changes made by Rahul 18,Aug 2022 <<<<<<<<<<<<<<
              #>>>>>> Changes made : inserted a try and except block in line no. 411 <<<<<<<<<
              try:
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
                  all_fns = soup.find_all("a",{"title":"Click To View"})
                  all_fns = [x['href'] for x in all_fns]
                  all_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in all_fns]
                  output['Level_2_Functions'] = all_fns
                  if "last" in row['Page_Nos'].lower():
                    output['Page'] = int(lp.iloc[len(lp)-2,1].split("Page$")[-1])+1
                  else:
                    output['Page'] = int(row['Page_Nos'].split("Page$")[-1])
                  output = output.dropna(how='all',axis=1)
                  output['Relevant_Date'] = today.date()
                  output['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                  print("############", str(row['Page_Nos']) ,"############")
                  try:
                      output = level2data(output,soup,r,headers)
                  except Exception as e:
                      print(e.__traceback__.tb_lineno)
                  time.sleep(1)
              except:
                  pass
            #>>>>>>>>>>>>..end of changes <<<<<<<<<<<<<<<<<<<<<<
              try:
                  output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT",con = engine,if_exists = 'append',index = False)
              except:
                  try:
                      print("ERROR IN SENDNG TO SQL")
                      continue
                  except:
                      continue

            '''

            LAST PAGE : LAST PAGE USUALLY THROWS status code 500 because of Javascript error

            '''
            for i,row in page_df.tail(1).iterrows():
            #  print(row['Page_Nos'])
              time.sleep(1)
              ''' Pages Iteration '''
              pg_nos = row['Page_Nos'].split("$")[-1]
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

              url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
              headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
              headers['content-type'] = 'application/x-www-form-urlencoded'
              headers['Host']=  'environmentclearance.nic.in'
              headers['Origin']=  'http://environmentclearance.nic.in'
              headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

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
              all_fns = soup.find_all("a",{"title":"Click To View"})
              all_fns = [x['href'] for x in all_fns]
              all_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in all_fns]
              output['Level_2_Functions'] = all_fns
              if "last" in row['Page_Nos'].lower():
                output['Page'] = int(lp.iloc[len(lp)-2,1].split("Page$")[-1])+1
              else:
                output['Page'] = int(row['Page_Nos'].split("Page$")[-1])
              output = output.dropna(how='all',axis=1)
              output['Relevant_Date'] = today.date()
              output['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
              print("############", str(row['Page_Nos']) ,"############")
              output = level2data(output,soup,r,headers)
              time.sleep(1)

              try:
                  output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT",con = engine,if_exists = 'append',index = False)
              except:
                  try:
                      print("ERROR IN SENDNG TO SQL")
                      continue
                  except:
                      continue

            time.sleep(300)

            '''

            RERUNNING CODE FOR Not Found Values

            '''
            def tableDataText(table):
                """Parses a html segment started with tag <table> followed
                by multiple <tr> (table rows) and inner <td> (table data) tags.
                It returns a list of rows with inner columns.
                Accepts only one <th> (table header/data) in the first row.
                """
                def rowgetDataText(tr, coltag='td'): # td (data) or th (header)
                    return [td.get_text(strip=True) for td in tr.find_all(coltag)]
                rows = []
                trs = table.find_all('tr')
                headerow = rowgetDataText(trs[0], 'th')
                if headerow: # if there is a header row include first
                    rows.append(headerow)
                    trs = trs[1:]
                for tr in trs: # for every table row
                    rows.append(rowgetDataText(tr, 'td') ) # data row
                return rows

            def get_pages(soup,r,r2):
              ''' Pages Iteration '''
              headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}
              url = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'


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

              url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
              headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
              headers['content-type'] = 'application/x-www-form-urlencoded'
              headers['Host']=  'environmentclearance.nic.in'
              headers['Origin']=  'http://environmentclearance.nic.in'
              headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

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

            def level2data(output,soup,sesh,headers):

              r = sesh
              sub_data = pd.DataFrame()

              ''' Level 2 '''
              #GETTING ALL JAVASCRIPT FUNCTIONS FROM PAGE
              view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
              event_target        = soup.select('input[name=__EVENTTARGET]')[0]['value']
              #last_focus          = soup.select('input[name=__LASTFOCUS]')[0]['value']
              event_argument      = soup.select('input[name=__EVENTARGUMENT]')[0]['value']
              view_stategenerator = soup.select('input[name=__VIEWSTATEGENERATOR]')[0]['value']
              view_statencrypted  = soup.select('input[name=__VIEWSTATEENCRYPTED]')[0]['value']
              view_state          = soup.select('input[name=__VIEWSTATE]')[0]['value']
              event_validation    = soup.select('input[name=__EVENTVALIDATION]')[0]['value']

              for i,row in output.copy().iterrows():

                time.sleep(2)
                try:
                  fns = row['Level_2_Functions']

                  data = {
                          "__EVENTTARGET":fns,
                          "__EVENTARGUMENT":event_argument,
                          "__VIEWSTATE":view_state,
                          "__VIEWSTATEGENERATOR":view_stategenerator,
                          "__VIEWSTATEENCRYPTED":view_statencrypted,
                          "__EVENTVALIDATION":event_validation,
                          "ctl00$ContentPlaceHolder1$textbox2": ""}

                  url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
                  headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
                  headers['content-type'] = 'application/x-www-form-urlencoded'
                  headers['Host']=  'environmentclearance.nic.in'
                  headers['Origin']=  'http://environmentclearance.nic.in'
                  headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

                  r3 = r.post(url2, headers=headers, data=data)
                  print(r3.status_code)
                  soup3 = BeautifulSoup(r3.text,'html')
                #  table = soup3.find("table",{"class":"table Grid1"})
                #  details1 = pd.read_html(str(table))
                  #soup = BeautifulSoup(r3.content,"html")

                  '''CLEANING THE DATA'''

                  #AFTER GETTING THE PAGE DATA INTO TABULAR FORMAT
                  df2 = tableDataText(soup3)
                  df2 = [x for x in df2 if len(x)==3]
                  df2 = [" ".join(x) for x in df2]
                  df2 = [x for x in df2 if ":" in x]
                  df2 = [re.sub(' +',' ',x) for x in df2]
                  df2 = [x.replace(" : ",":") for x in df2]
                  df2 = [x for x in df2 if "enter" not in x.lower()]
                #  df2 = ["{"+x+"}" for x in df2]
                #  df2 = [x.replace("[","") for x in df2]
                #  import yaml

                  def list_to_dict(rlist):
                      return dict(map(lambda s : s.split(':'), rlist))
                #  df2 = [yaml.load(x) for x in df2]
                  details2 = pd.DataFrame(list_to_dict(df2),index=[0])
                #  del df2
                  a = pd.DataFrame(row).T.reset_index(drop=True)
                  b = details2.reset_index(drop=True)

                  data1 = pd.concat([a,b],axis=1)
                  data1['Function'] = fns
                  data1['Timestamp'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                #  del details2
                  pt = fns.split("ContentPlaceHolder1")[-1]
                  sub_data = pd.concat([sub_data,data1])
                  print("####### Done : ",pt , "#######" )
                except:
                  print("NA ",row['Level_2_Functions'])
                  continue



              sub_data['Data_Check'] = np.where(sub_data['Proposal_No']==sub_data['Proposal No'],0,1)


              return sub_data
            done = pd.read_sql("Select Distinct Page from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT",con=engine)
            done_cols = pd.read_sql("Select * from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT limit 1",con=engine)
            headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"}

            url = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'
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
                    "ctl00$ContentPlaceHolder1$textbox2": "Search",
                    "ctl00$ContentPlaceHolder1$btn": "Search"}


            headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
            headers['content-type'] = 'application/x-www-form-urlencoded'
            headers['Host']=  'environmentclearance.nic.in'
            headers['Origin']=  'http://environmentclearance.nic.in'
            headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

            r2 = r.post(url, headers=headers, data=data)
            print(r2.status_code)


            soup = BeautifulSoup(r2.content,"html")
            #print(soup)
            bool1 = done[done.iloc[:,0].astype(str)==str('1')].any().iloc[0]


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
            output = output.dropna(how='all',axis=1)
    #        output.columns = [x.replace("Moefcc")]
            all_fns = soup.find_all("a",{"title":"Click To View"})
            all_fns = [x['href'] for x in all_fns]
            all_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in all_fns]
            output['Level_2_Functions'] = all_fns
            output['Page'] = 1
            output['Relevant_Date'] = today.date()
            output['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            ''' PAGE 1 '''
            if bool1:
               print("We Good")
               pass
            else:
              output = level2data(output,soup,r,headers)
              output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT_NF",con = engine,if_exists = 'append',index = False)

            check = pd.DataFrame()

            page_df = get_pages(soup,r,r2)
            page_df['Page'] = page_df.iloc[:,1].str.split("$").str[-1].astype(str)
            done.iloc[:,0] = done.iloc[:,0].astype(str)
            #page_df = page_df[~page_df['Page'].isin(list(done.iloc[:,0]))]

            lp = page_df.copy()

            #page_df = page_df.head(15)

            for i,row in page_df.iterrows():
              try:
                  print(row['Page'])
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

                  url2 = "http://environmentclearance.nic.in/proposal_status_new1.aspx"
                  headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r2.cookies])
                  headers['content-type'] = 'application/x-www-form-urlencoded'
                  headers['Host']=  'environmentclearance.nic.in'
                  headers['Origin']=  'http://environmentclearance.nic.in'
                  headers['Referer'] = 'http://environmentclearance.nic.in/proposal_status_new1.aspx'

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
                  all_fns = soup.find_all("a",{"title":"Click To View"})
                  all_fns = [x['href'] for x in all_fns]
                  all_fns = [eval(x.split("javascript:__doPostBack")[-1])[0] for x in all_fns]
                  output['Level_2_Functions'] = all_fns
                  if "last" in row['Page_Nos'].lower():
                    output['Page'] = int(lp.iloc[len(lp)-2,1].split("Page$")[-1])+1
                  else:
                    output['Page'] = int(row['Page_Nos'].split("Page$")[-1])
                  output = output.dropna(how='all',axis=1)
                  output['Relevant_Date'] = today.date()
                  output['Runtime'] =  pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
                  print("############", str(row['Page_Nos']) ,"############")
                  bool1 =done[done.iloc[:,0].astype(str)==str(row['Page'])].any().iloc[0]
                  if bool1:
                    print("We Good")
                    time.sleep(2)
                    continue
                  else:
                    output = level2data(output,soup,r,headers)
                    check = pd.concat([check,output])
        #            output.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH",con = engine,if_exists = 'append',index = False)
                    time.sleep(1)
        #            output = output[list(done_cols.columns)]
              except Exception as e:
                  print(e.__traceback__.tb_lineno)
            check.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT_NF",con = engine,if_exists = 'append',index = False)
            print("Data Uploaded")
            connection = engine.connect()


            time.sleep(300)

            '''

            CLEANING THE DATA

            '''
            cleandf = pd.read_sql("Select * from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT;"
                             , con = engine)
            df = cleandf.copy()
            df.columns = [re.sub(' +', ' ', x) for x in df.columns]
            df.columns = ["_".join(x.split(" of")) if " of" in x else x for x in df.columns]
            print(df.columns)

            df.columns = ['Company', 'Date_EC_Granted', 'Date_Receipt_For_EC',
                   'Date_Receipt_For_Proposal', 'Date_Receipt_Proposal', 'Date_TOR_Granted',
                   'Date_Apply_For_TOR', 'Date_Closed', 'Date_Delisted', 'Date_Rejected',
                   'District', 'File_No', 'Function', 'Level_2_Functions', 'Moefcc_File_No',
                   'Page', 'Project_Name', 'ProposalName', 'ProposalNo', 'Proposal_No',
                   'Proposal_Status', 'Relevant_Date', 'Runtime', 'SNo', 'State',
                   'Timestamp', 'Village', 'Data_Check']


            def get_date(x):
              try:
                return parser.parse(x).date()
              except:
                return None

            for vals in df.columns:

                if "date_" in vals.lower():
                    df[vals] = df[vals].apply(lambda x : get_date(x))


            df = df[['Company', 'Date_EC_Granted', 'Date_Receipt_For_EC',
                   'Date_Receipt_For_Proposal', 'Date_Receipt_Proposal', 'Date_TOR_Granted',
                   'Date_Apply_For_TOR', 'Date_Closed', 'Date_Delisted', 'Date_Rejected',
                   'District', 'File_No', 'Moefcc_File_No',
                   'Page', 'Project_Name', 'ProposalName','Proposal_No',
                   'Proposal_Status', 'Relevant_Date', 'Runtime', 'SNo', 'State', 'Village', 'Data_Check']]

            df.columns = [x.replace('ProposalName','Proposal_Name') for x in df.columns]
            #df = df[df['Data_Check']==0]
            df['District'] = np.where(df['District'].str.lower().str.contains("null|select"),np.nan,df['District'])
            cd1 = df.copy()

            cleandf = pd.read_sql("Select * from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_NO_PIT_NF"
                             , con = engine)

            df = cleandf.copy()
            df.columns = [re.sub(' +', ' ', x) for x in df.columns]
            df.columns = ["_".join(x.split(" of")) if " of" in x else x for x in df.columns]
            print(df.columns)
            df['Date_Receipt_Proposal'] = df['Date_ Receipt for Proposal']+df['Date_ Receipt_ Proposal']

            df = df[[x for x in df.columns if x not in ['Date_ Receipt for Proposal','Date_ Receipt_ Proposal']]]
            df.columns = ['Company', 'Data_Check', 'Date_Consideration_EC',
                   'Date_EC_Granted', 'Date_Receipt_EC', 'Date_TOR_Granted',
                   'Date_Apply_TOR', 'Date_Closed', 'Date_Delisted', 'Date_Rejected',
                   'Date_Returned', 'Date_Transferred', 'Date_Withdrawn', 'District',
                   'File_No', 'Function', 'Level_2_Functions', 'Moefcc_File_No', 'Page',
                   'Project_Name', 'Proposal_Name', 'Proposal No', 'Proposal_No',
                   'Proposal_Status', 'Relevant_Date', 'Runtime', 'SNo', 'State',
                   'Timestamp', 'Village', 'Date_Receipt_Proposal']


            df = df[[x for x in df.columns if x!= 'Proposal No']]

            for vals in df.columns:

                if "date_" in vals.lower():
                    df[vals] = df[vals].apply(lambda x : get_date(x))

            #l1 = list(set(df.columns).intersection(set(['Company', 'Date_EC_Granted', 'Date_Receipt_For_EC',
            #       'Date_Receipt_For_Proposal', 'Date_Receipt_Proposal', 'Date_TOR_Granted',
            #       'Date_Apply_For_TOR', 'Date_Closed', 'Date_Delisted', 'Date_Rejected',
            #       'District', 'File_No', 'Moefcc_File_No',
            #       'Page', 'Project_Name', 'Proposal_Name','Proposal_No',
            #       'Proposal_Status', 'Relevant_Date', 'Runtime', 'SNo', 'State', 'Village', 'Data_Check'])))
            #df = df[l1]

            #df = df[df['Data_Check']==0]
            df['District'] = np.where(df['District'].str.lower().str.contains("null|select"),np.nan,df['District'])

            cd2 = df.copy()


            final = pd.concat([cd1,cd2])
            final = final[[x for x in final.columns if x!= 'Level_2_Functions']]
            final = final[[x for x in final.columns if x!= 'Function']]
            final['SNo'] = final['SNo'].astype(int)
            final = final[sorted(final.columns)]
            final = final[['SNo',"State","District","Village", 'Project_Name', 'Proposal_Name', 'Proposal_No', 'Proposal_Status']+[x for x in final.columns if x not in ['SNo',"State","District","Village", 'Project_Name', 'Proposal_Name', 'Proposal_No', 'Proposal_Status','Relevant_Date',"Runtime"]]+['Relevant_Date',"Runtime"]]

            final['Date_Apply_TOR'] = final['Date_Apply_TOR']+final['Date_Apply_For_TOR']
            final['Date_Receipt_EC'] = final['Date_Receipt_EC']+final['Date_Receipt_For_EC']
            final['Date_Receipt_Proposal'] = final['Date_Receipt_Proposal']+final['Date_Receipt_For_Proposal']


            final = final[[x for x in final.columns if x not in ['Date_Apply_For_TOR','Date_Receipt_For_EC','Date_Receipt_For_Proposal','Timestamp']]]
            final['Runtime'] = pd.to_datetime(today.strftime("%Y-%m-%d %H:%M:%S"))
            a = list(final.columns)

            final.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FINAL_PIT_CLEAN_DATA",con = engine,if_exists = 'append',index = False)

        else:
          print("DATA IS UPTO DATE in DB")

        log.job_end_log(table_name,job_start_time, no_of_ping)

    except:
        try:
            connection.close()
        except:
            pass
        error_type = str(re.search("'(.+?)'",str(sys.exc_info()[0])).group(1))
        error_msg = str(sys.exc_info()[1]) + "line " + str(sys.exc_info()[-1].tb_lineno)
        print(error_msg)

        log.job_error_log(table_name,job_start_time,error_type,error_msg, no_of_ping)

if(__name__=='__main__'):
    run_program(run_by = 'manual')
