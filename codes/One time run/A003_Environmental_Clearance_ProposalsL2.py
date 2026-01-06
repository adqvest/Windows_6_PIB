# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 15:24:40 2021

@author: Abhishek Shankar
"""



import pandas as pd
from dateutil import parser
import datetime as datetime
from datetime import date
import timeit
import io
import numpy as np
from pytz import timezone
import pandas as pd
from dateutil import parser
import datetime as datetime
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

sys.path.insert(0, 'C:/Users/Administrator/AdQvestDir/Adqvest_Function')
#sys.path.insert(0, r'C:\Adqvest')
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
#%%
def run_program(run_by = 'Adqvest_Bot', py_file_name = None):
    os.chdir('C:/Users/Administrator/AdQvestDir/')
#    os.chdir(r'C:\Adqvest')

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

#%%%
        #df = pd.read_excel("C:\Adqvest\Adam")
        main = pd.read_sql("Select * from AdqvestDB.MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_FUNCTIONS_PIT;",con=engine)
        main = main[main.iloc[:,2].str.lower().str.contains("existing")==False].reset_index(drop=True)


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
        #nos_pages = soup.find_all("")
        #df = pd.read_html(soup)

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

        all_fns = main['Level_2_Functions'].to_list()
        pages = list(main['Page'].unique())

        #main = main.head(1)
        for pg_nos in pages:
          sub_data = pd.DataFrame()

          frame = main[main['Page']==pg_nos]

          for i,row in frame.iterrows():

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

              url2 = "http://environmentclearance.nic.in/offlineproposal_status.aspx"
              headers['Cookie'] = '; '.join([x.name + '=' + x.value for x in r.cookies])
              headers['content-type'] = 'application/x-www-form-urlencoded'
              headers['Host']=  'environmentclearance.nic.in'
              headers['Origin']=  'http://environmentclearance.nic.in'
              headers['Referer'] = 'http://environmentclearance.nic.in/offlineproposal_status.aspx'

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
  #            print()
#              break
            #continue


          try:
            print("Upload")
            sub_data.to_sql(name = "MEFCC_PROPOSALS_YEARLY_DATA_PARIVESH_RAW_PIT",con = engine,if_exists = 'append',index = False)
          except:
            wd = "C:/Users/Administrator/AdQvestDir/codes/INPUT_FILES"
            sub_data.to_csv(wd+"PARIVESH"+str(pg_nos)+".csv",index=False)
            continue
            #raise Exception("Something wrong while sending data to sql")

#%%NA  ctl00$ContentPlaceHolder1$grdevents$ctl04$lnkDelete

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
